import importlib.util
import os
import pathlib
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from unittest import mock


ROOT = pathlib.Path(__file__).resolve().parents[2]
MODULE_PATH = ROOT / "scripts" / "init-media-bucket.py"
SPEC = importlib.util.spec_from_file_location("init_media_bucket", MODULE_PATH)
media_bucket = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(media_bucket)


class BucketAPI:
    def __init__(self, bucket=None, readback=None):
        self.buckets = {"media-video": bucket} if bucket is not None else {}
        self.readback = readback
        self.calls = []

    def request(self, method, path, payload=None):
        self.calls.append((method, path, payload))
        if method == "GET":
            bucket_id = path.rsplit("/", 1)[-1]
            if bucket_id not in self.buckets:
                return 404, {}
            if bucket_id == "media-video" and self.readback is not None:
                return 200, self.readback
            return 200, self.buckets[bucket_id]
        bucket_id = payload["id"] if method == "POST" else path.rsplit("/", 1)[-1]
        self.buckets[bucket_id] = payload
        if method == "PUT":
            return 200, {}
        return 200, {}


class MediaStorageBootstrapTests(unittest.TestCase):
    def test_create_bucket_and_read_back_policy(self):
        api = BucketAPI()
        media_bucket.ensure_bucket(api, media_bucket.EXPECTED_BUCKET)
        self.assertEqual(
            api.calls,
            [
                ("GET", "/bucket/media-video", None),
                ("POST", "/bucket", media_bucket.EXPECTED_BUCKET),
                ("GET", "/bucket/media-video", None),
            ],
        )

    def test_v174_no_such_bucket_400_is_narrowly_classified_as_missing(self):
        class MissingBucketAPI(BucketAPI):
            def request(self, method, path, payload=None):
                self.calls.append((method, path, payload))
                if method == "GET" and path == "/bucket/media-video" and path.rsplit("/", 1)[-1] not in self.buckets:
                    return 400, {"code": "NoSuchBucket"}
                return super().request(method, path, payload)

        api = MissingBucketAPI()
        media_bucket.ensure_bucket(api, media_bucket.EXPECTED_BUCKET)
        self.assertEqual(api.calls[0], ("GET", "/bucket/media-video", None))
        self.assertEqual(api.calls[1], ("POST", "/bucket", media_bucket.EXPECTED_BUCKET))
        self.assertEqual(api.calls[-1], ("GET", "/bucket/media-video", None))

    def test_updates_existing_bucket_then_verifies_readback(self):
        existing = {"id": "media-video", "public": True}
        api = BucketAPI(bucket=existing)
        media_bucket.ensure_bucket(api, media_bucket.EXPECTED_BUCKET)
        self.assertEqual(api.calls[1][0:2], ("PUT", "/bucket/media-video"))
        self.assertEqual(api.calls[1][2], media_bucket.EXPECTED_BUCKET)

    def test_rejects_malformed_or_mismatched_readback(self):
        for readback in ({}, {"id": "media-video", "public": False,
                              "file_size_limit": 80_000_000,
                              "allowed_mime_types": ["video/mp4", "image/png"]}):
            with self.subTest(readback=readback):
                with self.assertRaises(media_bucket.BootstrapError):
                    media_bucket.ensure_bucket(
                        BucketAPI(readback=readback), media_bucket.EXPECTED_BUCKET
                    )

    def test_media_mode_keeps_image_bucket_cap(self):
        api = BucketAPI()
        media_bucket.ensure_media_bucket(api)
        image_write = next(call for call in api.calls if call[2] and call[2]["id"] == "media-images")
        self.assertEqual(image_write[2]["file_size_limit"], 52_428_800)
        self.assertFalse(image_write[2]["public"])

    def test_image_bucket_update_preserves_policy_and_never_raises_limit(self):
        existing = {
            "id": "media-images",
            "name": "legacy-images",
            "public": True,
            "file_size_limit": 90_000_000,
            "allowed_mime_types": ["image/jpeg", "image/png"],
        }
        api = BucketAPI()
        api.buckets["media-images"] = existing.copy()
        media_bucket.ensure_image_bucket(api)
        update = api.calls[1][2]
        self.assertEqual(update["file_size_limit"], 52_428_800)
        self.assertTrue(update["public"])
        self.assertEqual(update["allowed_mime_types"], ["image/jpeg", "image/png"])
        self.assertEqual(update["name"], "legacy-images")

        api = BucketAPI()
        api.buckets["media-images"] = {
            **existing, "file_size_limit": 1_000_000
        }
        media_bucket.ensure_image_bucket(api)
        self.assertEqual([call[0] for call in api.calls], ["GET"])

    def test_unbounded_image_bucket_gets_cap_and_keeps_existing_settings(self):
        api = BucketAPI()
        api.buckets["media-images"] = {
            "id": "media-images",
            "name": "media-images",
            "public": True,
            "file_size_limit": None,
            "allowed_mime_types": ["image/webp"],
        }
        media_bucket.ensure_image_bucket(api)
        self.assertEqual(api.calls[1][2]["file_size_limit"], 52_428_800)
        self.assertTrue(api.calls[1][2]["public"])
        self.assertEqual(api.calls[1][2]["allowed_mime_types"], ["image/webp"])

    def test_incomplete_image_metadata_fails_closed_before_update(self):
        api = BucketAPI()
        api.buckets["media-images"] = {
            "id": "media-images", "public": True, "file_size_limit": 90_000_000
        }
        with self.assertRaises(media_bucket.BootstrapError):
            media_bucket.ensure_image_bucket(api)
        self.assertEqual([call[0] for call in api.calls], ["GET"])

    def test_storage_opener_ignores_proxy_and_does_not_follow_redirect(self):
        observed = []

        class RedirectHandler(BaseHTTPRequestHandler):
            def do_GET(self):
                observed.append((self.path, self.headers.get("Authorization")))
                if self.path.endswith("/bucket/media-video"):
                    self.send_response(302)
                    self.send_header("Location", "/sink?token=do-not-forward")
                    self.end_headers()
                else:
                    self.send_response(200)
                    self.end_headers()

            def log_message(self, *_args):
                pass

        server = HTTPServer(("127.0.0.1", 0), RedirectHandler)
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        try:
            base_url = "http://127.0.0.1:{}".format(server.server_port)
            api = media_bucket.StorageAPI(base_url, "fixture-only-key")
            with mock.patch.dict(
                os.environ,
                {
                    "HTTP_PROXY": "http://127.0.0.1:1",
                    "http_proxy": "http://127.0.0.1:1",
                    "NO_PROXY": "",
                    "no_proxy": "",
                },
            ):
                status, _ = api.request("GET", "/bucket/media-video")
            self.assertEqual(status, 302)
            self.assertEqual(len(observed), 1)
            self.assertEqual(observed[0][0], "/bucket/media-video")
            self.assertEqual(observed[0][1], "Bearer fixture-only-key")
        finally:
            server.shutdown()
            thread.join()
            server.server_close()

    def test_non_not_found_read_status_fails_closed(self):
        class ForbiddenAPI(BucketAPI):
            def request(self, method, path, payload=None):
                self.calls.append((method, path, payload))
                return 403, {}

        with self.assertRaises(media_bucket.BootstrapError):
            media_bucket.ensure_media_bucket(ForbiddenAPI())

    def test_local_override_is_opt_in_and_does_not_change_root_limit(self):
        override = (ROOT / "docker-compose.stage8-media.yml").read_text()
        compose = (ROOT / "docker-compose.yml").read_text()
        storage_client = (ROOT / "src" / "internal" / "mediastorage" / "client.go").read_text()
        self.assertIn("FILE_SIZE_LIMIT: 80000000", override)
        self.assertNotIn("profiles: !reset []", override)
        self.assertIn("FILE_SIZE_LIMIT: ${STORAGE_FILE_SIZE_LIMIT:-52428800}", compose)
        self.assertIn('init-media-bucket.py', (ROOT / "scripts" / "init-buckets.sh").read_text())
        self.assertIn('VideoBucketID      = "media-video"', storage_client)
        self.assertIn('http://supabase-storage:5000', (ROOT / "scripts" / "init-media-bucket.py").read_text())

    def test_root_compose_can_configure_media_without_the_fixture_overlay(self):
        compose = (ROOT / "docker-compose.yml").read_text()
        self.assertIn("  stage8-media-upload:\n", compose)
        listener = compose.split("  stage8-media-upload:\n", 1)[1].split("\n  #", 1)[0]
        self.assertIn('profiles: ["stage8-media"]', listener)
        self.assertIn("host_ip: 127.0.0.1", listener)
        self.assertIn('target: 80', listener)
        self.assertIn('published: "${MEDIA_HTTP_PORT:-0}"', listener)
        self.assertNotIn("MEDIA_TLS_DIR", listener)
        self.assertIn("MEDIA_UPLOAD_ENABLED: ${MEDIA_UPLOAD_ENABLED:-false}", compose)
        self.assertIn("MEDIA_UPLOAD_PUBLIC_ORIGIN: ${MEDIA_UPLOAD_PUBLIC_ORIGIN:-}", compose)
        self.assertIn("MEDIA_PLAYBACK_ENABLED: ${MEDIA_PLAYBACK_ENABLED:-false}", compose)
        self.assertIn("MEDIA_PLAYBACK_PUBLIC_ORIGIN: ${MEDIA_PLAYBACK_PUBLIC_ORIGIN:-}", compose)
        self.assertIn("MEDIA_UPLOAD_MQTT_CLIENT_ID: ${MEDIA_UPLOAD_MQTT_CLIENT_ID:-backend_media}", compose)
        self.assertIn("UPLOAD_SIGNED_URL_EXPIRATION_TIME: ${MEDIA_UPLOAD_GRANT_TTL_SECONDS:-60}", compose)
        example = (ROOT / ".env.example").read_text().splitlines()
        for name in ("MEDIA_UPLOAD_MQTT_CLIENT_ID", "MEDIA_UPLOAD_PUBLIC_ORIGIN",
                     "STORAGE_INTERNAL_URL", "MEDIA_UPLOAD_GRANT_TTL_SECONDS",
                     "MEDIA_UPLOAD_ROUTE_KEY", "MEDIA_HTTP_PORT",
                     "STORAGE_FILE_SIZE_LIMIT", "STORAGE_LOG_LEVEL",
                     "MEDIA_PLAYBACK_ENABLED", "MEDIA_PLAYBACK_PUBLIC_ORIGIN",
                     "MEDIA_PLAYBACK_ROUTE_KEY"):
            self.assertTrue(any(line.startswith(name + "=") for line in example), name)
        self.assertFalse(any(line.startswith(("MEDIA_TLS_DIR=", "MEDIA_HTTPS_PORT="))
                             for line in example))

    def test_playback_bootstrap_service_is_in_maintenance_profile_and_depends_on_storage_healthy(self):
        compose = (ROOT / "docker-compose.yml").read_text()
        self.assertIn("  stage8-media-playback-bootstrap:\n", compose)
        section = compose.split("  stage8-media-playback-bootstrap:\n", 1)[1].split("\n  #", 1)[0]
        # Regression: must not use the shared stage8-media profile used for upload rollback
        self.assertNotIn('profiles: ["stage8-media"]', section)
        self.assertIn('profiles: ["stage8-media-playback-bootstrap"]', section)
        # Must wait for supabase-storage to be healthy (not just started) to avoid schema races
        self.assertIn("condition: service_healthy", section.split("supabase-storage:", 1)[1].split("\n    environment:", 1)[0])

    def test_signed_put_route_is_exact_opt_in_and_fail_closed(self):
        override = (ROOT / "docker-compose.stage8-media.yml").read_text()
        nginx = (ROOT / "config" / "nginx" / "stage8-media.conf.template").read_text()
        base_nginx = (ROOT / "config" / "nginx" / "nginx.conf.template").read_text()
        envoy = (ROOT / "config" / "envoy" / "lds.template.yaml").read_text()
        self.assertIn("stage8-media-upload:", override)
        self.assertIn("stage8-media.conf.template", (ROOT / "docker-compose.yml").read_text())
        self.assertIn("listen 80;", nginx)
        self.assertNotIn("listen 443 ssl;", nginx)
        self.assertNotIn("ssl_certificate", nginx)
        self.assertIn('location ~ "^/storage/v1/object/upload/sign/media-video/', nginx)
        self.assertIn("client_max_body_size 80m", nginx)
        self.assertIn("proxy_request_buffering off", nginx)
        self.assertIn("location /storage/v1/ { return 404; }", nginx)
        self.assertIn("access_log off;", nginx)
        self.assertIn("error_log /dev/null emerg;", nginx)
        self.assertIn("return 404;", base_nginx)
        self.assertIn("stage8-media-signed-put", envoy)
        self.assertIn("route=%ROUTE_NAME%", envoy)
        self.assertIn("- x-upsert", envoy)
        self.assertNotIn("X-ENVOY-ORIGINAL-PATH?:PATH", envoy)
        self.assertIn("stage8-media-blocked", envoy)
        self.assertIn("UPLOAD_SIGNED_URL_EXPIRATION_TIME", (ROOT / "docker-compose.yml").read_text())
        self.assertLess(envoy.index("name: stage8-media-signed-put"),
                        envoy.index("name: stage8-media-blocked"))
        self.assertLess(envoy.index("name: stage8-media-blocked"),
                        envoy.index("prefix: /storage/v1/"))
        self.assertIn("MEDIA_UPLOAD_ROUTE_KEY", override)
        self.assertNotIn("MEDIA_TLS_DIR", override)
        self.assertIn("target: 443", override)
        self.assertIn("stage8-media-upload-test-edge:", override)
        test_edge = (ROOT / "config" / "nginx" / "stage8-media-test-edge.conf.template").read_text()
        self.assertIn("listen 443 ssl;", test_edge)
        self.assertIn("set $api_upstream http://stage8-shared-nginx:80;", test_edge)
        self.assertIn("proxy_pass $api_upstream;", test_edge)
        fixture = (ROOT / "scripts" / "tests" / "stage8_media_storage_http_fixture.py").read_text()
        self.assertIn("TestNativeStage8SignedUpload", fixture)
        self.assertIn("NoSuchKey", fixture)
        self.assertIn("MAX_OBJECT_BYTES + 1", fixture)
        self.assertIn("token_free_logs_and_artifacts", fixture)
        self.assertIn("p2-ca.crt", fixture)

    def test_shared_nginx_media_route_is_disabled_by_default_and_proxies_to_envoy(self):
        compose = (ROOT / "docker-compose.yml").read_text()
        example = (ROOT / ".env.example").read_text()
        nginx = (ROOT / "config" / "nginx" / "nginx.conf.template").read_text()
        fixture = (ROOT / "scripts" / "tests" / "stage8_media_storage_http_fixture.py").read_text()
        nginx_service = compose.split("  nginx:\n", 1)[1].split("\n  # 2.", 1)[0]
        self.assertIn("- MEDIA_UPLOAD_ENABLED=${MEDIA_UPLOAD_ENABLED:-false}", nginx_service)
        self.assertIn("- MEDIA_UPLOAD_ROUTE_KEY=${MEDIA_UPLOAD_ROUTE_KEY:-}", compose)
        self.assertIn("MEDIA_UPLOAD_ENABLED=false", example)
        self.assertNotIn("MEDIA_UPLOAD_EDGE_ENABLED", compose + example + nginx + fixture)
        self.assertIn("location ~", nginx)
        self.assertIn("MEDIA_UPLOAD_ENABLED", nginx)
        self.assertIn("MEDIA_UPLOAD_ROUTE_KEY", nginx)
        self.assertIn("proxy_pass $supabase_upstream", nginx)
        self.assertIn("proxy_request_buffering off", nginx)
        self.assertIn("client_max_body_size 80m", nginx)
        self.assertIn('set $media_upload_enabled "${MEDIA_UPLOAD_ENABLED}";', nginx)
        self.assertIn("if ($media_upload_enabled != true) { return 404; }", nginx)
        self.assertIn('set $media_upload_route_key "${MEDIA_UPLOAD_ROUTE_KEY}";', nginx)
        self.assertIn('if ($media_upload_route_key = "") { return 404; }', nginx)
        self.assertIn("proxy_set_header X-Stage8-Media-Route $media_upload_route_key;", nginx)
        self.assertIn('proxy_set_header X-Stage8-Media-Route "";', nginx)
        self.assertIn('"MEDIA_UPLOAD_ENABLED": "false" if stage8_local_base else "true"', fixture)
        self.assertIn('services = list(smoke.fixture_services(fixture)) + ["stage8-media-upload-test-edge"]', fixture)
        self.assertNotIn('services = list(smoke.fixture_services(fixture)) + [\n                "stage8-media-upload"', fixture)

    def test_legacy_storage_route_not_shadowed_by_media_blocked(self):
        envoy = (ROOT / "config" / "envoy" / "lds.template.yaml").read_text()
        self.assertNotIn(
            "- name: stage8-media-blocked\n                        match:\n                          prefix: /storage/v1/\n",
            envoy,
        )

    def test_envoy_media_blocked_discriminates_bucket_position_not_arbitrary_substring(self):
        import re
        envoy = (ROOT / "config" / "envoy" / "lds.template.yaml").read_text()
        # Locate the regex used in stage8-media-blocked
        match = re.search(
            r"- name: stage8-media-blocked\s+match:\s+safe_regex:\s+google_re2: \{\}\s+regex: '([^']+)'",
            envoy,
        )
        self.assertIsNotNone(match, "stage8-media-blocked regex not found in lds.template.yaml")
        raw_regex = match.group(1)
        compiled = re.compile(raw_regex)

        # Legacy operations in media-images where media-video is part of the object name
        # (both exact basename and nested segment) must NOT match the media-blocked route:
        self.assertIsNone(compiled.match("/storage/v1/object/media-images/media-video.png"),
                          "Exact basename media-video.png in media-images must not be blocked")
        self.assertIsNone(compiled.match("/storage/v1/object/media-images/nested/media-video/thumb.png"),
                          "Nested segment media-video in media-images must not be blocked")
        self.assertIsNone(compiled.match("/storage/v1/object/authenticated/media-images/media-video.png"),
                          "Authenticated exact basename in media-images must not be blocked")
        self.assertIsNone(compiled.match("/storage/v1/object/authenticated/media-images/nested/media-video/thumb.png"),
                          "Authenticated nested segment in media-images must not be blocked")
        self.assertIsNone(compiled.match("/storage/v1/bucket/media-images"),
                          "Bucket media-images must not be blocked")

        # Operations targeting media-video as the bucket MUST match the media-blocked route:
        self.assertIsNotNone(compiled.match("/storage/v1/bucket/media-video"),
                             "Bucket media-video must be blocked")
        self.assertIsNotNone(compiled.match("/storage/v1/object/media-video/file.mp4"),
                             "Direct object media-video must be blocked")
        self.assertIsNotNone(compiled.match("/storage/v1/object/authenticated/media-video/file.mp4"),
                             "Authenticated media-video must be blocked")
        self.assertIsNotNone(compiled.match("/storage/v1/object/upload/sign/media-video/file.mp4"),
                             "Upload sign media-video without header must be blocked")
        self.assertIsNotNone(compiled.match("/storage/v1/object/list/media-video"),
                             "Object list media-video must be blocked")


class Stage8PlaybackSupportTests(unittest.TestCase):
    def test_fixture_cli_help_includes_playback_api_flag(self):
        fixture_text = (ROOT / "scripts" / "tests" / "stage8_media_storage_http_fixture.py").read_text()
        self.assertIn("--stage8-local-playback-api", fixture_text)
        self.assertIn("--stage8-local-playback", fixture_text)
        self.assertIn("--stage8-local-playback-browser", fixture_text)

    def test_browser_runner_script_and_demo_mount_exist(self):
        runner_path = ROOT / "scripts" / "tests" / "stage8_browser_playback_runner.js"
        self.assertTrue(runner_path.exists(), "stage8_browser_playback_runner.js must exist")
        override = (ROOT / "docker-compose.stage8-media.yml").read_text()
        self.assertIn("./docs/examples/media-playback/index.html:/var/www/demo/index.html:ro", override)
        self.assertIn("./docs/examples/media-playback/player.js:/var/www/demo/player.js:ro", override)
        edge_template = (ROOT / "config" / "nginx" / "stage8-media-test-edge.conf.template").read_text()
        self.assertIn("location = /demo/index.html", edge_template)
        self.assertIn("location = /demo/player.js", edge_template)

    def test_fixture_playback_tls_edge_configured_correctly(self):
        override = (ROOT / "docker-compose.stage8-media.yml").read_text()
        self.assertIn("stage8-media-playback-test-edge:", override)
        edge_template_path = ROOT / "config" / "nginx" / "stage8-media-playback-test-edge.conf.template"
        self.assertTrue(edge_template_path.exists(), "stage8-media-playback-test-edge.conf.template must exist")
        edge_template = edge_template_path.read_text()
        self.assertIn("listen 443 ssl;", edge_template)
        self.assertIn("set $playback_upstream http://stage8-shared-nginx:8081;", edge_template)
        self.assertIn("proxy_pass $playback_upstream;", edge_template)

    def test_verify_signed_url_in_memory(self):
        import base64
        import datetime
        import sys
        import time
        sys.path.insert(0, str(ROOT / "scripts" / "tests"))
        from stage8_playback_support import verify_signed_url_in_memory, PlaybackProofFailure

        now = int(time.time())
        exp = now + 120
        payload = base64.urlsafe_b64encode(f'{{"exp":{exp}}}'.encode()).decode().rstrip("=")
        fake_token = f"header.{payload}.sig"
        media_id = "00000000-0000-0000-0000-000000000001"
        url = f"https://playback.local.invalid/storage/v1/object/sign/media-video/{media_id}.mp4?token={fake_token}"
        expires_at = datetime.datetime.fromtimestamp(exp, tz=datetime.timezone.utc).isoformat().replace("+00:00", "Z")

        tok = verify_signed_url_in_memory(url, expires_at, "playback.local.invalid", media_id)
        self.assertEqual(tok, fake_token)

        # Invalid scheme
        with self.assertRaises(PlaybackProofFailure):
            verify_signed_url_in_memory(url.replace("https://", "http://"), expires_at, "playback.local.invalid", media_id)

        # Origin mismatch
        with self.assertRaises(PlaybackProofFailure):
            verify_signed_url_in_memory(url, expires_at, "other.origin.invalid", media_id)

        # Fragment present
        with self.assertRaises(PlaybackProofFailure):
            verify_signed_url_in_memory(url + "#frag", expires_at, "playback.local.invalid", media_id)

        # Extra query parameter
        with self.assertRaises(PlaybackProofFailure):
            verify_signed_url_in_memory(url + "&extra=1", expires_at, "playback.local.invalid", media_id)

    def test_get_header_case_insensitive(self):
        import sys
        sys.path.insert(0, str(ROOT / "scripts" / "tests"))
        from stage8_playback_support import get_header
        headers = {"cache-control": "no-store", "Content-Type": "application/json"}
        self.assertEqual(get_header(headers, "Cache-Control"), "no-store")
        self.assertEqual(get_header(headers, "content-type"), "application/json")
        self.assertIsNone(get_header(headers, "Authorization"))


class Stage8PlaybackDataPlaneContractTests(unittest.TestCase):
    def test_env_example_and_compose_defaults(self):
        example = (ROOT / ".env.example").read_text().splitlines()
        self.assertTrue(any(line.startswith("MEDIA_PLAYBACK_ROUTE_KEY=") for line in example),
                        "MEDIA_PLAYBACK_ROUTE_KEY must be in .env.example")
        self.assertTrue(any(line.startswith("MEDIA_PLAYBACK_ENABLED=false") for line in example),
                        "MEDIA_PLAYBACK_ENABLED=false must be in .env.example")
        smoke_text = (ROOT / "scripts" / "tests" / "native_compose_smoke.py").read_text()
        self.assertIn('MEDIA_PLAYBACK_ROUTE_KEY=""', smoke_text)

    def test_playback_listener_topology_and_port(self):
        compose = (ROOT / "docker-compose.yml").read_text()
        nginx_service = compose.split("  nginx:\n", 1)[1].split("\n  # 2.", 1)[0]
        # Only port 80:80 published on host, no host ports added for 8081
        self.assertIn('ports:\n      - "80:80"\n', nginx_service)
        self.assertNotIn("8081:8081", compose)
        self.assertNotIn('published: "8081"', compose)
        self.assertNotIn("published: 8081", compose)
        # Environment variables passed to Nginx and Envoy
        self.assertIn("- MEDIA_PLAYBACK_ENABLED=${MEDIA_PLAYBACK_ENABLED:-false}", nginx_service)
        self.assertIn("- MEDIA_PLAYBACK_ROUTE_KEY=${MEDIA_PLAYBACK_ROUTE_KEY:-}", nginx_service)
        envoy_service = compose.split("  supabase-envoy:\n", 1)[1].split("\n  # 5.", 1)[0]
        self.assertIn("MEDIA_PLAYBACK_ROUTE_KEY: ${MEDIA_PLAYBACK_ROUTE_KEY:-}", envoy_service)
        # Volume mount for shared playback config template and entrypoint validation hook
        self.assertIn("stage8-media-playback.conf.template", nginx_service)
        self.assertIn("18-validate-playback-key.envsh", nginx_service)

    def test_nginx_entrypoint_envsh_hook(self):
        hook_path = ROOT / "config" / "nginx" / "18-validate-playback-key.envsh"
        self.assertTrue(hook_path.exists(), "18-validate-playback-key.envsh must exist")
        self.assertTrue(os.access(hook_path, os.X_OK), "18-validate-playback-key.envsh must be executable")
        content = hook_path.read_text()
        self.assertNotIn("echo", content, "Hook must not log or print anything")
        self.assertNotIn("printf", content, "Hook must not log or print anything")

        # Test hook evaluation with various keys via sh
        import subprocess
        def eval_hook(key_val, enabled_val="true"):
            env = dict(os.environ, MEDIA_PLAYBACK_ROUTE_KEY=key_val, MEDIA_PLAYBACK_ENABLED=enabled_val)
            cmd = [
                "/bin/sh", "-ec",
                f'. "{hook_path}"; echo "KEY=$MEDIA_PLAYBACK_ROUTE_KEY;ENABLED=$MEDIA_PLAYBACK_ENABLED"'
            ]
            res = subprocess.run(cmd, env=env, capture_output=True, text=True, check=True)
            return res.stdout.strip()

        # Valid 64-hex key preserved
        valid_key = "a" * 64
        self.assertEqual(eval_hook(valid_key), f"KEY={valid_key};ENABLED=true")

        # Short/long/invalid chars/quotes/newlines/pipes disabled cleanly
        for bad_key in ["a" * 63, "a" * 65, "A" * 64, "bad'quote", 'bad"quote', "bad|pipe", "bad\nnewline", "bad\\backslash"]:
            self.assertEqual(eval_hook(bad_key), "KEY=;ENABLED=false")

    def test_playback_config_shared_template(self):
        tpl_path = ROOT / "config" / "nginx" / "stage8-media-playback.conf.template"
        self.assertTrue(tpl_path.exists(), "stage8-media-playback.conf.template must exist")
        tpl = tpl_path.read_text()
        # Internal fixed port 8081
        self.assertIn("listen 8081;", tpl)
        # Bounded active connections keyed to listener/server
        self.assertIn("limit_conn_zone $server_name zone=stage8_playback_conn:10m;", tpl)
        self.assertIn("limit_conn stage8_playback_conn 64;", tpl)
        # Never log signed query in success, deny or upstream failure
        self.assertIn("access_log off;", tpl)
        self.assertIn("error_log /dev/null emerg;", tpl)
        # Only canonical lowercase UUID path
        self.assertIn('location ~ "^/storage/v1/object/sign/media-video/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\\.mp4$"', tpl)
        # Fail closed on feature flag and 64 lowercase hex key
        self.assertIn('set $media_playback_enabled "${MEDIA_PLAYBACK_ENABLED}";', tpl)
        self.assertIn('set $media_playback_route_key "${MEDIA_PLAYBACK_ROUTE_KEY}";', tpl)
        self.assertIn("if ($media_playback_enabled != true) { return 404; }", tpl)
        self.assertIn('if ($media_playback_route_key !~ "^[0-9a-f]{64}$") { return 404; }', tpl)
        # Pass exact GET only to start; leave HEAD closed
        self.assertIn("if ($request_method != GET) { return 405; }", tpl)
        # Require raw request_uri to match canonical path and 3-part base64url JWT query token
        self.assertIn('if ($request_uri !~ "^/storage/v1/object/sign/media-video/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\\.mp4\\?token=[A-Za-z0-9_-]+\\.[A-Za-z0-9_-]+\\.[A-Za-z0-9_-]+$")', tpl)
        # Whitelist forwarded headers via proxy_pass_request_headers off
        self.assertIn("proxy_pass_request_headers off;", tpl)
        self.assertIn("proxy_pass_request_body off;", tpl)
        self.assertIn("client_max_body_size 1k;", tpl)
        # Explicit proxy_cache off, gzip off, hide upstream Cache-Control
        self.assertIn("proxy_cache off;", tpl)
        self.assertIn("gzip off;", tpl)
        self.assertIn("proxy_hide_header Cache-Control;", tpl)
        # Cache control always private, no-store at server level including denied paths
        self.assertIn('add_header Cache-Control "private, no-store" always;', tpl)
        # Forward only controlled Host/guard and Range/If-Range
        self.assertIn("proxy_set_header Host $host;", tpl)
        self.assertIn("proxy_set_header Range $http_range;", tpl)
        self.assertIn("proxy_set_header If-Range $http_if_range;", tpl)
        self.assertIn("proxy_set_header X-Stage8-Media-Playback-Route $media_playback_route_key;", tpl)
        # Suppress external auth
        self.assertIn('proxy_set_header Authorization "";', tpl)
        self.assertIn('proxy_set_header apikey "";', tpl)
        self.assertIn('proxy_set_header Content-Length "";', tpl)
        self.assertIn('proxy_set_header Content-Type "";', tpl)
        # No shared cache / whole-file buffering
        self.assertIn("proxy_buffering off;", tpl)
        self.assertIn("proxy_request_buffering off;", tpl)
        # Default deny all other paths
        self.assertIn("location / {", tpl)
        self.assertIn("return 404;", tpl)

    def test_common_origin_strips_guard_and_suppresses_logs_on_denials(self):
        base_nginx = (ROOT / "config" / "nginx" / "nginx.conf.template").read_text()
        p2_nginx = (ROOT / "config" / "nginx" / "paper-p2.conf.template").read_text()
        p7_nginx = (ROOT / "config" / "nginx" / "paper-p7-ws.conf.template").read_text()
        for name, text in [("base_nginx", base_nginx), ("p2_nginx", p2_nginx), ("p7_nginx", p7_nginx)]:
            # Strip incoming playback guard header on common origin
            self.assertIn('proxy_set_header X-Stage8-Media-Playback-Route "";', text,
                          f"{name} must strip incoming X-Stage8-Media-Playback-Route")
            # Suppress request/error logging on signed media read denials
            self.assertIn("location ~* ^/storage/v1/object/sign/media-video", text,
                          f"{name} must have a denial location for signed media-video")
            storage_deny_block = text.split("location ~* ^/storage/v1/object/sign/media-video", 1)[1].split("}", 1)[0]
            self.assertIn("access_log off;", storage_deny_block,
                          f"{name} must turn access_log off on signed media read denials")
            self.assertIn("error_log /dev/null emerg;", storage_deny_block,
                          f"{name} must suppress error_log on signed media read denials")
            self.assertIn("return 404;", storage_deny_block,
                          f"{name} must return 404 on signed media read denials")

    def test_envoy_playback_route_and_security(self):
        envoy = (ROOT / "config" / "envoy" / "lds.template.yaml").read_text()
        entrypoint = (ROOT / "config" / "envoy" / "docker-entrypoint.sh").read_text()
        # Entrypoint sanitizes MEDIA_PLAYBACK_ROUTE_KEY before sed substitution
        self.assertIn("SAFE_MEDIA_PLAYBACK_ROUTE_KEY", entrypoint)
        self.assertIn(r"s|\${MEDIA_PLAYBACK_ROUTE_KEY}|${SAFE_MEDIA_PLAYBACK_ROUTE_KEY}|g", entrypoint)
        # Route exists and placed before stage8-media-blocked
        self.assertIn("name: stage8-media-signed-get", envoy)
        self.assertIn("name: stage8-media-blocked", envoy)
        self.assertLess(envoy.index("name: stage8-media-signed-get"),
                        envoy.index("name: stage8-media-blocked"),
                        "stage8-media-signed-get must be ahead of stage8-media-blocked")
        # Route regex and method
        get_block = envoy.split("name: stage8-media-signed-get", 1)[1].split("name: stage8-media-blocked", 1)[0]
        self.assertIn("regex: '^/storage/v1/object/sign/media-video/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\\.mp4$'", get_block)
        self.assertIn("exact: GET", get_block)
        self.assertNotIn("exact: HEAD", get_block)
        # Route requires guard header
        self.assertIn("name: x-stage8-media-playback-route", get_block)
        self.assertIn("regex: '^.+$'", get_block)
        self.assertIn("exact: '${MEDIA_PLAYBACK_ROUTE_KEY}'", get_block)
        # Guard stripped before upstream, plus cookie and both media guards stripped
        headers_removed = get_block.split("request_headers_to_remove:", 1)[1].split("typed_per_filter_config:", 1)[0]
        self.assertIn("- cookie", headers_removed)
        self.assertIn("- x-stage8-media-route", headers_removed)
        self.assertIn("- x-stage8-media-playback-route", headers_removed)
        # Access log format must remain metadata-only (no :path or query)
        self.assertNotIn("%REQ(:PATH)%", envoy)
        self.assertNotIn("%REQ(X-ENVOY-ORIGINAL-PATH?:PATH)%", envoy)

    def test_regex_matching_rules(self):
        import re
        # Raw request_uri regex: canonical lowercase UUID path + 3-part base64url JWT query token
        uri_re = re.compile(r"^/storage/v1/object/sign/media-video/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.mp4\?token=[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$")

        valid_uri = "/storage/v1/object/sign/media-video/01234567-89ab-cdef-0123-456789abcdef.mp4?token=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.sig"
        self.assertTrue(uri_re.match(valid_uri))

        # Rejected: Non-JWT query (single part or two parts)
        self.assertFalse(uri_re.match("/storage/v1/object/sign/media-video/01234567-89ab-cdef-0123-456789abcdef.mp4?token=notajwt"))
        self.assertFalse(uri_re.match("/storage/v1/object/sign/media-video/01234567-89ab-cdef-0123-456789abcdef.mp4?token=part1.part2"))
        self.assertFalse(uri_re.match("/storage/v1/object/sign/media-video/01234567-89ab-cdef-0123-456789abcdef.mp4?token=part1.part2.part3.part4"))

        # Rejected: Normalized raw path aliases
        self.assertFalse(uri_re.match("//storage/v1/object/sign/media-video/01234567-89ab-cdef-0123-456789abcdef.mp4?token=a.b.c"))
        self.assertFalse(uri_re.match("/storage/v1/object/sign/media-video/./01234567-89ab-cdef-0123-456789abcdef.mp4?token=a.b.c"))
        self.assertFalse(uri_re.match("/storage/v1/object/sign/media-video/../media-video/01234567-89ab-cdef-0123-456789abcdef.mp4?token=a.b.c"))

        # Rejected: Uppercase UUID
        self.assertFalse(uri_re.match("/storage/v1/object/sign/media-video/01234567-89AB-CDEF-0123-456789ABCDEF.mp4?token=a.b.c"))

        # Rejected: Duplicate token query
        self.assertFalse(uri_re.match(valid_uri + "&token=a.b.c"))

        # Rejected: Foreign query parameter
        self.assertFalse(uri_re.match(valid_uri + "&extra=1"))
        self.assertFalse(uri_re.match(valid_uri.replace("?token=", "?other=1&token=")))

        # Rejected: Alternate raw encoding
        self.assertFalse(uri_re.match("/storage/v1/object/sign/media-video/01234567-89ab-cdef-0123-456789abcdef.mp4?token%3Da.b.c"))
        self.assertFalse(uri_re.match("/storage/v1/object/sign/media-video/01234567-89ab-cdef-0123-456789abcdef.mp4?%74oken=a.b.c"))

        # Rejected: Empty token
        self.assertFalse(uri_re.match("/storage/v1/object/sign/media-video/01234567-89ab-cdef-0123-456789abcdef.mp4?token="))
        self.assertFalse(uri_re.match("/storage/v1/object/sign/media-video/01234567-89ab-cdef-0123-456789abcdef.mp4"))

        # Route key regex: 64 lowercase hex
        key_re = re.compile(r"^[0-9a-f]{64}$")
        self.assertTrue(key_re.match("a" * 64))
        self.assertTrue(key_re.match("0123456789abcdef" * 4))
        self.assertFalse(key_re.match("A" * 64))
        self.assertFalse(key_re.match("a" * 63))
        self.assertFalse(key_re.match("a" * 65))
        self.assertFalse(key_re.match("g" * 64))
        self.assertTrue(key_re.match("a" * 64))
        self.assertTrue(key_re.match("0123456789abcdef" * 4))
        # Uppercase rejected
        self.assertFalse(key_re.match("A" * 64))
        # Shorter or longer rejected
        self.assertFalse(key_re.match("a" * 63))
        self.assertFalse(key_re.match("a" * 65))
        # Non-hex rejected
        self.assertFalse(key_re.match("g" * 64))


if __name__ == "__main__":
    unittest.main()
