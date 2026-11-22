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
        self.assertIn("MEDIA_UPLOAD_MQTT_CLIENT_ID: ${MEDIA_UPLOAD_MQTT_CLIENT_ID:-backend_media}", compose)
        self.assertIn("UPLOAD_SIGNED_URL_EXPIRATION_TIME: ${MEDIA_UPLOAD_GRANT_TTL_SECONDS:-60}", compose)
        example = (ROOT / ".env.example").read_text().splitlines()
        for name in ("MEDIA_UPLOAD_MQTT_CLIENT_ID", "MEDIA_UPLOAD_PUBLIC_ORIGIN",
                     "STORAGE_INTERNAL_URL", "MEDIA_UPLOAD_GRANT_TTL_SECONDS",
                     "MEDIA_UPLOAD_ROUTE_KEY", "MEDIA_HTTP_PORT",
                     "STORAGE_FILE_SIZE_LIMIT", "STORAGE_LOG_LEVEL"):
            self.assertTrue(any(line.startswith(name + "=") for line in example), name)
        self.assertFalse(any(line.startswith(("MEDIA_TLS_DIR=", "MEDIA_HTTPS_PORT="))
                             for line in example))

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


if __name__ == "__main__":
    unittest.main()
