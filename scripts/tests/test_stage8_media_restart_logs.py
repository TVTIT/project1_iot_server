import http.server
import importlib.util
import json
import pathlib
import shutil
import tempfile
import threading
import time
import unittest
import urllib.request


ROOT = pathlib.Path(__file__).resolve().parents[2]
FIXTURE_PATH = ROOT / "scripts" / "tests" / "stage8_media_storage_http_fixture.py"
SPEC = importlib.util.spec_from_file_location("stage8_media_http_fixture", FIXTURE_PATH)
fixture = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(fixture)


class FakeClock:
    def __init__(self):
        self.now = 0.0

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


class Stage8RestartLogTests(unittest.TestCase):
    def test_counter_only_counts_the_named_media_put_route(self):
        logs = "\n".join((
            '... "PUT HTTP/1.1" 200 12 route=auth-admin',
            '... "PUT HTTP/1.1" 200 12 route=stage8-media-signed-put-extra',
            '... "PUT HTTP/1.1" 200 12 route=stage8-media-signed-put',
            '... "GET HTTP/1.1" 200 12 route=stage8-media-signed-put',
        ))
        self.assertEqual(fixture.fixture_media_put_count(logs), 1)

    def test_wait_observes_delayed_log_visibility(self):
        clock = FakeClock()
        counts = iter((0, 0, 1))
        result = fixture.wait_for_media_put_count(
            lambda: next(counts), expected=1, timeout=5, poll_interval=0.5,
            monotonic=clock.monotonic, sleep=clock.sleep,
        )
        self.assertEqual(result, {"count": 1, "observations": 3, "wait_seconds": 1.0})

    def test_wait_rejects_duplicate_put_immediately(self):
        clock = FakeClock()
        with self.assertRaisesRegex(
            fixture.ProofFailure, "fixture_media_put_duplicate_count_2"
        ):
            fixture.wait_for_media_put_count(
                lambda: 2, expected=1, timeout=5,
                monotonic=clock.monotonic, sleep=clock.sleep,
            )

    def test_quiescence_window_rejects_late_duplicate_put(self):
        clock = FakeClock()
        counts = iter((1, 1, 2))
        with self.assertRaisesRegex(
            fixture.ProofFailure, "fixture_media_put_duplicate_count_2"
        ):
            fixture.wait_for_media_put_quiescence(
                lambda: next(counts), expected=1, quiet_seconds=4, timeout=35,
                poll_interval=0.5, monotonic=clock.monotonic, sleep=clock.sleep,
            )


class UpstreamStorageHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, _format, *_args):
        return

    def do_GET(self):
        self._respond(b"upstream-storage-payload")

    def do_POST(self):
        self._respond(b"upstream-post-payload")

    def _respond(self, body):
        self.server.requests.append((self.command, self.path))
        self.send_response(200)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class Stage8StorageHoldProxyTests(unittest.TestCase):
    def setUp(self):
        self.upstream = http.server.ThreadingHTTPServer(("127.0.0.1", 0), UpstreamStorageHandler)
        self.upstream.requests = []
        self.upstream_thread = threading.Thread(target=self.upstream.serve_forever, daemon=True)
        self.upstream_thread.start()
        self.upstream_port = self.upstream.server_address[1]
        self.proxy = None
        self.temp_dir = pathlib.Path(tempfile.mkdtemp(prefix="stage8-proxy-test-"))

    def tearDown(self):
        if self.proxy is not None:
            self.proxy.close()
        self.upstream.shutdown()
        self.upstream.server_close()
        self.upstream_thread.join(timeout=5)
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _request(self, method, path, headers=None, port=None):
        req_port = port if port is not None else self.proxy.port
        url = f"http://127.0.0.1:{req_port}{path}"
        req = urllib.request.Request(url, method=method, headers=headers or {})
        with urllib.request.urlopen(req, timeout=10) as response:
            return response.status, response.read()

    def test_first_matched_read_held_until_explicit_release_and_later_exact_reads_pass(self):
        self.proxy = fixture.FirstStorageReadHoldProxy(
            self.upstream_port, "127.0.0.1", target_path="test-clip.mp4",
        )
        self.proxy.start()

        target_url_path = "/object/authenticated/media-video/test-clip.mp4"
        secret_token = "secret-token-xyz-123"

        response_holder = []
        finished = threading.Event()

        def background_client():
            status, payload = self._request(
                "GET", target_url_path,
                headers={"Authorization": f"Bearer {secret_token}"},
            )
            response_holder.append((status, payload))
            finished.set()

        client_thread = threading.Thread(target=background_client, daemon=True)
        client_thread.start()

        # Proves first matched read is held
        held = self.proxy.wait_held(timeout=3.0)
        self.assertTrue(held, "first matched read was not held within timeout")
        self.assertTrue(self.proxy.is_held)
        self.assertFalse(finished.is_set(), "client completed while proxy was still held")

        # Call explicit release
        self.proxy.release()
        client_thread.join(timeout=3.0)
        self.assertTrue(finished.is_set(), "client did not finish after explicit release")
        self.assertEqual(response_holder[0], (200, b"upstream-storage-payload"))

        # Later exact matching read passes immediately without hold
        start_time = time.monotonic()
        status2, payload2 = self._request(
            "GET", target_url_path,
            headers={"Authorization": f"Bearer {secret_token}"},
        )
        duration2 = time.monotonic() - start_time
        self.assertEqual(status2, 200)
        self.assertEqual(payload2, b"upstream-storage-payload")
        self.assertLess(duration2, 1.0, "second read should pass through without hold")

        # Evidence only contains path/count, no tokens
        evidence = self.proxy.evidence()
        self.assertEqual(evidence["matched_path"], target_url_path)
        self.assertEqual(evidence["matched_get_count"], 2)
        self.assertTrue(evidence["first_read_held"])
        self.assertTrue(evidence["released"])
        self.assertEqual(evidence["release_reason"], "explicit")
        evidence_str = json.dumps(evidence)
        self.assertNotIn(secret_token, evidence_str)
        self.assertNotIn("Authorization", evidence_str)
        self.assertNotIn("Bearer", evidence_str)

    def test_hold_deadline_releases_when_not_explicitly_released(self):
        self.proxy = fixture.FirstStorageReadHoldProxy(
            self.upstream_port, "127.0.0.1",
            target_path="test-clip.mp4",
            hold_deadline_seconds=0.2,
        )
        self.proxy.start()

        target_url_path = "/object/authenticated/media-video/test-clip.mp4"
        start_time = time.monotonic()
        status, payload = self._request("GET", target_url_path)
        elapsed = time.monotonic() - start_time

        self.assertEqual(status, 200)
        self.assertEqual(payload, b"upstream-storage-payload")
        self.assertGreaterEqual(elapsed, 0.18, "should hold until deadline")

        evidence = self.proxy.evidence()
        self.assertEqual(evidence["matched_get_count"], 1)
        self.assertTrue(evidence["first_read_held"])
        self.assertTrue(evidence["released"])
        self.assertEqual(evidence["release_reason"], "deadline")

    def test_unrelated_method_and_public_reads_unaffected(self):
        self.proxy = fixture.FirstStorageReadHoldProxy(
            self.upstream_port, "127.0.0.1", target_path="test-clip.mp4",
        )
        self.proxy.start()

        # Unrelated path: other clip
        s1, p1 = self._request("GET", "/object/authenticated/media-video/other-clip.mp4")
        self.assertEqual((s1, p1), (200, b"upstream-storage-payload"))

        # Unrelated path: bucket query
        s2, p2 = self._request("GET", "/bucket/media-video")
        self.assertEqual((s2, p2), (200, b"upstream-storage-payload"))

        # Unrelated method: POST on target path
        s3, p3 = self._request("POST", "/object/authenticated/media-video/test-clip.mp4")
        self.assertEqual((s3, p3), (200, b"upstream-post-payload"))

        # Public read: unauthenticated object route
        s4, p4 = self._request("GET", "/object/public/media-video/test-clip.mp4")
        self.assertEqual((s4, p4), (200, b"upstream-storage-payload"))

        # Verify proxy was never held
        self.assertFalse(self.proxy.is_held)
        evidence = self.proxy.evidence()
        self.assertEqual(evidence["matched_get_count"], 0)
        self.assertFalse(evidence["first_read_held"])

    def test_auto_bind_canonical_uuid_path_and_unrelated_paths_do_not_bind(self):
        self.proxy = fixture.FirstStorageReadHoldProxy(
            self.upstream_port, "127.0.0.1",
        )
        self.proxy.start()

        # Unrelated paths do not bind and do not count
        s1, _ = self._request("GET", "/bucket/media-video")
        self.assertEqual(s1, 200)
        self.assertIsNone(self.proxy.target_path)
        self.assertEqual(self.proxy.matched_get_count, 0)

        s2, _ = self._request("GET", "/object/authenticated/media-video/not-a-uuid.mp4")
        self.assertEqual(s2, 200)
        self.assertIsNone(self.proxy.target_path)
        self.assertEqual(self.proxy.matched_get_count, 0)

        # Canonical UUID path binds atomically and is held
        target_uuid_path = "/object/authenticated/media-video/11111111-2222-3333-4444-555555555555.mp4"
        finished = threading.Event()

        def background_client():
            self._request("GET", target_uuid_path)
            finished.set()

        client_thread = threading.Thread(target=background_client, daemon=True)
        client_thread.start()

        self.assertTrue(self.proxy.wait_held(timeout=3.0))
        self.assertEqual(self.proxy.target_path, target_uuid_path)
        self.assertEqual(self.proxy.matched_get_count, 1)

        # Release first GET
        self.proxy.release()
        client_thread.join(timeout=3.0)
        self.assertTrue(finished.is_set())

        # Later GET for the same bound path passes and counts
        s3, _ = self._request("GET", target_uuid_path)
        self.assertEqual(s3, 200)
        self.assertEqual(self.proxy.matched_get_count, 2)

        # GET for a different UUID path does not match the bound path
        different_uuid_path = "/object/authenticated/media-video/99999999-8888-7777-6666-555555555555.mp4"
        s4, _ = self._request("GET", different_uuid_path)
        self.assertEqual(s4, 200)
        self.assertEqual(self.proxy.matched_get_count, 2)

    def test_legacy_delay_proxy_behavior_preserved(self):
        # Legacy FirstStorageReadDelayProxy still behaves as expected
        delay_proxy = fixture.FirstStorageReadDelayProxy(
            self.upstream_port, "127.0.0.1", delay_seconds=0.05,
        )
        delay_proxy.start()
        try:
            status, payload = self._request(
                "GET", "/object/authenticated/media-video/any.mp4",
                port=delay_proxy.port,
            )
            self.assertEqual(status, 200)
            self.assertEqual(payload, b"upstream-storage-payload")
            self.assertTrue(delay_proxy.first_get_delayed.is_set())
            self.assertEqual(delay_proxy.get_count, 1)
        finally:
            delay_proxy.close()


if __name__ == "__main__":
    unittest.main()
