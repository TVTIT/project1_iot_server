import importlib.util
import pathlib
import unittest


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


if __name__ == "__main__":
    unittest.main()
