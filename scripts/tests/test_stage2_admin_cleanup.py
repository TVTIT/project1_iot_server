"""Run the real shell cleanup function against a non-Docker command stub."""
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]


class AdminCleanupTests(unittest.TestCase):
    def cleanup(self, mode):
        source = (ROOT / 'scripts/test-stage2-admin.sh').read_text()
        function = source[source.index('cleanup() {'):source.index('trap cleanup EXIT')]
        with tempfile.TemporaryDirectory(dir='/var/tmp/opencode') as directory:
            root = Path(directory)
            cid = 'a' * 64
            (root / 'container.cid').write_text(cid)
            driver = r'''
RUN_ID=fixture_admin
CONTAINER=fixture_admin
VOLUME=fixture_admin-data
created=true
volume_created=true
CID_FILE="$1/container.cid"
CALLS="$1/calls"
MODE="$2"
docker() {
    printf '%s\n' "$*" >> "$CALLS"
    case "$1 $2" in
        "inspect -f")
            if [ "$MODE" = foreign ]; then echo '/fixture_admin foreign';
            else echo '/fixture_admin fixture_admin'; fi ;;
        "volume inspect")
            if [ "$MODE" = foreign ]; then echo foreign; else echo fixture_admin; fi ;;
        "container ls") : ;;
        "volume ls") [ "$MODE" != remaining ] || echo fixture_admin-data ;;
        "rm -f"|"rm -fv") [ "$MODE" != daemon_error ] ;;
        "volume rm") [ "$MODE" != daemon_error ] ;;
        *) return 1 ;;
    esac
}
'''
            result = subprocess.run(['sh', '-c', driver + function + '\ncleanup\n', 'test', str(root), mode],
                                    capture_output=True, text=True, timeout=10)
            calls = (root / 'calls').read_text().splitlines()
            return result, calls, cid

    def test_cleanup_uses_owned_exact_id_and_named_volume_and_verifies_absence(self):
        result, calls, cid = self.cleanup('owned')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(any(call in ('rm -f -v ' + cid, 'rm -fv ' + cid) for call in calls))
        self.assertIn('volume rm fixture_admin-data', calls)
        self.assertTrue(any(call.startswith('container ls ') for call in calls))
        self.assertTrue(any(call.startswith('volume ls ') for call in calls))

    def test_foreign_receipt_never_authorizes_removal(self):
        result, calls, _ = self.cleanup('foreign')
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(any(call.startswith(('rm ', 'volume rm ')) for call in calls))

    def test_remaining_volume_and_daemon_errors_veto_success(self):
        for mode in ('remaining', 'daemon_error'):
            with self.subTest(mode=mode):
                result, _, _ = self.cleanup(mode)
                self.assertNotEqual(result.returncode, 0)


if __name__ == '__main__':
    unittest.main()
