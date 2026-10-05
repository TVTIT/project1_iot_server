"""Current-source helper builds must work with an empty, run-owned directory."""
import os
from pathlib import Path
import tempfile
import unittest

from task26_harness_helpers import build_helper


class BuildContract(unittest.TestCase):
    def test_snapshot_build_from_empty_directory(self):
        with tempfile.TemporaryDirectory(prefix='task26-build-') as directory:
            binary = build_helper('snapshot', Path(directory))
            self.assertEqual(binary.parent, Path(directory))
            self.assertTrue(binary.is_file())
            self.assertTrue(os.access(binary, os.X_OK))
            self.assertEqual(binary.stat().st_mode & 0o077, 0)

    def test_unknown_helper_fails_closed(self):
        with tempfile.TemporaryDirectory(prefix='task26-build-') as directory:
            with self.assertRaises(ValueError):
                build_helper('../stale', Path(directory))


if __name__ == '__main__':
    unittest.main()
