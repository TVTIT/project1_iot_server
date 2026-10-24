"""Keep active builder, module and CI toolchain pins aligned."""
from pathlib import Path
import re
import unittest

ROOT = Path(__file__).resolve().parents[2]


class GoToolchainTests(unittest.TestCase):
    def test_active_toolchain_pins(self):
        version = '1.27.3'
        self.assertIn('go ' + version + '\n', (ROOT / 'src/go.mod').read_text())
        self.assertIn('FROM golang:' + version + '-alpine AS builder',
                      (ROOT / 'src/Dockerfile').read_text())
        self.assertIn('FROM golang:' + version + '-alpine AS builder',
                      (ROOT / 'scripts/spikes/stage3-task30/sqlite/Dockerfile').read_text())
        ci = (ROOT / '.github/workflows/ci.yml').read_text()
        pins = re.findall(r"go-version:\s*'([^']+)'", ci)
        self.assertTrue(pins)
        self.assertEqual(set(pins), {version})


if __name__ == '__main__':
    unittest.main()
