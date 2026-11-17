#!/usr/bin/env python3
"""Focused tests for fixture-owned Mosquitto startup diagnostics."""

import contextlib
import io
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts/tests"))

import stage3_mqtt_ingress_runner as R  # noqa: E402


class BrokerStartupDiagnosticTests(unittest.TestCase):
    def test_diagnostic_inspects_owned_state_access_and_combined_logs(self):
        with tempfile.TemporaryDirectory() as temp:
            work = Path(temp)
            output = io.StringIO()
            with patch.object(R, "docker", side_effect=[
                b"status=exited exit=3 error= user=1883:1883",
                b"1000:1000 0600 /mosquitto/dynsec/dynamic-security.json\nbroker_uid_access=ok\n",
            ]) as docker, patch.object(
                R, "container_logs", return_value="permission denied on stderr"
            ), contextlib.redirect_stdout(output):
                R.diagnose_broker_start("owned-broker-id", work, "owned-dynsec-volume")

        text = output.getvalue()
        self.assertIn("status=exited exit=3", text)
        self.assertIn("broker_uid_access=ok", text)
        self.assertIn("permission denied on stderr", text)
        self.assertEqual(docker.call_count, 2)
        diagnostic_command = docker.call_args_list[1].args[0]
        self.assertIn("--user", diagnostic_command)
        self.assertIn("1883:1883", diagnostic_command)
        self.assertIn("owned-dynsec-volume", " ".join(diagnostic_command))

    def test_container_logs_merges_stderr_without_echoing_commands(self):
        proc = SimpleNamespace(stdout=b"stdout line\nstderr line\n")
        with patch.object(R.subprocess, "run", return_value=proc) as run:
            self.assertEqual(R.container_logs("owned-broker-id"), "stdout line\nstderr line\n")
        self.assertIs(run.call_args.kwargs["stderr"], R.subprocess.STDOUT)


if __name__ == "__main__":
    unittest.main()
