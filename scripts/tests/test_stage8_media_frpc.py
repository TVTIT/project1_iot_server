#!/usr/bin/env python3
"""Compact deterministic contract tests for Stage 8.8B frpc configuration."""

from pathlib import Path
import re
import tomllib
import unittest
import yaml

ROOT = Path(__file__).resolve().parents[2]
EXAMPLE_PATH = ROOT / "frpc_config" / "frpc.toml.example"
COMPOSE_PATH = ROOT / "docker-compose.yml"

MQTTS_TUPLE = {
    "name": "mqtts",
    "type": "tcp",
    "localIP": "mosquitto",
    "localPort": 8883,
    "remotePort": 8883,
}


class Stage8FrpcContractTest(unittest.TestCase):
    """Deterministic contract tests for frpc.toml.example and Compose wiring."""

    def test_inactive_config_contract(self):
        """Verify baseline frpc example contract in inactive (default) state."""
        raw = EXAMPLE_PATH.read_text(encoding="utf-8")
        parsed = tomllib.loads(raw)

        self.assertEqual(parsed.get("serverAddr"), "frp.example.com")
        self.assertEqual(parsed.get("serverPort"), 7000)
        self.assertEqual(
            parsed.get("auth"),
            {"method": "token", "tokenSource": {"type": "file", "file": {"path": "/etc/frp/token"}}},
        )
        self.assertEqual(
            parsed.get("transport", {}).get("tls"),
            {"enable": True, "trustedCaFile": "/etc/frp/tls/ca.crt", "serverName": "frp.example.com"},
        )
        self.assertEqual(parsed.get("proxies"), [MQTTS_TUPLE])
        self.assertRegex(
            raw,
            r"# \[\[proxies\]\]\n# name = \"media-playback\"\n# type = \"tcp\"\n# localIP = \"nginx\"\n# localPort = 8081\n# remotePort = 8081",
        )

    def test_explicitly_activated_media_playback(self):
        """Uncommenting media-playback block enables TCP proxy to nginx:8081 with unchanged MQTTS."""
        raw = EXAMPLE_PATH.read_text(encoding="utf-8")
        activated = re.sub(
            r"# (\[\[proxies\]\]\n(?:# [^\n]+\n)+# remotePort = 8081)",
            lambda m: m.group(1).replace("# ", ""),
            raw,
        )
        parsed = tomllib.loads(activated)
        expected_media = {
            "name": "media-playback",
            "type": "tcp",
            "localIP": "nginx",
            "localPort": 8081,
            "remotePort": 8081,
        }
        self.assertEqual(parsed.get("proxies"), [MQTTS_TUPLE, expected_media])
        self.assertNotEqual(expected_media["remotePort"], MQTTS_TUPLE["remotePort"])

    def test_compose_shared_network_and_no_host_publish(self):
        """Verify frpc shares iot_net with nginx without exposing 8081 on host."""
        with open(COMPOSE_PATH, "r", encoding="utf-8") as f:
            services = yaml.safe_load(f).get("services", {})

        frpc = services.get("frpc", {})
        self.assertEqual(frpc.get("image"), "ghcr.io/fatedier/frpc:v0.71.0")
        self.assertIn("iot_net", frpc.get("networks", []))
        self.assertNotIn("ports", frpc)

        nginx = services.get("nginx", {})
        self.assertIn("iot_net", nginx.get("networks", []))
        self.assertEqual(nginx.get("ports"), ["80:80"])

        cf = services.get("cloudflared", {})
        self.assertNotIn("8081", str(cf.get("ports", [])))


if __name__ == "__main__":
    unittest.main()
