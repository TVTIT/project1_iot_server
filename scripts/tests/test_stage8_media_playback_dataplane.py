#!/usr/bin/env python3
"""Minimal targeted syntax and proxy configuration tests for Stage 8.7-A playback listener."""

import os
import subprocess
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


class TestStage8PlaybackDataPlaneSyntax(unittest.TestCase):
    """Targeted local validation of Nginx and Envoy configs using pinned repo images."""

    @classmethod
    def setUpClass(cls):
        try:
            subprocess.run(["docker", "info"], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception:
            raise unittest.SkipTest("Docker daemon not available")

    def test_isolated_nginx_base_syntax(self):
        """Verify Nginx passes syntax check with base template and playback listener."""
        cmd = [
            "docker", "run", "--rm",
            "--add-host", "backend:127.0.0.1",
            "-e", "API_DOMAIN=localhost",
            "-e", "MEDIA_UPLOAD_ENABLED=false",
            "-e", "MEDIA_UPLOAD_ROUTE_KEY=",
            "-e", "MEDIA_PLAYBACK_ENABLED=true",
            "-e", "MEDIA_PLAYBACK_ROUTE_KEY=" + ("a" * 64),
            "-v", f"{ROOT}/config/nginx/nginx.conf.template:/etc/nginx/templates/default.conf.template:ro",
            "-v", f"{ROOT}/config/nginx/stage8-media-playback.conf.template:/etc/nginx/templates/stage8-media-playback.conf.template:ro",
            "-v", f"{ROOT}/config/nginx/18-validate-playback-key.envsh:/docker-entrypoint.d/18-validate-playback-key.envsh:ro",
            "nginx:1.25-alpine",
            "/bin/sh", "-ec", "/docker-entrypoint.d/20-envsubst-on-templates.sh && nginx -t"
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
        self.assertEqual(proc.returncode, 0, f"Nginx base syntax check failed: {proc.stderr.strip()}")
        self.assertIn("syntax is ok", proc.stderr + proc.stdout)

    def test_isolated_nginx_p2_syntax(self):
        """Verify Nginx passes syntax check with Paper P2 template and playback listener."""
        tls_dir = Path("/var/tmp/opencode/p2_syntax_tls")
        tls_dir.mkdir(parents=True, exist_ok=True)
        key_file = tls_dir / "server.key"
        crt_file = tls_dir / "server.crt"
        if not key_file.exists() or not crt_file.exists():
            subprocess.run([
                "openssl", "req", "-x509", "-nodes", "-days", "1", "-newkey", "rsa:2048",
                "-keyout", str(key_file), "-out", str(crt_file),
                "-subj", "/CN=localhost"
            ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        cmd = [
            "docker", "run", "--rm",
            "--add-host", "backend:127.0.0.1",
            "--add-host", "kong:127.0.0.1",
            "-e", "API_DOMAIN=localhost",
            "-e", "MEDIA_PLAYBACK_ENABLED=true",
            "-e", "MEDIA_PLAYBACK_ROUTE_KEY=" + ("a" * 64),
            "-v", f"{ROOT}/config/nginx/paper-p2.conf.template:/etc/nginx/templates/default.conf.template:ro",
            "-v", f"{ROOT}/config/nginx/stage8-media-playback.conf.template:/etc/nginx/templates/stage8-media-playback.conf.template:ro",
            "-v", f"{ROOT}/config/nginx/18-validate-playback-key.envsh:/docker-entrypoint.d/18-validate-playback-key.envsh:ro",
            "-v", f"{tls_dir}:/etc/nginx/p2-tls:ro",
            "nginx:1.25-alpine",
            "/bin/sh", "-ec", "/docker-entrypoint.d/20-envsubst-on-templates.sh && nginx -t"
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
        self.assertEqual(proc.returncode, 0, f"Nginx P2 syntax check failed: {proc.stderr.strip()}")
        self.assertIn("syntax is ok", proc.stderr + proc.stdout)

    def test_nginx_envsh_hook_in_container(self):
        """Verify 18-validate-playback-key.envsh runs cleanly inside the nginx alpine container."""
        # Test valid key preserved
        cmd_valid = [
            "docker", "run", "--rm",
            "-e", "MEDIA_PLAYBACK_ROUTE_KEY=" + ("f" * 64),
            "-e", "MEDIA_PLAYBACK_ENABLED=true",
            "-v", f"{ROOT}/config/nginx/18-validate-playback-key.envsh:/docker-entrypoint.d/18-validate-playback-key.envsh:ro",
            "nginx:1.25-alpine",
            "/bin/sh", "-ec", '. /docker-entrypoint.d/18-validate-playback-key.envsh; echo "KEY=$MEDIA_PLAYBACK_ROUTE_KEY;ENABLED=$MEDIA_PLAYBACK_ENABLED"'
        ]
        proc = subprocess.run(cmd_valid, capture_output=True, text=True, timeout=10)
        self.assertEqual(proc.returncode, 0)
        self.assertEqual(proc.stdout.strip(), f"KEY={'f'*64};ENABLED=true")

        # Test corrupt key sanitized to empty string and disabled
        cmd_corrupt = [
            "docker", "run", "--rm",
            "-e", 'MEDIA_PLAYBACK_ROUTE_KEY=bad"quote;rm -rf /|pipe',
            "-e", "MEDIA_PLAYBACK_ENABLED=true",
            "-v", f"{ROOT}/config/nginx/18-validate-playback-key.envsh:/docker-entrypoint.d/18-validate-playback-key.envsh:ro",
            "nginx:1.25-alpine",
            "/bin/sh", "-ec", '. /docker-entrypoint.d/18-validate-playback-key.envsh; echo "KEY=$MEDIA_PLAYBACK_ROUTE_KEY;ENABLED=$MEDIA_PLAYBACK_ENABLED"'
        ]
        proc = subprocess.run(cmd_corrupt, capture_output=True, text=True, timeout=10)
        self.assertEqual(proc.returncode, 0)
        self.assertEqual(proc.stdout.strip(), "KEY=;ENABLED=false")

        # Test noncanonical flags normalized to false and key cleared
        for noncanonical in ["TRUE", "1", " true", "yes", "false", ""]:
            cmd = [
                "docker", "run", "--rm",
                "-e", "MEDIA_PLAYBACK_ROUTE_KEY=" + ("f" * 64),
                "-e", f"MEDIA_PLAYBACK_ENABLED={noncanonical}",
                "-v", f"{ROOT}/config/nginx/18-validate-playback-key.envsh:/docker-entrypoint.d/18-validate-playback-key.envsh:ro",
                "nginx:1.25-alpine",
                "/bin/sh", "-ec", '. /docker-entrypoint.d/18-validate-playback-key.envsh; echo "KEY=$MEDIA_PLAYBACK_ROUTE_KEY;ENABLED=$MEDIA_PLAYBACK_ENABLED"'
            ]
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            self.assertEqual(proc.returncode, 0)
            self.assertEqual(proc.stdout.strip(), "KEY=;ENABLED=false", f"Failed for {noncanonical}")

    def test_envoy_entrypoint_inert_when_playback_disabled(self):
        """When MEDIA_PLAYBACK_ENABLED is false/missing/noncanonical, Envoy route key is empty and route is inert."""
        for enabled_val in ["false", "", "TRUE", "1"]:
            cmd = [
                "docker", "run", "--rm",
                "-e", "ANON_KEY=test_anon_key",
                "-e", "SERVICE_ROLE_KEY=test_service_role_key",
                "-e", "MEDIA_UPLOAD_ROUTE_KEY=" + ("b" * 64),
                "-e", "MEDIA_PLAYBACK_ROUTE_KEY=" + ("c" * 64),
                "-e", f"MEDIA_PLAYBACK_ENABLED={enabled_val}",
                "-v", f"{ROOT}/config/envoy/envoy.yaml:/etc/envoy/envoy.yaml:ro",
                "-v", f"{ROOT}/config/envoy/cds.yaml:/etc/envoy/cds.yaml:ro",
                "-v", f"{ROOT}/config/envoy/lds.template.yaml:/etc/envoy/lds.template.yaml:ro",
                "-v", f"{ROOT}/config/envoy/docker-entrypoint.sh:/docker-entrypoint.sh:ro",
                "--entrypoint", "/bin/sh",
                "envoyproxy/envoy:v1.39.1",
                "-ec", "sh /docker-entrypoint.sh --mode validate && cat /etc/envoy/lds.yaml"
            ]
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
            self.assertEqual(proc.returncode, 0)
            self.assertIn("exact: ''", proc.stdout, f"Envoy lds.yaml did not have exact: '' for {enabled_val}")
            self.assertNotIn("c" * 64, proc.stdout, f"Envoy lds.yaml exposed key for {enabled_val}")

    def test_isolated_envoy_syntax_valid_key(self):
        """Verify Envoy passes configuration validation after template substitution with valid key."""
        cmd = [
            "docker", "run", "--rm",
            "-e", "ANON_KEY=test_anon_key",
            "-e", "SERVICE_ROLE_KEY=test_service_role_key",
            "-e", "MEDIA_UPLOAD_ROUTE_KEY=" + ("b" * 64),
            "-e", "MEDIA_PLAYBACK_ROUTE_KEY=" + ("c" * 64),
            "-e", "SUPABASE_PUBLIC_URL=http://localhost",
            "-e", "DASHBOARD_USERNAME=admin",
            "-e", "DASHBOARD_PASSWORD=admin",
            "-v", f"{ROOT}/config/envoy/envoy.yaml:/etc/envoy/envoy.yaml:ro",
            "-v", f"{ROOT}/config/envoy/cds.yaml:/etc/envoy/cds.yaml:ro",
            "-v", f"{ROOT}/config/envoy/lds.template.yaml:/etc/envoy/lds.template.yaml:ro",
            "-v", f"{ROOT}/config/envoy/docker-entrypoint.sh:/docker-entrypoint.sh:ro",
            "--entrypoint", "/bin/sh",
            "envoyproxy/envoy:v1.39.1",
            "-ec", "sh /docker-entrypoint.sh --mode validate"
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
        self.assertEqual(proc.returncode, 0, f"Envoy validation failed: {proc.stderr.strip()}")
        self.assertIn("configuration '/etc/envoy/envoy.yaml' OK", proc.stderr + proc.stdout)

    def test_live_envoy_lds_listener_accepted(self):
        """Live probe to ensure Envoy dynamic filesystem LDS accepts listener 'supabase' without RE2 size overflow."""
        cmd = [
            "docker", "run", "-d", "--rm",
            "-e", "ANON_KEY=test_anon_key",
            "-e", "SERVICE_ROLE_KEY=test_service_role_key",
            "-e", "MEDIA_UPLOAD_ROUTE_KEY=" + ("b" * 64),
            "-e", "MEDIA_PLAYBACK_ROUTE_KEY=" + ("c" * 64),
            "-v", f"{ROOT}/config/envoy/envoy.yaml:/etc/envoy/envoy.yaml:ro",
            "-v", f"{ROOT}/config/envoy/cds.yaml:/etc/envoy/cds.yaml:ro",
            "-v", f"{ROOT}/config/envoy/lds.template.yaml:/etc/envoy/lds.template.yaml:ro",
            "-v", f"{ROOT}/config/envoy/docker-entrypoint.sh:/docker-entrypoint.sh:ro",
            "--entrypoint", "/bin/sh",
            "envoyproxy/envoy:v1.39.1",
            "-ec", "sh /docker-entrypoint.sh"
        ]
        cid = subprocess.run(cmd, capture_output=True, text=True, check=True).stdout.strip()
        try:
            listener_accepted = False
            for _ in range(30):
                time_logs = subprocess.run(["docker", "logs", cid], capture_output=True, text=True).stderr
                if "RE2 program size" in time_logs:
                    self.fail(f"Envoy rejected regex with RE2 program size overflow: {time_logs}")
                if "lds: add/update listener 'supabase'" in time_logs and "starting workers" in time_logs:
                    listener_accepted = True
                    break
                import time
                time.sleep(0.2)
            self.assertTrue(listener_accepted, "Envoy failed to dynamically accept and start listener 'supabase'")
        finally:
            subprocess.run(["docker", "rm", "-f", cid], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    def test_isolated_envoy_syntax_corrupt_key_disabled_cleanly(self):
        """Verify Envoy entrypoint sanitizes invalid key syntax and doesn't corrupt sed/YAML."""
        cmd = [
            "docker", "run", "--rm",
            "-e", "ANON_KEY=test_anon_key",
            "-e", "SERVICE_ROLE_KEY=test_service_role_key",
            "-e", "MEDIA_UPLOAD_ROUTE_KEY=" + ("b" * 64),
            "-e", "MEDIA_PLAYBACK_ROUTE_KEY=bad'\"|\\n$(rm -rf /)key",
            "-e", "SUPABASE_PUBLIC_URL=http://localhost",
            "-e", "DASHBOARD_USERNAME=admin",
            "-e", "DASHBOARD_PASSWORD=admin",
            "-v", f"{ROOT}/config/envoy/envoy.yaml:/etc/envoy/envoy.yaml:ro",
            "-v", f"{ROOT}/config/envoy/cds.yaml:/etc/envoy/cds.yaml:ro",
            "-v", f"{ROOT}/config/envoy/lds.template.yaml:/etc/envoy/lds.template.yaml:ro",
            "-v", f"{ROOT}/config/envoy/docker-entrypoint.sh:/docker-entrypoint.sh:ro",
            "--entrypoint", "/bin/sh",
            "envoyproxy/envoy:v1.39.1",
            "-ec", "sh /docker-entrypoint.sh --mode validate"
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
        self.assertEqual(proc.returncode, 0, f"Envoy validation with corrupt key failed: {proc.stderr.strip()}")
        self.assertIn("configuration '/etc/envoy/envoy.yaml' OK", proc.stderr + proc.stdout)


    def test_live_envoy_playback_off_blocks_get_preserves_upload_put(self):
        """Verify Envoy live dispatch: playback-off blocks signed GET even with valid key, signed PUT works."""
        # 1. Run Envoy with MEDIA_PLAYBACK_ENABLED=false
        upload_key = "b" * 64
        playback_key = "c" * 64
        cmd = [
            "docker", "run", "-d", "--rm",
            "-p", "127.0.0.1::8000",
            "-e", "ANON_KEY=test_anon_key",
            "-e", "SERVICE_ROLE_KEY=test_service_role_key",
            "-e", f"MEDIA_UPLOAD_ROUTE_KEY={upload_key}",
            "-e", f"MEDIA_PLAYBACK_ROUTE_KEY={playback_key}",
            "-e", "MEDIA_PLAYBACK_ENABLED=false",
            "-v", f"{ROOT}/config/envoy/envoy.yaml:/etc/envoy/envoy.yaml:ro",
            "-v", f"{ROOT}/config/envoy/cds.yaml:/etc/envoy/cds.yaml:ro",
            "-v", f"{ROOT}/config/envoy/lds.template.yaml:/etc/envoy/lds.template.yaml:ro",
            "-v", f"{ROOT}/config/envoy/docker-entrypoint.sh:/docker-entrypoint.sh:ro",
            "--entrypoint", "/bin/sh",
            "envoyproxy/envoy:v1.39.1",
            "-ec", "sh /docker-entrypoint.sh"
        ]
        cid = subprocess.run(cmd, capture_output=True, text=True, check=True).stdout.strip()
        try:
            # Wait for Envoy to start workers
            import time
            started = False
            for _ in range(30):
                logs = subprocess.run(["docker", "logs", cid], capture_output=True, text=True).stderr
                if "starting workers" in logs:
                    started = True
                    break
                time.sleep(0.2)
            self.assertTrue(started, "Envoy container failed to start")

            port_out = subprocess.run(["docker", "port", cid, "8000/tcp"], capture_output=True, text=True, check=True).stdout.strip()
            envoy_port = port_out.split(":")[-1]

            import urllib.request
            import urllib.error

            # Signed GET for playback when playback is disabled must return 404 direct response
            req_get = urllib.request.Request(
                f"http://127.0.0.1:{envoy_port}/storage/v1/object/sign/media-video/00000000-0000-0000-0000-000000000001.mp4",
                headers={"x-stage8-media-playback-route": playback_key},
                method="GET",
            )
            with self.assertRaises(urllib.error.HTTPError) as ctx:
                urllib.request.urlopen(req_get, timeout=5)
            self.assertEqual(ctx.exception.code, 404, "Envoy should return 404 direct response for playback GET when disabled")

            # Signed PUT (upload) with upload route key must match the upload route (attempts cluster upstream -> 503 since no storage backend)
            req_put = urllib.request.Request(
                f"http://127.0.0.1:{envoy_port}/storage/v1/object/upload/sign/media-video/00000000-0000-0000-0000-000000000001.mp4",
                data=b"test-bytes",
                headers={"x-stage8-media-route": upload_key},
                method="PUT",
            )
            with self.assertRaises(urllib.error.HTTPError) as ctx:
                urllib.request.urlopen(req_put, timeout=5)
            self.assertEqual(ctx.exception.code, 503, "Envoy should route upload to cluster (returning 503 upstream connect fail, NOT 404 direct response)")
        finally:
            subprocess.run(["docker", "rm", "-f", cid], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


if __name__ == "__main__":
    unittest.main()
