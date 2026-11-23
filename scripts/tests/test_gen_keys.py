import base64
import contextlib
import hashlib
import hmac
import importlib.util
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock


SCRIPT = Path(__file__).resolve().parents[1] / "gen-keys.py"
spec = importlib.util.spec_from_file_location("gen_keys", SCRIPT)
gen_keys = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gen_keys)


def decode_part(part):
    return json.loads(base64.urlsafe_b64decode(part + "=" * (-len(part) % 4)))


class GenerateKeysTests(unittest.TestCase):
    def test_generates_all_tokens_with_secret_from_dotenv(self):
        with tempfile.TemporaryDirectory(dir="/var/tmp/opencode") as directory:
            root = Path(directory)
            (root / "scripts").mkdir()
            (root / ".env").write_text("JWT_SECRET=synthetic-test-secret\n", encoding="utf-8")
            output = io.StringIO()
            with mock.patch.object(gen_keys, "__file__", str(root / "scripts" / "gen-keys.py")), \
                    mock.patch.object(sys, "argv", ["gen-keys.py"]), \
                    mock.patch.dict(os.environ, {}, clear=True), \
                    mock.patch.object(gen_keys.time, "time", return_value=1_700_000_000), \
                    contextlib.redirect_stdout(output):
                gen_keys.main()

        lines = dict(line.split("=", 1) for line in output.getvalue().splitlines())
        self.assertEqual(set(lines), {"ANON_KEY", "SERVICE_ROLE_KEY", "MEDIA_PLAYBACK_STORAGE_TOKEN"})
        for name, role in (("ANON_KEY", "anon"), ("SERVICE_ROLE_KEY", "service_role"),
                           ("MEDIA_PLAYBACK_STORAGE_TOKEN", "storage_playback_signer")):
            header, payload, signature = lines[name].split(".")
            self.assertEqual(decode_part(header)["alg"], "HS256")
            claims = decode_part(payload)
            self.assertEqual(claims["role"], role)
            self.assertEqual(claims["iss"], "supabase")
            self.assertEqual(claims["iat"], 1_700_000_000)
            self.assertGreater(claims["exp"], claims["iat"])
            expected = hmac.new(
                b"synthetic-test-secret", f"{header}.{payload}".encode(), hashlib.sha256,
            ).digest()
            self.assertTrue(hmac.compare_digest(
                base64.urlsafe_b64decode(signature + "=" * (-len(signature) % 4)), expected,
            ))

    def test_existing_secret_source_precedence_is_preserved(self):
        with mock.patch.object(sys, "argv", ["gen-keys.py", "cli-secret"]), \
                mock.patch.dict(os.environ, {"JWT_SECRET": "environment-secret"}, clear=True):
            self.assertEqual(gen_keys.get_secret_from_env(), "cli-secret")
        with mock.patch.object(sys, "argv", ["gen-keys.py"]), \
                mock.patch.dict(os.environ, {"JWT_SECRET": "environment-secret"}, clear=True):
            self.assertEqual(gen_keys.get_secret_from_env(), "environment-secret")


if __name__ == "__main__":
    unittest.main()
