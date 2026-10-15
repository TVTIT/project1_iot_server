"""Finding3 only: disposable certificates, no deployment env/resources."""
import hashlib
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]


def certificate_fixture_env(root, fake_openssl=False, rootful=False, foreign_owner=''):
    """Normalize ONLY shared test-base metadata; keep fixture guards real.

    Rootful/foreign-owner cases simulate metadata/identity, NOT kernel privilege.
    No shared directory permissions are modified.
    """
    binaries = root / 'certificate-test-bin'
    binaries.mkdir(mode=0o755, exist_ok=True)
    driver = binaries / 'python3'
    driver.write_text('''#!/usr/bin/python3
import os, sys
ambient = os.lstat('/tmp/opencode')
lstat, fstat = os.lstat, os.fstat
def adjusted(st, path):
    fields = list(st)
    if (st.st_dev, st.st_ino) == (ambient.st_dev, ambient.st_ino): fields[0] &= ~0o022
    if os.environ.get('CERT_TEST_FOREIGN_OWNER') == str(path): fields[4] = os.getuid() + 99
    return os.stat_result(fields)
os.lstat = lambda path, *a, **kw: adjusted(lstat(path, *a, **kw), path)
os.fstat = lambda fd: adjusted(fstat(fd), os.readlink('/proc/self/fd/' + str(fd)))
if os.environ.get('CERT_TEST_ROOTFUL') == 'true': os.geteuid = lambda: 0
sys.argv = sys.argv[1:]
code = sys.stdin.read()
trace_dir = os.environ.get('CERT_TEST_TRACE_DIR')
if trace_dir:
    import pathlib, trace
    directory = pathlib.Path(trace_dir)
    directory.mkdir(parents=True, exist_ok=True)
    source = directory / 'certificate_preflight.py'
    source.write_text(code)  # Public source only, never env/certificate contents.
    runner = trace.Trace(count=True, trace=False)
    try:
        runner.runctx(compile(code, str(source), 'exec'), globals(), globals())
    finally:
        counts = str(directory / 'counts.pickle')
        selected = {key: value for key, value in runner.counts.items() if key[0] == str(source)}
        trace.CoverageResults(selected, infile=counts, outfile=counts).write_results(show_missing=True, summary=False, coverdir=str(directory / 'lines'))
else:
    exec(compile(code, '<certificate-fixture-driver>', 'exec'))
''')
    driver.chmod(0o755)
    if rootful:
        identity = binaries / 'id'
        identity.write_text('#!/bin/sh\ncase "$1" in -u|-g) echo 0;; *) exec /usr/bin/id "$@";; esac\n')
        identity.chmod(0o755)
    if fake_openssl:
        openssl = binaries / 'openssl'
        openssl.write_text('''#!/bin/bash
printf 'called\n' >> "$CERT_TEST_OPENSSL_MARKER"
previous=''
for arg in "$@"; do
    case "$previous" in -keyout|-out) printf 'synthetic-generated' > "$arg";; esac
    previous="$arg"
done
if [ "$1" = rand ]; then echo 00112233445566778899aabbccddeeff; fi
if [ "$1" = x509 ] && [ -n "${CERT_TEST_SWAP_CONFIG:-}" ]; then
    mv "$CERT_TEST_SWAP_CONFIG" "$CERT_TEST_RELOCATED_CONFIG"
    ln -s "$CERT_TEST_OUTSIDE_CONFIG" "$CERT_TEST_SWAP_CONFIG"
fi
''')
        openssl.chmod(0o755)
    env = {'PATH': str(binaries) + ':' + os.environ['PATH'],
            'MQTT_CERT_UID': str(os.getuid()), 'MQTT_CERT_GID': str(os.getgid()),
            'CERT_TEST_ROOTFUL': str(rootful).lower(), 'SUDO_UID': str(os.getuid()),
            'CERT_TEST_FOREIGN_OWNER': str(foreign_owner),
            'CERT_TEST_OPENSSL_MARKER': str(root / 'openssl-called')}
    if os.environ.get('TASK283_CERT_TRACE_DIR'):
        env['CERT_TEST_TRACE_DIR'] = os.environ['TASK283_CERT_TRACE_DIR']
    if os.environ.get('TASK283_CERT_BASH_LINES'):
        hook = binaries / 'coverage-hook.sh'
        hook.write_text('''set -T
trap 'if [[ "${BASH_SOURCE[0]:-}" = */scripts/gen-certs.sh ]]; then printf "%s\\n" "$LINENO" >> "$CERT_TEST_BASH_LINES"; fi' DEBUG
''')
        hook.chmod(0o600)
        env['BASH_ENV'] = str(hook)
        env['CERT_TEST_BASH_LINES'] = os.environ['TASK283_CERT_BASH_LINES']
    return env


class CertificateDestinationTests(unittest.TestCase):
    def prepare(self, root, seeded=True):
        for name, mode in [('scripts', 0o755), ('config', 0o755),
                           ('config/mosquitto', 0o755), ('config/mosquitto/certs', 0o755), ('certs', 0o700)]:
            (root / name).mkdir(mode=mode, parents=True, exist_ok=True)
            (root / name).chmod(mode)
        script = root / 'scripts/gen-certs.sh'
        script.write_text((ROOT / 'scripts/gen-certs.sh').read_text())
        script.chmod(0o644)
        if seeded:
            for name, mode in [('config/mosquitto/certs/server.key', 0o600),
                               ('config/mosquitto/certs/server.crt', 0o644),
                               ('config/mosquitto/certs/ca.crt', 0o644),
                               ('certs/ca.key', 0o600), ('certs/ca.crt', 0o644)]:
                (root / name).write_text('existing fixture ' + name)
                (root / name).chmod(mode)
        return script

    def snapshot(self, path):
        data = {}
        for p in [path, *path.rglob('*')]:
            st = p.lstat()
            body = hashlib.sha256(p.read_bytes()).hexdigest() if p.is_file() and not p.is_symlink() else None
            data[str(p.relative_to(path))] = (st.st_mode, st.st_uid, st.st_gid, st.st_ino, body)
        return data

    def run_script(self, script, env, *args):
        return subprocess.run(['bash', str(script), *args], env=env, capture_output=True, timeout=30)

    def test_ancestor_symlinks_reject_before_any_mutation_nonroot_and_rootful_model(self):
        for level in ['config', 'config/mosquitto', 'config/mosquitto/certs', 'certs']:
            for rootful in [False, True]:
                with self.subTest(level=level, rootful=rootful), tempfile.TemporaryDirectory(dir='/tmp/opencode') as d:
                    base = Path(d)
                    root = base / 'checkout'
                    root.mkdir(mode=0o700)
                    script = self.prepare(root)
                    outside = base / 'outside'
                    (root / level).rename(outside)
                    (root / level).symlink_to(outside, target_is_directory=True)
                    before = self.snapshot(outside)
                    env = certificate_fixture_env(root, fake_openssl=True, rootful=rootful)
                    result = self.run_script(script, env)
                    self.assertNotEqual(result.returncode, 0)
                    self.assertFalse((root / 'openssl-called').exists())
                    self.assertEqual(self.snapshot(outside), before)

    def test_symlinked_checkout_and_script_parent_are_not_silently_resolved(self):
        for level in ['checkout', 'scripts']:
            with self.subTest(level=level), tempfile.TemporaryDirectory(dir='/tmp/opencode') as d:
                base = Path(d)
                root = base / 'real'
                root.mkdir(mode=0o700)
                script = self.prepare(root)
                env = certificate_fixture_env(root, fake_openssl=True)
                if level == 'checkout':
                    alias = base / 'alias'
                    alias.symlink_to(root, target_is_directory=True)
                    script = alias / 'scripts/gen-certs.sh'
                else:
                    (root / 'scripts').rename(root / 'other-scripts')
                    (root / 'scripts').symlink_to(root / 'other-scripts', target_is_directory=True)
                before = self.snapshot(root / 'config/mosquitto/certs')
                result = self.run_script(script, env)
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse((root / 'openssl-called').exists())
                self.assertEqual(self.snapshot(root / 'config/mosquitto/certs'), before)

    def test_unsafe_modes_preserve_old_contents_modes_and_ownership(self):
        for relative, mode in [('config', 0o777), ('config', 0o1777), ('config', 0o775), ('config/mosquitto', 0o777),
                               ('config/mosquitto/certs', 0o775), ('certs', 0o755)]:
            with self.subTest(relative=relative, mode=oct(mode)), tempfile.TemporaryDirectory(dir='/tmp/opencode') as d:
                root = Path(d)
                script = self.prepare(root)
                (root / relative).chmod(mode)
                before_ca = self.snapshot(root / 'certs')
                before_server = self.snapshot(root / 'config/mosquitto/certs')
                result = self.run_script(script, certificate_fixture_env(root, fake_openssl=True))
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse((root / 'openssl-called').exists())
                self.assertEqual(self.snapshot(root / 'certs'), before_ca)
                self.assertEqual(self.snapshot(root / 'config/mosquitto/certs'), before_server)
                self.assertEqual((root / relative).stat().st_mode & 0o7777, mode)

    def test_foreign_parent_owner_rejected_before_mutation_rootful_model(self):
        with tempfile.TemporaryDirectory(dir='/tmp/opencode') as d:
            root = Path(d)
            script = self.prepare(root)
            before = self.snapshot(root / 'config/mosquitto/certs')
            env = certificate_fixture_env(root, fake_openssl=True, rootful=True, foreign_owner=root / 'config')
            result = self.run_script(script, env)
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse((root / 'openssl-called').exists())
            self.assertEqual(self.snapshot(root / 'config/mosquitto/certs'), before)

    def test_protected_env_cannot_redirect_fixed_destinations_or_execute_shell(self):
        for key in ['PROJECT_ROOT', 'MOSQUITTO_CERTS_DIR', 'SECURE_CA_DIR', 'shell']:
            with self.subTest(key=key), tempfile.TemporaryDirectory(dir='/tmp/opencode') as d:
                root = Path(d)
                script = self.prepare(root)
                outside = root / 'outside'
                outside.mkdir(mode=0o700)
                (outside / 'sentinel').write_text('unrelated fixture')
                before = self.snapshot(outside)
                env_file = root / '.env'
                env_file.write_text(('touch ' + str(root / 'executed')) if key == 'shell' else key + '=' + str(outside))
                env_file.chmod(0o600)
                result = self.run_script(script, certificate_fixture_env(root, fake_openssl=True))
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse((root / 'executed').exists())
                self.assertFalse((root / 'openssl-called').exists())
                self.assertEqual(self.snapshot(outside), before)

    def test_nonroot_cannot_assign_another_uid_without_insecure_fallback(self):
        with tempfile.TemporaryDirectory(dir='/tmp/opencode') as d:
            root = Path(d)
            script = self.prepare(root)
            before = self.snapshot(root / 'config/mosquitto/certs')
            env = certificate_fixture_env(root, fake_openssl=True)
            env['MQTT_CERT_UID'] = str(os.getuid() + 1)
            result = self.run_script(script, env)
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(self.snapshot(root / 'config/mosquitto/certs'), before)

    def test_unsafe_leaves_and_unprotected_env_are_rejected_without_repairs(self):
        for kind in ['symlink', 'hardlink', 'fifo', 'key0644', 'env0644']:
            with self.subTest(kind=kind), tempfile.TemporaryDirectory(dir='/tmp/opencode') as d:
                root = Path(d)
                script = self.prepare(root)
                key = root / 'config/mosquitto/certs/server.key'
                if kind in ['symlink', 'hardlink']:
                    key.unlink()
                    victim = root / 'victim'
                    victim.write_text('outside fixture')
                    victim.chmod(0o600)
                    if kind == 'symlink':
                        key.symlink_to(victim)
                    else:
                        os.link(victim, key)
                elif kind == 'fifo':
                    key.unlink()
                    os.mkfifo(key, 0o600)
                elif kind == 'key0644':
                    key.chmod(0o644)
                else:
                    (root / '.env').write_text('MQTT_BROKER_DOMAIN=fixture.invalid')
                    (root / '.env').chmod(0o644)
                before = self.snapshot(root / 'config/mosquitto/certs')
                result = self.run_script(script, certificate_fixture_env(root, fake_openssl=True))
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse((root / 'openssl-called').exists())
                self.assertEqual(self.snapshot(root / 'config/mosquitto/certs'), before)

    def test_rootful_assignment_failure_keeps_old_certificates_and_ca_untouched(self):
        with tempfile.TemporaryDirectory(dir='/tmp/opencode') as d:
            root = Path(d)
            script = self.prepare(root)
            env = certificate_fixture_env(root, fake_openssl=True, rootful=True)
            env['MQTT_CERT_UID'] = str(os.getuid() + 1)
            deny = root / 'certificate-test-bin/chown'
            deny.write_text('#!/bin/sh\nexit 46\n')
            deny.chmod(0o755)
            ca, server = self.snapshot(root / 'certs'), self.snapshot(root / 'config/mosquitto/certs')
            result = self.run_script(script, env)
            self.assertNotEqual(result.returncode, 0)
            self.assertTrue((root / 'openssl-called').exists())
            self.assertEqual(self.snapshot(root / 'certs'), ca)
            self.assertEqual(self.snapshot(root / 'config/mosquitto/certs'), server)

    def test_second_publication_failure_restores_all_preexisting_inodes(self):
        with tempfile.TemporaryDirectory(dir='/tmp/opencode') as d:
            root = Path(d)
            script = self.prepare(root)
            env = certificate_fixture_env(root, fake_openssl=True)
            fail = root / 'certificate-test-bin/mv'
            fail.write_text('''#!/bin/bash
if [[ "$1" = -f && "$2" = */.generation.*/server.crt ]]; then exit 47; fi
exec /usr/bin/mv "$@"
''')
            fail.chmod(0o755)
            ca, server = self.snapshot(root / 'certs'), self.snapshot(root / 'config/mosquitto/certs')
            result = self.run_script(script, env)
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(self.snapshot(root / 'certs'), ca)
            self.assertEqual(self.snapshot(root / 'config/mosquitto/certs'), server)

    def test_mid_generation_ancestor_swap_is_detected_without_following_replacement(self):
        with tempfile.TemporaryDirectory(dir='/tmp/opencode') as d:
            base = Path(d)
            root, outside = base / 'checkout', base / 'outside'
            root.mkdir(mode=0o700)
            outside.mkdir(mode=0o700)
            script = self.prepare(root)
            self.prepare(outside)
            before = self.snapshot(outside / 'config')
            original = self.snapshot(root / 'config')
            env = certificate_fixture_env(root, fake_openssl=True)
            env.update({'CERT_TEST_SWAP_CONFIG': str(root / 'config'),
                        'CERT_TEST_RELOCATED_CONFIG': str(root / 'relocated-config'),
                        'CERT_TEST_OUTSIDE_CONFIG': str(outside / 'config')})
            result = self.run_script(script, env)
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(self.snapshot(outside / 'config'), before)
            self.assertEqual(self.snapshot(root / 'relocated-config'), original)

    def test_protected_fixture_env_is_parsed_as_data_and_rootful_metadata_model_succeeds(self):
        with tempfile.TemporaryDirectory(dir='/tmp/opencode') as d:
            root = Path(d)
            script = self.prepare(root, seeded=False)
            config = root / '.env'
            config.write_text('MQTT_BROKER_DOMAIN="fixture.invalid"\nMQTT_ADDITIONAL_SAN=other.invalid\nJWT_SECRET=fixture-ignored\n')
            config.chmod(0o600)
            result = self.run_script(script, certificate_fixture_env(root, rootful=True))
            self.assertEqual(result.returncode, 0)
            self.assertNotIn(b'fixture-ignored', result.stdout + result.stderr)
            self.assertEqual((root / 'config/mosquitto/certs/server.key').stat().st_mode & 0o777, 0o600)

    def test_trusted_success_reuses_ca_and_never_chmods_or_chowns_existing_directories(self):
        with tempfile.TemporaryDirectory(dir='/tmp/opencode') as d:
            root = Path(d)
            script = self.prepare(root, seeded=False)
            env = certificate_fixture_env(root)
            parents = {n: (root / n).stat() for n in ['config', 'config/mosquitto', 'config/mosquitto/certs', 'certs']}
            self.assertEqual(self.run_script(script, env).returncode, 0)
            before_key = (root / 'certs/ca.key').read_bytes()
            before_key_stat = (root / 'certs/ca.key').stat()
            self.assertEqual(self.run_script(script, env).returncode, 0)
            self.assertEqual((root / 'certs/ca.key').read_bytes(), before_key)
            after_key_stat = (root / 'certs/ca.key').stat()
            self.assertEqual((before_key_stat.st_ino, before_key_stat.st_uid, before_key_stat.st_gid),
                             (after_key_stat.st_ino, after_key_stat.st_uid, after_key_stat.st_gid))
            for n, old in parents.items():
                new = (root / n).stat()
                self.assertEqual((old.st_mode, old.st_uid, old.st_gid, old.st_ino), (new.st_mode, new.st_uid, new.st_gid, new.st_ino))
            self.assertEqual({p.name for p in (root / 'config/mosquitto/certs').iterdir()}, {'server.key', 'server.crt', 'ca.crt'})
            self.assertEqual((root / 'config/mosquitto/certs/server.key').stat().st_mode & 0o777, 0o600)
            self.assertEqual((root / 'certs/ca.key').stat().st_mode & 0o777, 0o600)


if __name__ == '__main__':
    unittest.main()
