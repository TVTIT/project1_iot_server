#!/usr/bin/env python3
"""Safe PostgreSQL-only native fixture runner for Task 2.8 simplified schema 019 transition.
Adheres strictly to actual fixture ownership and failure-path safety:
- Creates an isolated docker container on the host Docker daemon with --cidfile receipt
- Uses pinned timescale image without published ports
- Destructive cleanup targets only the confirmed container ID from valid cidfile receipt
- No name guessing; foreign containers on name collision are preserved
- Atomically created private env file (mode 0600) in private directory (mode 0700)
- Secrets passed via env-file/stdin, never in argv; stdout/stderr/timeout outputs sanitized
- Strict bounded timeouts on all subprocess calls and probe loops
- Distinguishes authoritative container/directory absence from daemon/permission errors
- Preserves primary errors during cleanup; reports cleanup uncertainty separately
"""
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import stat
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parent.parent.parent

# Pinned image and timeouts
PG_IMAGE = 'timescale/timescaledb@sha256:289d55704b1b3ee8263cd3805c6930f9cd54506835a8f19f9b85dad17d5c5a8a'
IMAGE_INSPECT_TIMEOUT = 10
IMAGE_PULL_TIMEOUT = 300
CONTAINER_CREATE_TIMEOUT = 60
CONTAINER_START_TIMEOUT = 60
READINESS_TOTAL_TIMEOUT = 90
PROBE_TIMEOUT = 2
SQL_TIMEOUT = 60
CLEANUP_TIMEOUT = 30

ENV = {k: v for k, v in os.environ.items() if k in (
    'PATH', 'HOME', 'USER', 'DOCKER_HOST', 'DOCKER_TLS_VERIFY', 'DOCKER_CERT_PATH')}


def sanitize_output(text: str, secrets_list: list) -> str:
    """Replace all sensitive secrets with [REDACTED]."""
    if not text:
        return text
    sanitized = text
    for secret in secrets_list:
        if secret:
            sanitized = sanitized.replace(secret, "[REDACTED]")
    return sanitized


def create_private_env_file(env_file_path: Path, parent_dir: Path, env_vars: dict):
    """Atomically create private parent directory (0700) and env file (0600)."""
    parent_dir.mkdir(parents=True, exist_ok=True)
    os.chmod(parent_dir, 0o700)
    lines = "".join(f"{k}={v}\n" for k, v in env_vars.items())
    fd = os.open(str(env_file_path), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with open(fd, 'w', encoding='utf-8') as f:
            f.write(lines)
    except Exception:
        try:
            os.close(fd)
        except OSError:
            pass
        raise


def build_docker_create_command(cidfile_path: str, env_file_path: str, name: str, tmpfs_mounts: list = None) -> list:
    """Build docker create command using --cidfile receipt, tmpfs overrides, and NO published ports."""
    cmd = [
        'docker', 'create', '--pull=never',
        '--name', name,
        '--cidfile', cidfile_path,
        '--env-file', env_file_path,
    ]
    mounts = tmpfs_mounts if tmpfs_mounts is not None else ['/var/lib/postgresql/data']
    for dest in mounts:
        cmd.extend(['--tmpfs', f"{dest}:rw"])
    cmd.append(PG_IMAGE)
    return cmd


class DatabaseFixture:
    """Small fixture-local implementation of bounded docker container lifecycle."""

    def __init__(self):
        self.directory = Path(tempfile.mkdtemp(prefix='task28-sim-schema-', dir='/tmp/opencode'))
        os.chmod(self.directory, 0o700)
        self.name = 'task28-sim-pg-' + secrets.token_hex(8)
        self.passwords = [secrets.token_hex(32) for _ in range(4)]
        self.cidfile = self.directory / 'container.cid'
        self.env_file = self.directory / 'database.env'
        self.container_id = None
        self._created_receipt = False
        self.tmpfs_mounts = ['/var/lib/postgresql/data']
        self.owned_volumes = []
        self._effective_mounts_verified = None

    def prepare_image(self):
        """Inspect pinned image; only pull if absent locally."""
        try:
            inspected = subprocess.run(
                ['docker', 'image', 'inspect', PG_IMAGE],
                env=ENV, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
                timeout=IMAGE_INSPECT_TIMEOUT
            )
            if inspected.returncode == 0:
                vol_proc = subprocess.run(
                    ['docker', 'image', 'inspect', '-f', '{{json .Config.Volumes}}', PG_IMAGE],
                    env=ENV, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                    timeout=IMAGE_INSPECT_TIMEOUT
                )
                if vol_proc.returncode == 0 and vol_proc.stdout.strip():
                    try:
                        vols = json.loads(vol_proc.stdout.decode('utf-8'))
                        if isinstance(vols, dict) and vols:
                            self.tmpfs_mounts = list(vols.keys())
                    except Exception:
                        pass
                return
            err_msg = inspected.stderr.decode('utf-8', errors='replace').lower()
            if 'no such image' not in err_msg:
                raise RuntimeError('image_inspect_failed')
        except subprocess.TimeoutExpired:
            raise RuntimeError('image_inspect_timeout') from None
        except OSError:
            raise RuntimeError('image_inspect_failed') from None

        try:
            pull_proc = subprocess.run(
                ['docker', 'pull', PG_IMAGE],
                env=ENV, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
                timeout=IMAGE_PULL_TIMEOUT
            )
            if pull_proc.returncode != 0:
                raise RuntimeError('image_pull_failed')
        except subprocess.TimeoutExpired:
            raise RuntimeError('image_pull_timeout') from None
        except OSError:
            raise RuntimeError('image_pull_failed') from None

    def _load_and_verify_receipt(self) -> str | None:
        """Centralized receipt parsing and inspect verification.

        Requires:
        1. A real regular private receipt inside self.directory
        2. Exactly 64 lowercase hexadecimal characters
        3. Matching actual inspect ID before granting destructive ownership.
        """
        if not self.cidfile.exists():
            return None

        try:
            st = self.cidfile.lstat()
            if not stat.S_ISREG(st.st_mode) or self.cidfile.is_symlink():
                raise RuntimeError("invalid_cidfile_receipt: not regular file")
            if self.cidfile.resolve().parent != self.directory.resolve():
                raise RuntimeError("invalid_cidfile_receipt: outside directory")
        except OSError as e:
            raise RuntimeError(f"invalid_cidfile_receipt: stat error: {e}") from None

        raw = self.cidfile.read_text(encoding='utf-8').strip()
        cid = raw.lower()
        if not re.match(r'^[0-9a-f]{64}$', cid):
            raise RuntimeError("invalid_cidfile_receipt: malformed receipt content")

        # Verify against actual container inspect ID before granting destructive ownership
        try:
            inspect_proc = subprocess.run(
                ['docker', 'inspect', '-f', '{{.Id}}', cid],
                env=ENV, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                timeout=min(10, IMAGE_INSPECT_TIMEOUT)
            )
            if inspect_proc.returncode != 0:
                err_clean = sanitize_output(inspect_proc.stderr.decode('utf-8', errors='replace'), self.passwords)
                raise RuntimeError(f"inspect_failed_for_receipt: {err_clean}")
            inspected_id = inspect_proc.stdout.decode('utf-8', errors='replace').strip().lower()
            if inspected_id != cid:
                raise RuntimeError(f"inspected_id_mismatch: {inspected_id} != {cid}")
        except subprocess.TimeoutExpired:
            raise RuntimeError("inspect_timeout_for_receipt") from None
        except OSError as e:
            raise RuntimeError(f"inspect_os_error_for_receipt: {e}") from None

        self.container_id = cid
        self._created_receipt = True
        return cid

    def _inspect_and_verify_mounts(self):
        """Verify effective mounts on the created container to ensure no anonymous volumes exist."""
        if not self.container_id:
            raise RuntimeError("mount_inspect_without_container_id")
        try:
            mount_proc = subprocess.run(
                ['docker', 'inspect', '-f', '{{json .Mounts}}', self.container_id],
                env=ENV, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                timeout=min(10, IMAGE_INSPECT_TIMEOUT)
            )
            if mount_proc.returncode != 0:
                err_clean = sanitize_output(mount_proc.stderr.decode('utf-8', errors='replace'), self.passwords)
                raise RuntimeError(f"mount_inspect_failed: {err_clean}")
            raw_stdout = mount_proc.stdout.decode('utf-8', errors='replace').strip() or "[]"
            mounts = json.loads(raw_stdout)
            self.owned_volumes = [m.get("Name") for m in mounts if m.get("Type") == "volume" and m.get("Name")]
            if self.owned_volumes:
                self._effective_mounts_verified = False
            else:
                self._effective_mounts_verified = True
        except subprocess.TimeoutExpired:
            raise RuntimeError("mount_inspect_timeout") from None
        except OSError as e:
            raise RuntimeError(f"mount_inspect_failed: {e}") from None
        except Exception as e:
            raise RuntimeError(f"mount_inspect_failed: {e}") from None

    def create(self):
        """Create container using --cidfile; capture exact container ID receipt."""
        create_private_env_file(self.env_file, self.directory, {
            'POSTGRES_USER': 'fixture_admin',
            'POSTGRES_DB': 'fixture_service',
            'POSTGRES_PASSWORD': self.passwords[0],
            'BACKEND_DB_PASSWORD': self.passwords[1],
            'AUTH_DB_PASSWORD': self.passwords[2],
            'STORAGE_DB_PASSWORD': self.passwords[3],
        })

        cmd = build_docker_create_command(str(self.cidfile), str(self.env_file), self.name, tmpfs_mounts=self.tmpfs_mounts)
        create_err = None
        try:
            proc = subprocess.run(
                cmd, env=ENV, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                timeout=CONTAINER_CREATE_TIMEOUT
            )
            if proc.returncode != 0:
                err_clean = sanitize_output(proc.stderr.decode('utf-8', errors='replace'), self.passwords)
                create_err = RuntimeError(f"create_failed: {err_clean}")
        except subprocess.TimeoutExpired:
            create_err = RuntimeError("create_timeout")
        except OSError as e:
            create_err = RuntimeError(f"create_os_error: {e}")

        # Centralized receipt check: attempt to load receipt whether proc succeeded or failed
        receipt_loaded = False
        receipt_err = None
        try:
            cid = self._load_and_verify_receipt()
            if cid:
                receipt_loaded = True
        except Exception as re_err:
            receipt_err = re_err

        if create_err:
            if receipt_err:
                create_err.__context__ = receipt_err
            raise create_err

        if receipt_err:
            raise receipt_err
        if not receipt_loaded or not self.container_id:
            raise RuntimeError("missing_cidfile_receipt")

        self._inspect_and_verify_mounts()

    def start(self):
        """Start container using verified container ID."""
        if not self.container_id:
            raise RuntimeError("start_without_container_id")
        try:
            proc = subprocess.run(
                ['docker', 'start', self.container_id],
                env=ENV, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
                timeout=CONTAINER_START_TIMEOUT
            )
            if proc.returncode != 0:
                err_clean = sanitize_output(proc.stderr.decode('utf-8', errors='replace'), self.passwords)
                raise RuntimeError(f"start_failed: {err_clean}")
        except subprocess.TimeoutExpired:
            raise RuntimeError("start_timeout") from None
        except OSError as e:
            raise RuntimeError(f"start_os_error: {e}") from None

    def wait_ready(self, timeout=READINESS_TOTAL_TIMEOUT):
        """Wait for database TCP readiness with bounded probe loops."""
        if not self.container_id:
            raise RuntimeError("wait_without_container_id")
        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RuntimeError("database_not_ready_timeout")
            try:
                probe = subprocess.run(
                    ['docker', 'exec', self.container_id, 'pg_isready', '-h', '127.0.0.1',
                     '-U', 'fixture_admin', '-d', 'fixture_service'],
                    env=ENV, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                    timeout=min(PROBE_TIMEOUT, remaining)
                )
                if probe.returncode == 0:
                    return
            except subprocess.TimeoutExpired:
                pass
            except OSError:
                raise RuntimeError("database_probe_failed") from None

            remaining = deadline - time.monotonic()
            if remaining > 0:
                try:
                    state = subprocess.run(
                        ['docker', 'inspect', '-f', '{{.State.Status}}', self.container_id],
                        env=ENV, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                        timeout=min(PROBE_TIMEOUT, remaining)
                    )
                    if state.returncode == 0:
                        status = state.stdout.strip().decode('ascii', errors='replace')
                        if status in ('exited', 'dead', 'removing'):
                            raise RuntimeError(f"database_terminal_state:{status}")
                except subprocess.TimeoutExpired:
                    pass
                except OSError:
                    raise RuntimeError("database_probe_failed") from None

            remaining = deadline - time.monotonic()
            if remaining > 0:
                time.sleep(min(1.0, remaining))

    def sql(self, data: bytes, expect: bool = True):
        """Run SQL against database using verified container ID."""
        if not self.container_id:
            raise RuntimeError("sql_without_container_id")
        try:
            p = subprocess.run(
                ['docker', 'exec', '-i', self.container_id, 'psql', '-X', '-U',
                 'fixture_admin', '-d', 'fixture_service', '-v', 'ON_ERROR_STOP=1'],
                input=data, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                env=ENV, timeout=SQL_TIMEOUT
            )
            output = p.stdout + p.stderr
            if any(s.encode() in output for s in self.passwords):
                raise RuntimeError("secret output rejected")
            if (p.returncode == 0) != expect:
                clean_err = sanitize_output(output.decode('utf-8', errors='replace'), self.passwords)
                raise RuntimeError(f"unexpected SQL outcome (returncode={p.returncode}, expect={expect}): {clean_err}")
            return p.returncode, output
        except subprocess.TimeoutExpired:
            raise RuntimeError("sql_timeout") from None
        except OSError as e:
            raise RuntimeError(f"sql_os_error: {e}") from None

    def cleanup(self) -> bool:
        """Destructively clean up container and directory with verified absence.

        Returns True if absence verified, raises on error."""
        container_absence_verified = False
        container_error = None

        if self.container_id:
            target = self.container_id
            try:
                if self._effective_mounts_verified is False:
                    raise RuntimeError("absence_verification_failed: effective mount uncertainty")

                # Remove container by exact ID only
                rm_proc = subprocess.run(
                    ['docker', 'rm', '-f', '-v', target],
                    env=ENV, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
                    timeout=CLEANUP_TIMEOUT
                )

                # Authoritatively verify container absence via inspect
                inspect_proc = subprocess.run(
                    ['docker', 'inspect', target],
                    env=ENV, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
                    timeout=10
                )
                err_lower = inspect_proc.stderr.decode('utf-8', errors='replace').lower()
                is_absent = inspect_proc.returncode != 0 and (
                    f"no such container: {target}".lower() in err_lower or
                    f"no such object: {target}".lower() in err_lower
                )
                if not is_absent:
                    raise RuntimeError(f"absence_verification_failed: {sanitize_output(inspect_proc.stderr.decode('utf-8', errors='replace'), self.passwords)}")

                # Authoritatively verify absence of any owned anonymous volumes
                for vol in self.owned_volumes:
                    vol_proc = subprocess.run(
                        ['docker', 'volume', 'inspect', vol],
                        env=ENV, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
                        timeout=10
                    )
                    vol_err = vol_proc.stderr.decode('utf-8', errors='replace').lower()
                    if vol_proc.returncode == 0:
                        raise RuntimeError(f"volume_absence_verification_failed: volume {vol} still exists")
                    if "no such volume" not in vol_err:
                        raise RuntimeError(f"volume_absence_verification_failed: daemon error for {vol}")

                container_absence_verified = True
            except subprocess.TimeoutExpired:
                container_error = RuntimeError("cleanup_timeout")
            except OSError as e:
                container_error = RuntimeError(f"cleanup_os_error: {e}")
            except Exception as e:
                container_error = e
        else:
            # No container was created/owned
            container_absence_verified = True

        # Ensure private directory cleanup is STILL attempted when container cleanup fails
        dir_error = None
        if self.directory.exists():
            try:
                shutil.rmtree(self.directory, ignore_errors=False)
                if self.directory.exists():
                    dir_error = RuntimeError("directory_absence_failed")
            except Exception as de:
                dir_error = de

        if container_error:
            if dir_error:
                if hasattr(container_error, "add_note"):
                    container_error.add_note(f"Directory cleanup also failed: {dir_error}")
                container_error.__context__ = dir_error
            raise container_error

        if dir_error:
            raise dir_error

        return container_absence_verified


def safe_execute_with_cleanup(fixture: DatabaseFixture, action):
    """Execute action with fixture. If action fails and cleanup fails, preserve

    primary error. If action succeeds but cleanup fails/uncertain, raise RuntimeError."""
    primary_error = None
    res = None
    try:
        res = action()
    except Exception as e:
        primary_error = e

    cleanup_uncertain = False
    cleanup_error = None
    try:
        cleaned = fixture.cleanup()
        if not cleaned:
            cleanup_uncertain = True
    except Exception as ce:
        cleanup_error = ce
        cleanup_uncertain = True

    if primary_error:
        if cleanup_error:
            # Attach cleanup error context/note to primary error without masking
            if hasattr(primary_error, "add_note"):
                primary_error.add_note(f"Cleanup also failed: {cleanup_error}")
            primary_error.__context__ = cleanup_error
        raise primary_error

    if cleanup_uncertain or cleanup_error:
        raise RuntimeError(f"cleanup_uncertainty: {cleanup_error}")

    return res


def run_schema_migration(fixture: DatabaseFixture, red_mode: bool = False):
    """Execute migration pipeline and verifications."""
    fixture.prepare_image()
    fixture.create()
    fixture.start()
    fixture.wait_ready()

    verifier19 = ROOT / 'scripts/sql/verify-migration-000019.sql'
    regression19 = ROOT / 'scripts/sql/task28_simplified_schema_regression.sql'

    # Apply migrations 000000..000018 first
    for migration in sorted((ROOT / 'migrations').iterdir()):
        if int(migration.name[:6]) > 18:
            continue
        if migration.suffix == '.sh':
            subprocess.run(
                ['docker', 'cp', str(migration), f"{fixture.container_id}:/roles.sh"],
                env=ENV, check=True, stdout=subprocess.DEVNULL, timeout=30
            )
            subprocess.run(
                ['docker', 'exec', fixture.container_id, 'sh', '/roles.sh'],
                env=ENV, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=30
            )
        elif migration.suffix == '.sql':
            fixture.sql(migration.read_bytes())

    # RED PROOF (if red_mode):
    if red_mode:
        rc, out = fixture.sql(verifier19.read_bytes(), expect=False)
        if b'Migration 000019 is not recorded' not in out:
            raise RuntimeError('RED proof failed: verifier19 did not report missing 000019')
        print('RED VERIFIED: verify-migration-000019.sql fails on schema 018 as expected', flush=True)

        rc_reg, out_reg = fixture.sql(regression19.read_bytes(), expect=False)
        # Exact missing-feature oracle: must fail specifically due to constraint or missing snapshot_observed requirement,
        # never arbitrary or generic failure
        if b'violates check constraint "chk_mqtt_operation_contract"' not in out_reg:
            raise RuntimeError('RED proof failed: regression19 did not fail with exact chk_mqtt_operation_contract constraint violation')
        print('RED VERIFIED: task28_simplified_schema_regression.sql fails on schema 018 as expected (exact chk_mqtt_operation_contract failure)', flush=True)
        return

    # GREEN PATH: Apply migration 000019
    mig19 = ROOT / 'migrations/000019_mqtt_simplified_journal.up.sql'
    fixture.sql(mig19.read_bytes())
    print('PASS applied migration 000019', flush=True)

    # Run verify-migration-000019.sql
    rc, out = fixture.sql(verifier19.read_bytes(), expect=True)
    if b'Migration 000019 verification passed.' not in out:
        raise RuntimeError('verifier19 failed to confirm migration 000019')
    print('PASS verify-migration-000019.sql confirmed clean schema 019', flush=True)

    # Run regression test suite
    rc, out = fixture.sql(regression19.read_bytes(), expect=True)
    if b'Schema 019 regression tests passed.' not in out:
        raise RuntimeError('regression19 failed')
    print('PASS task28_simplified_schema_regression.sql passed all cases', flush=True)

    # Verify idempotency / reapply
    fixture.sql(mig19.read_bytes())
    rc, out = fixture.sql(verifier19.read_bytes(), expect=True)
    if b'Migration 000019 verification passed.' not in out:
        raise RuntimeError('verifier19 failed after reapply')
    print('PASS migration 000019 reapply is idempotent and clean', flush=True)


def main():
    red_mode = '--red' in sys.argv
    fixture = DatabaseFixture()
    safe_execute_with_cleanup(fixture, lambda: run_schema_migration(fixture, red_mode=red_mode))
    print('PASS strict owned cleanup absence (no published ports)', flush=True)


if __name__ == '__main__':
    main()
