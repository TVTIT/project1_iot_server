#!/usr/bin/env python3
"""Preserve standalone migration CI coverage in one owned, fresh fixture.

No pulls, deployment configuration, raw Docker output, or secret arguments.
The full upgrade/concurrency harness remains separate.
"""
from pathlib import Path
import re
import secrets
import signal
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
IMAGE = 'timescale/timescaledb@sha256:289d55704b1b3ee8263cd3805c6930f9cd54506835a8f19f9b85dad17d5c5a8a'
LABEL = 'stage2.migration-compat'


class Failure(Exception):
    """Fixed, safe diagnostics only."""


class Fixture:
    def __init__(self):
        self.nonce = secrets.token_hex(16)
        self.id = None
        self.passwords = {name: secrets.token_hex(32) for name in (
            'POSTGRES_PASSWORD', 'AUTH_DB_PASSWORD', 'STORAGE_DB_PASSWORD', 'BACKEND_DB_PASSWORD')}

    def command(self, args, *, input=None, timeout=30):
        try:
            result = subprocess.run(args, input=input, text=True, capture_output=True, timeout=timeout)
        except (OSError, subprocess.TimeoutExpired):
            raise Failure('Fixture command unavailable or timed out.') from None
        if result.returncode:
            # stderr may contain only SQL diagnostics from this disposable fixture;
            # never echo command arguments or stdin (which carries credentials).
            detail = next((line.strip() for line in result.stderr.splitlines()
                           if 'ERROR:' in line or 'FATAL:' in line), '')
            raise Failure('Fixture command failed.' + ((' ' + detail[:240]) if detail else ''))
        return result.stdout.strip()

    def start(self):
        self.command(['docker', 'image', 'inspect', '--format', '{{.Id}}', IMAGE])
        env = dict(self.passwords, POSTGRES_USER='stage2_admin', POSTGRES_DB='stage2_test')
        # Docker reads its env-file from protected stdin; no file/artifact remains.
        identifier = self.command([
            'docker', 'create', '--pull=never', '--platform', 'linux/amd64',
            '--name', 'migration-compat-' + self.nonce, '--label', LABEL + '=' + self.nonce,
            '--env-file', '/dev/stdin', '-v', str(ROOT / 'migrations') + ':/docker-entrypoint-initdb.d:ro',
            IMAGE], input=''.join(f'{key}={value}\n' for key, value in env.items()))
        if not re.fullmatch(r'[0-9a-f]{64}', identifier):
            raise Failure('Invalid fixture receipt.')
        self.id = identifier
        print('Owned compatibility container created: ' + self.id, flush=True)
        self.command(['docker', 'start', self.id])

    def sql(self, query, *, role='stage2_admin', password=None):
        args = ['docker', 'exec', '-i', self.id]
        if password is not None:
            args += ['sh', '-c', 'IFS= read -r PGPASSWORD; export PGPASSWORD; exec "$@"', 'sh']
        args += ['psql', '-v', 'ON_ERROR_STOP=1', '-U', role, '-d', 'stage2_test', '-At']
        if password is not None:
            args += ['-h', '127.0.0.1']
        return self.command(args, input=(password + '\n' if password is not None else '') + query + '\n')

    def expect(self, query, expected):
        if self.sql(query) != expected:
            raise Failure('Compatibility assertion failed.')

    def wait_ready(self, attempts=90):
        for _ in range(attempts):
            try:
                if self.sql(
                    "SELECT count(*) FROM schema_migrations WHERE version = 20 AND name = 'stage3_telemetry_ingestion';",
                    password=self.passwords['POSTGRES_PASSWORD'],
                ) == '1':
                    return
            except Failure:
                pass
            time.sleep(1)
        raise Failure('Database readiness failed.')

    def login(self, role, password):
        if self.sql('SELECT current_user;', role=role, password=password) != role:
            raise Failure('Service role TCP login failed.')

    def verify(self):
        for name in ('verify-schema-hardening.sql', 'verify-migration-000009.sql'):
            self.sql((ROOT / 'scripts/sql' / name).read_text())
            print('Compatibility verifier passed: ' + name, flush=True)
        # Match original standalone ordering: fresh schema first, then Auth setup.
        self.sql("CREATE TABLE auth.users (id uuid PRIMARY KEY, raw_user_meta_data jsonb NOT NULL DEFAULT '{}');")
        for _ in range(2):
            self.sql((ROOT / 'migrations/000004_supabase_compat.up.sql').read_text())
        for query, expected in (
            ("SELECT count(*) FROM schema_migrations WHERE version = 4 AND name = 'supabase_compat';", '1'),
            ("SELECT count(*) FROM pg_policies WHERE schemaname = 'public';", '6'),
            ("SELECT count(*) FROM pg_trigger WHERE tgname = 'on_auth_user_created';", '1'),
            ("SELECT count(*) FROM pg_constraint WHERE conname = 'profiles_id_fkey';", '1'),
        ):
            self.expect(query, expected)
        self.login('supabase_auth_admin', self.passwords['AUTH_DB_PASSWORD'])
        self.login('supabase_storage_admin', self.passwords['STORAGE_DB_PASSWORD'])
        print('Supabase repeated compatibility, RLS=6, trigger, FK and TCP logins passed.', flush=True)
        # Apply and verify migration 21 once Auth tables and triggers exist
        for _ in range(2):
            self.sql((ROOT / 'migrations/000021_admin_user_management.up.sql').read_text())
        self.sql((ROOT / 'scripts/sql/verify-migration-000021.sql').read_text())
        self.expect("SELECT count(*) FROM schema_migrations WHERE version = 21 AND name = 'admin_user_management';", '1')
        print('Migration 21 repeated compatibility, least-privilege grants and verifier passed.', flush=True)
        self.expect("SELECT has_table_privilege('iot_backend_app', 'public.user_gateways', 'DELETE');", 'f')
        for _ in range(2):
            self.sql((ROOT / 'migrations/000022_gateway_membership_revocation.up.sql').read_text())
        self.sql((ROOT / 'scripts/sql/verify-migration-000022.sql').read_text())
        self.login('iot_backend_app', self.passwords['BACKEND_DB_PASSWORD'])
        self.sql('DELETE FROM public.user_gateways WHERE false;', role='iot_backend_app',
                 password=self.passwords['BACKEND_DB_PASSWORD'])
        print('Migration 22 repeated compatibility, runtime DELETE and human-role denials passed.', flush=True)
        for _ in range(2):
            self.sql((ROOT / 'migrations/000023_sensor_control_intent.up.sql').read_text())
        self.sql((ROOT / 'scripts/sql/verify-migration-000023.sql').read_text())
        self.expect("SELECT count(*) FROM schema_migrations WHERE version=23 AND name='sensor_control_intent';", '1')
        print('Migration 23 repeated compatibility, control-intent grants and terminal passed.', flush=True)
        # Exercise the actual post-Auth migration against this disposable DB.
        # Keep a source row whose original sample timestamp must be preserved,
        # plus an unverifiable legacy ledger row that must remain NULL.
        self.sql("""INSERT INTO gateways(gateway_id,name) VALUES ('migration-fixture','migration fixture');
            INSERT INTO sensors(gateway_id,sensor_id,name) VALUES
                ('migration-fixture','sensor-a','sensor a'),('migration-fixture','sensor-b','sensor b');
            INSERT INTO processed_messages
            (gateway_id,message_id,payload_hash,source_topic,sensor_id,boot_id,first_sequence,last_sequence,sample_count)
            VALUES ('migration-fixture','00000000-0000-0000-0000-000000000024','hash','site/migration-fixture/telemetry',
                    'sensor-a','00000000-0000-0000-0000-000000000124',42,42,1),
                   ('migration-fixture','00000000-0000-0000-0000-000000000025','hash','site/migration-fixture/telemetry',
                    'sensor-b','00000000-0000-0000-0000-000000000125',43,43,1),
                   ('migration-fixture','00000000-0000-0000-0000-000000000026','hash','site/migration-fixture/telemetry',
                    'sensor-a','00000000-0000-0000-0000-000000000126',44,44,1);
            INSERT INTO telemetry(measured_at,gateway_id,sensor_id,sequence_num,value,boot_id,message_id,sample_index)
            VALUES ('2026-01-02T03:04:05.123456Z','migration-fixture','sensor-a',42,1,
                    '00000000-0000-0000-0000-000000000124','00000000-0000-0000-0000-000000000024',0),
                   ('2026-01-02T03:04:06Z','migration-fixture','sensor-b',44,1,
                    '00000000-0000-0000-0000-000000000126','00000000-0000-0000-0000-000000000026',0);""")
        migration24 = (ROOT / 'migrations/000024_telemetry_ledger_timestamp.up.sql').read_text()
        try:
            self.sql(migration24)
            raise Failure('Conflicting provenance was not refused.')
        except Failure as error:
            if 'conflicting telemetry provenance' not in str(error).lower():
                raise
        self.expect("SELECT count(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='processed_messages' AND column_name='timestamp';", '0')
        self.expect("SELECT count(*) FROM schema_migrations WHERE version=24;", '0')
        self.expect("SELECT count(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='processed_messages' AND column_name='timestamp';", '0')
        self.sql("DELETE FROM telemetry WHERE message_id='00000000-0000-0000-0000-000000000026'; DELETE FROM processed_messages WHERE message_id='00000000-0000-0000-0000-000000000026';")
        self.sql(migration24)
        self.sql(migration24)
        self.expect("SELECT timestamp::text FROM processed_messages WHERE message_id='00000000-0000-0000-0000-000000000024';", '2026-01-02 03:04:05.123456+00')
        self.expect("SELECT timestamp IS NULL FROM processed_messages WHERE message_id='00000000-0000-0000-0000-000000000025';", 't')
        self.expect("SELECT count(*) FROM schema_migrations WHERE version=24 AND name='telemetry_ledger_timestamp';", '1')
        self.expect("SELECT count(*) FROM processed_messages WHERE timestamp IS NULL;", '1')
        self.expect("SELECT has_table_privilege('iot_backend_app','public.processed_messages','SELECT');", 't')
        self.expect("SELECT pg_get_indexdef('processed_messages_gateway_sensor_message_timestamp'::regclass);", 'CREATE UNIQUE INDEX processed_messages_gateway_sensor_message_timestamp ON public.processed_messages USING btree (gateway_id, sensor_id, message_id, "timestamp")')
        self.expect("SELECT count(*) FROM pg_constraint WHERE conrelid='processed_messages'::regclass AND conname='processed_messages_pkey';", '1')
        self.expect("SELECT count(*) FROM pg_class WHERE oid='processed_stream_range'::regclass;", '1')
        print('Migration 24 native sample-zero backfill, legacy NULL, unique index, retained guards and reapply passed.', flush=True)

        migration25 = (ROOT / 'migrations/000025_drop_telemetry_sensor_fk.up.sql').read_text()
        self.sql(migration25)
        self.sql(migration25)
        self.expect("SELECT count(*) FROM schema_migrations WHERE version=25 AND name='drop_telemetry_sensor_fk';", '1')
        self.expect("SELECT count(*) FROM pg_constraint WHERE conrelid='public.telemetry'::regclass AND conname='fk_telemetry_sensor';", '0')
        self.expect("SELECT count(*) FROM pg_trigger WHERE tgrelid='public.sensors'::regclass AND tgname='trg_sensors_restrict_telemetry';", '1')
        print('Migration 25 native telemetry FK removal and sensors reverse restrict trigger passed.', flush=True)

        migration26 = (ROOT / 'migrations/000026_drop_telemetry_redundant_indexes.up.sql').read_text()
        self.sql(migration26)
        self.sql(migration26)
        self.expect("SELECT count(*) FROM schema_migrations WHERE version=26 AND name='drop_telemetry_redundant_indexes';", '1')
        self.expect("SELECT count(*) FROM pg_indexes WHERE schemaname='public' AND indexname='telemetry_message_sample';", '0')
        self.expect("SELECT count(*) FROM pg_constraint WHERE conrelid='public.telemetry'::regclass AND conname='telemetry_pkey';", '0')
        self.expect("SELECT count(*) FROM pg_indexes WHERE schemaname='public' AND tablename='telemetry' AND indexname='idx_telemetry_gateway_sensor_time';", '1')
        print('Migration 26 native redundant index removal on telemetry passed.', flush=True)

    def cleanup(self):
        if self.id is None:
            return
        owner = self.command(['docker', 'inspect', '-f', '{{index .Config.Labels "' + LABEL + '"}}', self.id])
        if owner != self.nonce:
            raise Failure('Cleanup refused incorrect ownership.')
        self.command(['docker', 'rm', '-fv', self.id])
        # Successful daemon query is required; command errors are NOT absence.
        if self.command(['docker', 'container', 'ls', '-aq', '--no-trunc', '--filter', 'id=' + self.id]):
            raise Failure('Cleanup verification failed.')
        print('Owned compatibility cleanup verified: ' + self.id, flush=True)
        self.id = None


def main():
    def interrupted(signum, frame):
        raise Failure('Compatibility fixture interrupted.')

    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    fixture = Fixture()
    status = 0
    try:
        fixture.start()
        fixture.wait_ready()
        print('Fresh compatibility schema 20 ready.', flush=True)
        fixture.verify()
    except Failure as error:
        print(str(error), file=sys.stderr)
        status = 1
    finally:
        try:
            fixture.cleanup()
        except Failure:
            print('Compatibility cleanup failed; success vetoed.', file=sys.stderr)
            status = 1
    if status == 0:
        print('Migration compatibility tests passed.', flush=True)
    return status


if __name__ == '__main__':
    sys.exit(main())
