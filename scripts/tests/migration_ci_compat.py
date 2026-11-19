#!/usr/bin/env python3
"""Preserve standalone migration CI coverage in one owned, fresh fixture.

No pulls, deployment configuration, raw Docker output, or secret arguments.
The full upgrade/concurrency harness remains separate.
"""
from pathlib import Path
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
import re
import secrets
import signal
import shutil
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[2]
IMAGE = 'timescale/timescaledb@sha256:289d55704b1b3ee8263cd3805c6930f9cd54506835a8f19f9b85dad17d5c5a8a'
LABEL = 'stage2.migration-compat'
TERMINAL_SCHEMA_VERSION = 27


class Failure(Exception):
    """Fixed, safe diagnostics only."""


class Fixture:
    def __init__(self):
        self.nonce = secrets.token_hex(16)
        self.id = None
        self.init_dir = None
        self.init_root = None
        self.init_identity = None
        self.fixture_clock = None
        self.retention_script_staged = False
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

    @staticmethod
    def trusted_temp_root():
        for candidate in (Path('/var/tmp/opencode'), Path('/tmp/opencode')):
            try:
                metadata = candidate.lstat()
            except OSError:
                continue
            if (candidate.is_symlink() or not candidate.is_dir()
                    or metadata.st_uid != os.getuid() or not os.access(candidate, os.W_OK | os.X_OK)):
                continue
            return candidate
        raise Failure('No existing user-owned fixture scratch directory is available.')

    def start(self):
        self.command(['docker', 'image', 'inspect', '--format', '{{.Id}}', IMAGE])
        self.init_root = self.trusted_temp_root()
        try:
            self.init_dir = Path(tempfile.mkdtemp(prefix='stage6-migration-init-', dir=self.init_root))
            info = self.init_dir.stat()
            self.init_identity = (info.st_dev, info.st_ino, info.st_uid)
            shutil.copy2(ROOT / 'migrations/000000_configure_supabase_roles.sh', self.init_dir)
            for migration in (ROOT / 'migrations').glob('*.up.sql'):
                match = re.match(r'(\d{6})_', migration.name)
                if match and int(match.group(1)) <= 20:
                    shutil.copy2(migration, self.init_dir / migration.name)
            self.init_dir.chmod(0o755)
        except OSError:
            raise Failure('Could not stage the owned migration init directory.') from None
        env = dict(self.passwords, POSTGRES_USER='stage2_admin', POSTGRES_DB='stage2_test')
        # Docker reads its env-file from protected stdin; no file/artifact remains.
        identifier = self.command([
            'docker', 'create', '--pull=never', '--platform', 'linux/amd64',
            '--name', 'migration-compat-' + self.nonce, '--label', LABEL + '=' + self.nonce,
            '--env-file', '/dev/stdin', '-v', str(self.init_dir) + ':/docker-entrypoint-initdb.d:ro',
            IMAGE], input=''.join(f'{key}={value}\n' for key, value in env.items()))
        if not re.fullmatch(r'[0-9a-f]{64}', identifier):
            raise Failure('Invalid fixture receipt.')
        self.id = identifier
        print('Owned compatibility container created: ' + self.id, flush=True)
        self.command(['docker', 'start', self.id])

    def sql(self, query, *, role='stage2_admin', password=None, expected_schema_terminal=None):
        args = ['docker', 'exec', '-i', self.id]
        if password is not None:
            args += ['sh', '-c', 'IFS= read -r PGPASSWORD; export PGPASSWORD; exec "$@"', 'sh']
        args += ['psql', '-v', 'ON_ERROR_STOP=1', '-U', role, '-d', 'stage2_test', '-At']
        if expected_schema_terminal is not None:
            args += ['-v', f'expected_schema_terminal={expected_schema_terminal}']
        if password is not None:
            args += ['-h', '127.0.0.1']
        return self.command(args, input=(password + '\n' if password is not None else '') + query + '\n')

    def retention_sql(self, mode=None, target='stage2_test', confirm=None):
        if not self.retention_script_staged:
            self.command(['docker', 'cp', str(ROOT / 'scripts/sql/stage6-command-retention.sql'),
                          self.id + ':/tmp/stage6-command-retention.sql'])
            self.command(['docker', 'cp', str(ROOT / 'scripts/sql/stage6-command-retention-candidates.sql'),
                          self.id + ':/tmp/stage6-command-retention-candidates.sql'])
            self.retention_script_staged = True
        args = ['docker', 'exec', '-i', self.id, 'psql', '-X', '-v', 'ON_ERROR_STOP=1',
                '-U', 'stage2_admin', '-d', 'stage2_test', '-At',
                '-v', 'target_database=' + target]
        if mode is not None:
            args += ['-v', 'retention_mode=' + mode]
        if confirm is not None:
            args += ['-v', 'confirm_apply_target=' + confirm]
        args += ['-f', '/tmp/stage6-command-retention.sql']
        return self.command(args)

    def expect(self, query, expected):
        got = self.sql(query)
        if got != expected:
            raise Failure(f'Compatibility assertion failed: query={query} got={got!r} expected={expected!r}')

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
        self.sql((ROOT / 'scripts/sql/verify-migration-000021.sql').read_text(),
                 expected_schema_terminal=21)
        self.expect("SELECT count(*) FROM schema_migrations WHERE version = 21 AND name = 'admin_user_management';", '1')
        print('Migration 21 repeated compatibility, least-privilege grants and verifier passed.', flush=True)
        self.expect("SELECT has_table_privilege('iot_backend_app', 'public.user_gateways', 'DELETE');", 'f')
        for _ in range(2):
            self.sql((ROOT / 'migrations/000022_gateway_membership_revocation.up.sql').read_text())
        self.sql((ROOT / 'scripts/sql/verify-migration-000022.sql').read_text(),
                 expected_schema_terminal=22)
        self.login('iot_backend_app', self.passwords['BACKEND_DB_PASSWORD'])
        self.sql('DELETE FROM public.user_gateways WHERE false;', role='iot_backend_app',
                 password=self.passwords['BACKEND_DB_PASSWORD'])
        print('Migration 22 repeated compatibility, runtime DELETE and human-role denials passed.', flush=True)
        for _ in range(2):
            self.sql((ROOT / 'migrations/000023_sensor_control_intent.up.sql').read_text())
        self.sql((ROOT / 'scripts/sql/verify-migration-000023.sql').read_text(),
                 expected_schema_terminal=23)
        self.expect("SELECT count(*) FROM schema_migrations WHERE version=23 AND name='sensor_control_intent';", '1')
        print('Migration 23 repeated compatibility, control-intent grants and terminal passed.', flush=True)
        # Exercise the actual post-Auth migration against this disposable DB.
        # Keep a source row whose original sample timestamp must be preserved,
        # plus an unverifiable legacy ledger row that must remain NULL.
        self.fixture_clock = datetime.now(timezone.utc)
        clock = self.fixture_clock
        pending_issued = clock
        pending_expires = clock + timedelta(minutes=30)
        completed_issued = clock - timedelta(minutes=3)
        completed_expires = clock - timedelta(minutes=1)
        completed_at = clock - timedelta(minutes=2)
        def wire_time(value):
            return value.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        pending_id = '00000000-0000-0000-0000-000000000001'
        completed_id = '00000000-0000-0000-0000-000000000002'
        pending_entity = 'urn:ngsi-ld:Sensor:migration-fixture:sensor-a'
        completed_entity = 'urn:ngsi-ld:Sensor:migration-fixture:sensor-b'
        pending_command = {
            'protocol_version': 1, 'message_type': 'command', 'command_id': pending_id,
            'gateway_id': 'migration-fixture', 'entity_id': pending_entity,
            'action': 'update_configuration', 'parameters': {'sample_interval_us': 100000},
            'desired_version': '1', 'issued_at': wire_time(pending_issued),
            'expires_at': wire_time(pending_expires),
        }
        completed_command = {
            'protocol_version': 1, 'message_type': 'command', 'command_id': completed_id,
            'gateway_id': 'migration-fixture', 'entity_id': completed_entity,
            'action': 'update_configuration', 'parameters': {'sample_interval_us': 200000},
            'desired_version': '2', 'issued_at': wire_time(completed_issued),
            'expires_at': wire_time(completed_expires),
        }
        result_envelope = {
            'protocol_version': 1, 'message_type': 'command_result', 'command_id': completed_id,
            'gateway_id': 'migration-fixture', 'entity_id': completed_entity,
            'desired_version': '2', 'status': 'succeeded',
            'reported_state': {'sample_interval_us': 200000},
            'completed_at': wire_time(completed_at),
        }
        pending_bytes = json.dumps(pending_command, separators=(',', ':'))
        completed_bytes = json.dumps(completed_command, separators=(',', ':'))
        result_bytes = json.dumps(result_envelope, separators=(',', ':'))
        fingerprint = hashlib.sha256(json.dumps(
            ['update_configuration', '10', '0'], separators=(',', ':')).encode()).hexdigest()
        response = {
            'entity_id': pending_entity, 'command_id': pending_id, 'status': 'pending',
            'requested_frequency_hz': 10, 'effective_frequency_hz': 10,
            'sample_interval_us': 100000, 'desired_version': '1',
            'expires_at': wire_time(pending_expires),
            'status_url': '/v1/digital-twins/' + pending_entity + '/commands/' + pending_id,
        }
        receipt_bytes = json.dumps(response, separators=(',', ':'))
        if (pending_command['desired_version'] != '1'
                or not re.fullmatch(r'.*\.\d{6}Z', pending_command['issued_at'])
                or not re.fullmatch(r'.*\.\d{6}Z', pending_command['expires_at'])
                or not re.fullmatch(r'.*\.\d{6}Z', completed_command['issued_at'])
                or not re.fullmatch(r'.*\.\d{6}Z', completed_command['expires_at'])
                or not re.fullmatch(r'.*\.\d{6}Z', result_envelope['completed_at'])
                or result_envelope['desired_version'] != completed_command['desired_version']
                or result_envelope['command_id'] != completed_command['command_id']
                or completed_at < completed_issued or completed_at > completed_expires):
            raise Failure('Synthetic Stage 5 wire fixture failed its contract checks.')
        def sql_literal(value):
            return "'" + value.replace("'", "''") + "'"
        pending_payload_sql = sql_literal(pending_bytes)
        completed_payload_sql = sql_literal(completed_bytes)
        result_payload_sql = sql_literal(result_bytes)
        pending_expiry_sql = sql_literal(wire_time(pending_expires))
        pending_issued_sql = sql_literal(wire_time(pending_issued))
        completed_issued_sql = sql_literal(wire_time(completed_issued))
        completed_expiry_sql = sql_literal(wire_time(completed_expires))
        completed_at_sql = sql_literal(wire_time(completed_at))
        receipt_sql = sql_literal(receipt_bytes)
        fingerprint_sql = sql_literal(fingerprint)
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

        # The data-bearing Stage 5 upgrade must reach the genuine schema-26
        # boundary before migration 27 is first applied.
        self.expect("SELECT count(*) FROM schema_migrations WHERE version=26 AND name='drop_telemetry_redundant_indexes';", '1')
        self.expect("SELECT count(*) FROM schema_migrations WHERE version=27;", '0')
        self.expect("SELECT count(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='twin_commands' AND column_name='stage6_managed';", '0')

        # Seed representative Stage 5 data on schema 26 before migration 27:
        # 1. User profile
        self.sql("INSERT INTO auth.users(id) VALUES ('11111111-1111-1111-1111-111111111111');")
        # 2. Twin entities and twin states
        self.sql("""INSERT INTO twin_entities(id, entity_id, entity_type, gateway_id)
            VALUES ('22222222-2222-2222-2222-222222222222', 'urn:ngsi-ld:Sensor:migration-fixture:sensor-a', 'Sensor', 'migration-fixture'),
                   ('33333333-3333-3333-3333-333333333333', 'urn:ngsi-ld:Sensor:migration-fixture:sensor-b', 'Sensor', 'migration-fixture');
            INSERT INTO twin_states(entity_id)
            VALUES ('22222222-2222-2222-2222-222222222222'),
                   ('33333333-3333-3333-3333-333333333333');
            UPDATE twin_states SET desired_state='{"sample_interval_us":100000}',
                desired_version=1 WHERE entity_id='22222222-2222-2222-2222-222222222222';
            UPDATE twin_states SET desired_state='{"sample_interval_us":200000}',
                desired_version=2, reported_version=1
                WHERE entity_id='33333333-3333-3333-3333-333333333333';""")
        reported_configuration = {
            'sample_interval_us': 200000, 'desired_version': '2',
            'applied_at': wire_time(completed_at),
        }
        reported_state = json.dumps({'configuration': reported_configuration}, separators=(',', ':'))
        self.sql("UPDATE twin_states SET reported_state=" + sql_literal(reported_state) +
                 "::jsonb WHERE entity_id='33333333-3333-3333-3333-333333333333';")
        # Full wire-compatible command and stable serialized outbox bytes.
        self.sql("""INSERT INTO twin_commands(command_id, entity_id, action, parameters, status, desired_version, idempotency_key, issued_by, issued_at, expires_at, control_version, requested_frequency_hz)
            VALUES ('00000000-0000-0000-0000-000000000001', '22222222-2222-2222-2222-222222222222', 'update_configuration', '{"sample_interval_us": 100000}'::jsonb, 'pending', 1, 'key-1', '11111111-1111-1111-1111-111111111111', """ + pending_issued_sql + """, """ + pending_expiry_sql + """, 1, '10');
            INSERT INTO twin_outbox(command_id, topic, payload, payload_bytes, status, next_attempt_at)
            VALUES ('00000000-0000-0000-0000-000000000001', 'gateways/migration-fixture/commands/00000000-0000-0000-0000-000000000001', """ + pending_payload_sql + """::jsonb, """ + pending_payload_sql + """::bytea, 'pending', """ + pending_issued_sql + """);
            INSERT INTO twin_command_requests(actor_id,entity_id,idempotency_key,fingerprint,status_code,response_bytes,command_id,created_at)
            VALUES ('11111111-1111-1111-1111-111111111111','22222222-2222-2222-2222-222222222222','key-1',
                    """ + fingerprint_sql + """,202,""" + receipt_sql + """::bytea,
                    '00000000-0000-0000-0000-000000000001',""" + pending_issued_sql + """);
            INSERT INTO twin_commands(command_id, entity_id, action, parameters, status, desired_version, idempotency_key, issued_by, issued_at, expires_at, completed_at, control_version, requested_frequency_hz, result_bytes, result_hash, result_received_at)
            VALUES ('00000000-0000-0000-0000-000000000002', '33333333-3333-3333-3333-333333333333', 'update_configuration', '{"sample_interval_us": 200000}'::jsonb, 'succeeded', 2, 'key-2', '11111111-1111-1111-1111-111111111111', """ + completed_issued_sql + """, """ + completed_expiry_sql + """, """ + completed_at_sql + """, 1, '5', """ + result_payload_sql + """::bytea, encode(sha256(""" + result_payload_sql + """::bytea),'hex'), """ + completed_at_sql + """);
            INSERT INTO twin_outbox(command_id, topic, payload, payload_bytes, status)
            VALUES ('00000000-0000-0000-0000-000000000002', 'gateways/migration-fixture/commands/00000000-0000-0000-0000-000000000002', """ + completed_payload_sql + """::jsonb, """ + completed_payload_sql + """::bytea, 'published');""")
        # 5. Pre-existing legacy cancelled command without cancellation evidence (permitted by schema 8)
        self.sql("""INSERT INTO twin_commands(command_id, entity_id, action, parameters, status, desired_version, idempotency_key, issued_by, issued_at, expires_at, completed_at, control_version, requested_frequency_hz)
            VALUES ('00000000-0000-0000-0000-000000000003', '33333333-3333-3333-3333-333333333333', 'update_configuration', '{"sample_interval_us": 500000}'::jsonb, 'cancelled', 1, 'key-3', '11111111-1111-1111-1111-111111111111', now() - interval '20s', now() - interval '10s', now() - interval '10s', 1, '2');""")

        self.expect("SELECT count(*) FROM twin_commands WHERE command_id IN ('00000000-0000-0000-0000-000000000001','00000000-0000-0000-0000-000000000002','00000000-0000-0000-0000-000000000003');", '3')
        self.expect("SELECT count(*) FROM twin_command_requests WHERE status_code=202 AND command_id='00000000-0000-0000-0000-000000000001';", '1')
        self.expect("SELECT count(*) FROM twin_states WHERE desired_version=2 AND reported_version=1 AND reported_state->'configuration'->>'sample_interval_us'='200000' AND reported_state->'configuration'->>'desired_version'='2';", '1')
        self.expect("SELECT count(*) FROM sensors WHERE gateway_id='migration-fixture' AND sensor_id IN ('sensor-a','sensor-b');", '2')
        self.expect("SELECT count(*) FROM schema_migrations WHERE version=26 AND name='drop_telemetry_redundant_indexes';", '1')
        self.expect("SELECT count(*) FROM schema_migrations WHERE version=27;", '0')
        self.expect("SELECT count(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='twin_commands' AND column_name='stage6_managed';", '0')
        self.expect("""SELECT count(*) FROM twin_outbox o JOIN twin_commands c USING(command_id)
            WHERE c.command_id='00000000-0000-0000-0000-000000000001'
              AND o.topic='gateways/migration-fixture/commands/00000000-0000-0000-0000-000000000001'
              AND convert_from(o.payload_bytes,'UTF8')::jsonb=o.payload
              AND convert_from(o.payload_bytes,'UTF8')::jsonb->>'protocol_version'='1'
              AND convert_from(o.payload_bytes,'UTF8')::jsonb->>'message_type'='command'
              AND convert_from(o.payload_bytes,'UTF8')::jsonb->>'command_id'=c.command_id::text
              AND convert_from(o.payload_bytes,'UTF8')::jsonb->>'gateway_id'='migration-fixture'
              AND convert_from(o.payload_bytes,'UTF8')::jsonb->>'entity_id'='urn:ngsi-ld:Sensor:migration-fixture:sensor-a'
              AND convert_from(o.payload_bytes,'UTF8')::jsonb->>'action'='update_configuration'
              AND convert_from(o.payload_bytes,'UTF8')::jsonb->>'desired_version'=c.desired_version::text
              AND (convert_from(o.payload_bytes,'UTF8')::jsonb->>'issued_at')::timestamptz=c.issued_at
              AND (convert_from(o.payload_bytes,'UTF8')::jsonb->>'expires_at')::timestamptz=c.expires_at
              AND convert_from(o.payload_bytes,'UTF8')::jsonb->'parameters'->>'sample_interval_us'='100000'
              AND c.control_version=1 AND c.status='pending' AND c.expires_at>clock_timestamp();""", '1')
        self.expect("""SELECT count(*) FROM twin_commands c JOIN twin_outbox o USING(command_id)
            WHERE c.command_id='00000000-0000-0000-0000-000000000002' AND c.status='succeeded'
              AND convert_from(o.payload_bytes,'UTF8')::jsonb=o.payload
              AND convert_from(c.result_bytes,'UTF8')::jsonb->>'protocol_version'='1'
              AND convert_from(c.result_bytes,'UTF8')::jsonb->>'message_type'='command_result'
              AND convert_from(c.result_bytes,'UTF8')::jsonb->>'command_id'=c.command_id::text
              AND convert_from(c.result_bytes,'UTF8')::jsonb->>'gateway_id'='migration-fixture'
              AND convert_from(c.result_bytes,'UTF8')::jsonb->>'entity_id'='urn:ngsi-ld:Sensor:migration-fixture:sensor-b'
              AND convert_from(c.result_bytes,'UTF8')::jsonb->>'desired_version'=c.desired_version::text
              AND convert_from(c.result_bytes,'UTF8')::jsonb->>'status'='succeeded'
              AND convert_from(c.result_bytes,'UTF8')::jsonb->'reported_state'->>'sample_interval_us'='200000'
              AND convert_from(c.result_bytes,'UTF8')::jsonb->>'completed_at'=to_char(c.completed_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')
              AND c.result_hash=encode(sha256(c.result_bytes),'hex')
              AND o.topic='gateways/migration-fixture/commands/'||c.command_id::text
              AND convert_from(o.payload_bytes,'UTF8')::jsonb->>'protocol_version'='1'
              AND convert_from(o.payload_bytes,'UTF8')::jsonb->>'message_type'='command'
              AND convert_from(o.payload_bytes,'UTF8')::jsonb->>'command_id'=c.command_id::text
              AND convert_from(o.payload_bytes,'UTF8')::jsonb->>'gateway_id'='migration-fixture'
              AND convert_from(o.payload_bytes,'UTF8')::jsonb->>'entity_id'='urn:ngsi-ld:Sensor:migration-fixture:sensor-b'
              AND convert_from(o.payload_bytes,'UTF8')::jsonb->>'action'='update_configuration'
              AND convert_from(o.payload_bytes,'UTF8')::jsonb->>'desired_version'=c.desired_version::text
              AND (convert_from(o.payload_bytes,'UTF8')::jsonb->>'issued_at')::timestamptz=c.issued_at
              AND (convert_from(o.payload_bytes,'UTF8')::jsonb->>'expires_at')::timestamptz=c.expires_at
              AND convert_from(o.payload_bytes,'UTF8')::jsonb->'parameters'->>'sample_interval_us'='200000';""", '1')
        self.expect("SELECT count(*) FROM twin_command_requests WHERE actor_id='11111111-1111-1111-1111-111111111111' AND entity_id='22222222-2222-2222-2222-222222222222' AND idempotency_key='key-1' AND status_code=202 AND command_id='00000000-0000-0000-0000-000000000001' AND fingerprint='" + fingerprint + "' AND convert_from(response_bytes,'UTF8')::jsonb=" + sql_literal(receipt_bytes) + "::jsonb;", '1')
        self.expect("SELECT count(*) FROM twin_states WHERE entity_id='22222222-2222-2222-2222-222222222222' AND desired_version=1 AND desired_state->>'sample_interval_us'='100000';", '1')
        self.expect("SELECT count(*) FROM twin_states WHERE entity_id='33333333-3333-3333-3333-333333333333' AND desired_version=2 AND reported_version=1 AND desired_state->>'sample_interval_us'='200000' AND reported_state->'configuration'->>'sample_interval_us'='200000' AND reported_state->'configuration'->>'desired_version'='2';", '1')
        print('Stage 5 command/result wire envelopes and Sensor correlation checks passed.', flush=True)
        preservation_query = """SELECT jsonb_build_object(
            'live', (SELECT jsonb_build_object('command_id',c.command_id,'status',c.status,'desired_version',c.desired_version,
                'attempt_count',c.attempt_count,'issued_at',c.issued_at,'expires_at',c.expires_at,'control_version',c.control_version,
                'requested_frequency_hz',c.requested_frequency_hz,'action',c.action,'parameters',c.parameters,
                'topic',o.topic,'outbox_status',o.status,'outbox_bytes_hex',encode(o.payload_bytes,'hex'))
                FROM twin_commands c JOIN twin_outbox o USING(command_id) WHERE c.command_id='00000000-0000-0000-0000-000000000001'),
            'request', (SELECT jsonb_build_object('fingerprint',fingerprint,'status_code',status_code,
                'response_bytes_hex',encode(response_bytes,'hex'),'command_id',command_id,'created_at',created_at)
                FROM twin_command_requests WHERE idempotency_key='key-1'),
            'completed', (SELECT jsonb_build_object('command_id',c.command_id,'status',c.status,'desired_version',c.desired_version,
                'attempt_count',c.attempt_count,'issued_at',c.issued_at,'expires_at',c.expires_at,'completed_at',c.completed_at,
                'control_version',c.control_version,'requested_frequency_hz',c.requested_frequency_hz,'action',c.action,
                'parameters',c.parameters,'topic',o.topic,'outbox_status',o.status,'outbox_bytes_hex',encode(o.payload_bytes,'hex'),
                'result_bytes_hex',encode(c.result_bytes,'hex'),'result_hash',c.result_hash,'result_received_at',c.result_received_at)
                FROM twin_commands c JOIN twin_outbox o USING(command_id) WHERE c.command_id='00000000-0000-0000-0000-000000000002'),
            'state_live', (SELECT jsonb_build_object('desired_state',desired_state,'desired_version',desired_version,
                'reported_state',reported_state,'reported_version',reported_version,'last_desired_at',last_desired_at,'last_reported_at',last_reported_at)
                FROM twin_states WHERE entity_id='22222222-2222-2222-2222-222222222222'),
            'state_completed', (SELECT jsonb_build_object('desired_state',desired_state,'desired_version',desired_version,
                'reported_state',reported_state,'reported_version',reported_version,'last_desired_at',last_desired_at,'last_reported_at',last_reported_at)
                FROM twin_states WHERE entity_id='33333333-3333-3333-3333-333333333333'))::text"""
        pre_upgrade_snapshot = self.sql(preservation_query)
        if not pre_upgrade_snapshot:
            raise Failure('Pre-upgrade Stage 5 oracle was empty.')
        print('Schema 26 seeded nonempty Stage 5 baseline established: live command, request receipt, completed result and confirmed configuration.', flush=True)
        self.expect("SELECT (expires_at > clock_timestamp())::text FROM twin_commands WHERE command_id='00000000-0000-0000-0000-000000000001';", 'true')

        migration27 = (ROOT / 'migrations/000027_stage6_control_resilience.up.sql').read_text()
        self.sql(migration27)
        self.expect("SELECT count(*) FROM schema_migrations WHERE version=27 AND name='stage6_control_resilience';", '1')
        self.expect("SELECT count(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='twin_commands' AND column_name='stage6_managed';", '1')
        self.expect("SELECT count(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='twin_states' AND column_name='next_query_at';", '1')
        if self.sql(preservation_query) != pre_upgrade_snapshot:
            raise Failure('First migration 27 application changed Stage 5 data oracle.')
        print('Migration 27 FIRST data-bearing application preserved Stage 5 oracle byte-for-byte.', flush=True)
        self.sql(migration27)
        self.expect("SELECT count(*) FROM schema_migrations WHERE version=27 AND name='stage6_control_resilience';", '1')
        if self.sql(preservation_query) != pre_upgrade_snapshot:
            raise Failure('Migration 27 reapply changed Stage 5 data oracle.')
        print('Migration 27 separate idempotent reapply preserved the same oracle.', flush=True)

        # 1. Verify legacy data survival and UNKNOWN history (byte-exact equality):
        self.expect("SELECT count(*) FROM twin_commands WHERE stage6_managed = false;", '3')
        self.expect("SELECT encode(o.payload_bytes,'hex') FROM twin_outbox o WHERE command_id='00000000-0000-0000-0000-000000000001';",
                    pending_bytes.encode().hex())
        self.expect("SELECT encode(result_bytes,'hex') FROM twin_commands WHERE command_id='00000000-0000-0000-0000-000000000002';",
                    result_bytes.encode().hex())
        self.expect("SELECT status || '|' || (cancelled_at IS NULL)::text || '|' || stage6_managed::text FROM twin_commands WHERE command_id='00000000-0000-0000-0000-000000000003';", 'cancelled|true|false')

        # 2. Verify fresh Stage 5-style insert without Stage 6 columns:
        self.sql("""INSERT INTO twin_commands(command_id, entity_id, action, parameters, status, desired_version, idempotency_key, issued_by, issued_at, expires_at, control_version, requested_frequency_hz)
            VALUES ('00000000-0000-0000-0000-000000000004', '33333333-3333-3333-3333-333333333333', 'update_configuration', '{"sample_interval_us": 300000}'::jsonb, 'pending', 3, 'key-4', '11111111-1111-1111-1111-111111111111', now(), now() + interval '60s', 1, '3.33');""")
        self.expect("SELECT stage6_managed::text || '|' || origin FROM twin_commands WHERE command_id='00000000-0000-0000-0000-000000000004';", 'false|client')

        # 3. Verify active sensor unique constraint (sensor-a already has command 1 pending):
        try:
            self.sql("""INSERT INTO twin_commands(command_id, entity_id, action, parameters, status, desired_version, idempotency_key, issued_by, issued_at, expires_at, control_version, requested_frequency_hz)
                VALUES ('00000000-0000-0000-0000-00000000000a', '22222222-2222-2222-2222-222222222222', 'update_configuration', '{"sample_interval_us": 100000}'::jsonb, 'pending', 2, 'key-a', '11111111-1111-1111-1111-111111111111', now(), now() + interval '60s', 1, '10');""")
            raise Failure('Duplicate active sensor command was not refused.')
        except Failure as error:
            if 'uq_twin_commands_active_sensor' not in str(error):
                raise

        # 4. Verify receipt evidence constraints and write-once immutability:
        self.sql("""INSERT INTO sensors(gateway_id, sensor_id, name) VALUES ('migration-fixture', 'sensor-c', 'sensor c');
            INSERT INTO twin_entities(id, entity_id, entity_type, gateway_id) VALUES ('44444444-4444-4444-4444-444444444444', 'urn:ngsi-ld:Sensor:migration-fixture:sensor-c', 'Sensor', 'migration-fixture');
            INSERT INTO twin_states(entity_id) VALUES ('44444444-4444-4444-4444-444444444444');
            INSERT INTO twin_commands(command_id, entity_id, action, parameters, status, desired_version, idempotency_key, issued_by, issued_at, expires_at, control_version, requested_frequency_hz, stage6_managed, origin, root_command_id)
            VALUES ('00000000-0000-0000-0000-000000000005', '44444444-4444-4444-4444-444444444444', 'update_configuration', '{"sample_interval_us": 100000}'::jsonb, 'pending', 1, 'key-5', '11111111-1111-1111-1111-111111111111', now(), now() + interval '60s', 1, '10', true, 'client', '00000000-0000-0000-0000-000000000005');""")
        # Partial receipt rejected:
        try:
            self.sql("UPDATE twin_commands SET receipt_bytes = 'test'::bytea WHERE command_id='00000000-0000-0000-0000-000000000005';")
            raise Failure('Partial receipt was not refused.')
        except Failure as error:
            if 'chk_twin_commands_receipt' not in str(error):
                raise
        # Hash mismatch rejected:
        try:
            self.sql("UPDATE twin_commands SET receipt_bytes = 'test'::bytea, receipt_hash = 'badhash', receipt_received_at = now() WHERE command_id='00000000-0000-0000-0000-000000000005';")
            raise Failure('Receipt hash mismatch was not refused.')
        except Failure as error:
            if 'chk_twin_commands_receipt' not in str(error):
                raise
        # Oversize receipt (>8192 bytes) rejected:
        try:
            self.sql("UPDATE twin_commands SET receipt_bytes = repeat('a', 8193)::bytea, receipt_hash = encode(sha256(repeat('a', 8193)::bytea),'hex'), receipt_received_at = now() WHERE command_id='00000000-0000-0000-0000-000000000005';")
            raise Failure('Oversize receipt (>8192) was not refused.')
        except Failure as error:
            if 'chk_twin_commands_receipt' not in str(error):
                raise
        # Valid receipt accepted:
        self.sql("UPDATE twin_commands SET receipt_bytes = 'test'::bytea, receipt_hash = encode(sha256('test'::bytea),'hex'), receipt_received_at = now(), status = 'acknowledged' WHERE command_id='00000000-0000-0000-0000-000000000005';")
        # Receipt immutability enforced:
        try:
            self.sql("UPDATE twin_commands SET receipt_bytes = 'new'::bytea WHERE command_id='00000000-0000-0000-0000-000000000005';")
            raise Failure('Receipt mutation was not refused.')
        except Failure as error:
            if 'canonical command receipt evidence is immutable' not in str(error).lower():
                raise
        # Complete command 5 so sensor-c active slot is released:
        self.sql("UPDATE twin_commands SET status = 'succeeded', completed_at = now() WHERE command_id='00000000-0000-0000-0000-000000000005';")

        # 5. Verify cancellation evidence, immutability, and user deletion:
        self.sql("INSERT INTO auth.users(id) VALUES ('55555555-5555-5555-5555-555555555555');")
        self.sql("""INSERT INTO twin_commands(command_id, entity_id, action, parameters, status, desired_version, idempotency_key, issued_by, issued_at, expires_at, completed_at, control_version, requested_frequency_hz, stage6_managed, cancelled_at, cancelled_by, origin, root_command_id)
            VALUES ('00000000-0000-0000-0000-000000000006', '44444444-4444-4444-4444-444444444444', 'update_configuration', '{"sample_interval_us": 100000}'::jsonb, 'cancelled', 2, 'key-6', '11111111-1111-1111-1111-111111111111', now(), now() + interval '60s', now(), 1, '10', true, now(), '55555555-5555-5555-5555-555555555555', 'client', '00000000-0000-0000-0000-000000000006');""")
        # Mutating cancelled_at rejected:
        try:
            self.sql("UPDATE twin_commands SET cancelled_at = now() - interval '1s' WHERE command_id='00000000-0000-0000-0000-000000000006';")
            raise Failure('Cancellation mutation was not refused.')
        except Failure as error:
            if 'cancellation evidence is immutable' not in str(error).lower():
                raise
        # Deleting actor profile does NOT fail or violate trigger because cancelled_by is an immutable audit snapshot:
        self.sql("DELETE FROM auth.users WHERE id = '55555555-5555-5555-5555-555555555555';")
        self.expect("SELECT cancelled_by::text FROM twin_commands WHERE command_id='00000000-0000-0000-0000-000000000006';", '55555555-5555-5555-5555-555555555555')

        # 6. Verify first reservation barrier immutability:
        self.sql("""INSERT INTO twin_commands(command_id, entity_id, action, parameters, status, desired_version, idempotency_key, issued_by, issued_at, expires_at, control_version, requested_frequency_hz, stage6_managed, first_reserved_at, attempt_count, origin, root_command_id)
            VALUES ('00000000-0000-0000-0000-000000000007', '44444444-4444-4444-4444-444444444444', 'update_configuration', '{"sample_interval_us": 100000}'::jsonb, 'pending', 3, 'key-7', '11111111-1111-1111-1111-111111111111', now(), now() + interval '60s', 1, '10', true, now(), 1, 'client', '00000000-0000-0000-0000-000000000007');""")
        try:
            self.sql("UPDATE twin_commands SET first_reserved_at = NULL WHERE command_id='00000000-0000-0000-0000-000000000007';")
            raise Failure('Clearing first reservation was not refused.')
        except Failure as error:
            if 'first reservation timestamp is immutable once written' not in str(error).lower():
                raise

        # 7. Verify lineage, budget bound 0..1, and deletion restriction:
        try:
            self.sql("UPDATE twin_commands SET auto_successors_used = 2 WHERE command_id='00000000-0000-0000-0000-000000000007';")
            raise Failure('Budget > 1 was not refused.')
        except Failure as error:
            if 'chk_twin_commands_auto_successors' not in str(error):
                raise
        self.sql("UPDATE twin_commands SET auto_successors_used = 1, status = 'timeout' WHERE command_id='00000000-0000-0000-0000-000000000007';")
        # Successor without parent rejected:
        try:
            self.sql("""INSERT INTO twin_commands(command_id, entity_id, action, parameters, status, desired_version, idempotency_key, issued_by, issued_at, expires_at, control_version, requested_frequency_hz, stage6_managed, origin, root_command_id, parent_command_id, auto_successors_used)
                VALUES ('00000000-0000-0000-0000-000000000008', '44444444-4444-4444-4444-444444444444', 'update_configuration', '{"sample_interval_us": 100000}'::jsonb, 'pending', 4, 'key-8', '11111111-1111-1111-1111-111111111111', now(), now() + interval '60s', 1, '10', true, 'reconciliation', '00000000-0000-0000-0000-000000000007', NULL, 0);""")
            raise Failure('Successor without parent was not refused.')
        except Failure as error:
            if 'chk_twin_commands_lineage' not in str(error):
                raise
        # Valid successor accepted:
        self.sql("""INSERT INTO twin_commands(command_id, entity_id, action, parameters, status, desired_version, idempotency_key, issued_by, issued_at, expires_at, control_version, requested_frequency_hz, stage6_managed, origin, root_command_id, parent_command_id, auto_successors_used)
            VALUES ('00000000-0000-0000-0000-000000000008', '44444444-4444-4444-4444-444444444444', 'update_configuration', '{"sample_interval_us": 100000}'::jsonb, 'pending', 4, 'key-8', '11111111-1111-1111-1111-111111111111', now(), now() + interval '60s', 1, '10', true, 'reconciliation', '00000000-0000-0000-0000-000000000007', '00000000-0000-0000-0000-000000000007', 0);""")
        # Deleting root command rejected by RESTRICT constraint:
        try:
            self.sql("DELETE FROM twin_commands WHERE command_id='00000000-0000-0000-0000-000000000007';")
            raise Failure('Deleting root command with active successor was not refused.')
        except Failure as error:
            if 'fk_twin_commands_root' not in str(error):
                raise

        # 8. Privileges & RLS:
        # Verify anon denied INSERT:
        try:
            self.sql("SET ROLE anon; INSERT INTO public.twin_commands(command_id, entity_id, action, parameters, status, desired_version, expires_at, target_entity_type) VALUES ('00000000-0000-0000-0000-000000000099', '22222222-2222-2222-2222-222222222222', 'update_configuration', '{}', 'pending', 1, now(), 'Sensor');")
            raise Failure('Anon INSERT was not refused.')
        except Failure as error:
            if 'permission denied' not in str(error).lower():
                raise
        # Verify authenticated denied UPDATE:
        try:
            self.sql("SET ROLE authenticated; UPDATE public.twin_commands SET status='failed';")
            raise Failure('Authenticated UPDATE was not refused.')
        except Failure as error:
            if 'permission denied' not in str(error).lower():
                raise
        # Reset role back to admin for subsequent operations:
        self.sql("RESET ROLE;")
        # Verify iot_backend_app can perform legitimate UPDATE using real login:
        self.sql("UPDATE public.twin_commands SET retry_stop_reason = 'result_received' WHERE command_id = '00000000-0000-0000-0000-000000000004';",
                 role='iot_backend_app', password=self.passwords['BACKEND_DB_PASSWORD'])
        self.expect("SELECT retry_stop_reason FROM twin_commands WHERE command_id = '00000000-0000-0000-0000-000000000004';", 'result_received')
        print('Migration 27 native control resilience schema, legacy preservation, constraints and triggers passed.', flush=True)

    def verify_retention(self):
        entrypoint = ROOT / 'scripts/sql/stage6-command-retention.sql'
        if not entrypoint.is_file():
            raise Failure('Stage 6 retention maintenance entrypoint is missing.')
        dry_run = self.retention_sql()
        if 'DRY-RUN:' not in dry_run:
            raise Failure('Default retention mode did not execute a dry-run.')
        for label, kwargs in (
            ('invalid mode', {'mode': 'delete'}),
            ('mismatched target', {'mode': 'dry-run', 'target': 'wrong_target'}),
            ('apply without confirmation', {'mode': 'apply'}),
            ('mismatched apply confirmation', {'mode': 'apply', 'confirm': 'wrong_target'}),
        ):
            try:
                self.retention_sql(**kwargs)
                raise Failure('Retention accepted invalid guard: ' + label)
            except Failure as error:
                if 'Fixture command failed.' not in str(error):
                    raise
        print('Retention default dry-run and invalid target/mode guards passed.', flush=True)
        self.expect("""SELECT bool_and(NOT has_table_privilege(role_name, relation_name, 'DELETE'))
            FROM (VALUES ('iot_backend_app'),('anon'),('authenticated')) roles(role_name)
            CROSS JOIN (VALUES ('public.twin_commands'),('public.twin_command_requests'),
                               ('public.twin_outbox')) relations(relation_name);""", 't')
        self.sql("""INSERT INTO gateways(gateway_id,name) VALUES ('retention-fixture','retention fixture');
            INSERT INTO sensors(gateway_id,sensor_id,name)
            SELECT 'retention-fixture','r'||lpad(i::text,3,'0'),'retention sensor '||i
            FROM generate_series(1,102) AS i;
            INSERT INTO twin_entities(id,entity_id,entity_type,gateway_id)
            SELECT md5('ret-entity-'||i)::uuid,
                   'urn:ngsi-ld:Sensor:retention-fixture:r'||lpad(i::text,3,'0'),
                   'Sensor','retention-fixture'
            FROM generate_series(1,102) AS i;
            INSERT INTO twin_states(entity_id,desired_state,reported_state,desired_version,reported_version)
            SELECT md5('ret-entity-'||i)::uuid,
                   '{"sample_interval_us":200000}'::jsonb,
                   '{"configuration":{"sample_interval_us":100000}}'::jsonb,
                   CASE WHEN i=1 THEN 3 ELSE 2 END,1
            FROM generate_series(1,102) AS i;""")
        self.sql("""WITH clock AS (SELECT statement_timestamp() AS captured),
            roots AS (
                SELECT i, md5('ret-root-'||i)::uuid AS command_id,
                       md5('ret-entity-'||i)::uuid AS entity_id,
                       'urn:ngsi-ld:Sensor:retention-fixture:r'||lpad(i::text,3,'0') AS urn,
                        CASE WHEN i=1 THEN 35 ELSE 40 END AS age_days,
                       CASE WHEN i=1 THEN 3 ELSE 2 END AS current_version
                FROM generate_series(1,101) AS i
            ),
            timed AS (
                SELECT roots.*, clock.captured - make_interval(days=>age_days) AS issued_at
                FROM roots CROSS JOIN clock
            )
            INSERT INTO twin_commands(command_id,entity_id,action,parameters,status,desired_version,
                idempotency_key,issued_by,issued_at,expires_at,acknowledged_at,completed_at,attempt_count,
                control_version,requested_frequency_hz,result_bytes,result_hash,result_received_at,
                stage6_managed,first_reserved_at,retry_stop_reason,origin,root_command_id,
                original_actor_id,auto_successors_used,reconcile_blocked,error_code,error_message)
            SELECT command_id,entity_id,'update_configuration','{"sample_interval_us":100000}'::jsonb,
                CASE WHEN i IN (1,2,3) THEN 'timeout' ELSE 'succeeded' END,
                1,'ret-root-'||i,'11111111-1111-1111-1111-111111111111',
                issued_at,issued_at+interval '1 minute',
                CASE WHEN i IN (1,2) THEN issued_at+interval '20 seconds' END,
                CASE WHEN i IN (1,2,3) THEN issued_at+interval '90 seconds'
                     ELSE issued_at+interval '30 seconds' END,
                1,1,CASE WHEN i=2 THEN '1e1' ELSE '10' END,
                CASE WHEN i IN (1,2,3) THEN NULL ELSE
                convert_to(jsonb_build_object('protocol_version',1,'message_type','command_result',
                    'command_id',command_id::text,'gateway_id','retention-fixture','entity_id',urn,
                    'desired_version','1','status','succeeded',
                    'reported_state',jsonb_build_object('sample_interval_us',100000),
                    'completed_at',to_char((issued_at+interval '30 seconds') AT TIME ZONE 'UTC',
                                            'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'))::text,'UTF8') END,
                CASE WHEN i IN (1,2,3) THEN NULL ELSE
                encode(sha256(convert_to(jsonb_build_object('protocol_version',1,'message_type','command_result',
                    'command_id',command_id::text,'gateway_id','retention-fixture','entity_id',urn,
                    'desired_version','1','status','succeeded',
                    'reported_state',jsonb_build_object('sample_interval_us',100000),
                    'completed_at',to_char((issued_at+interval '30 seconds') AT TIME ZONE 'UTC',
                                            'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'))::text,'UTF8')),'hex') END,
                CASE WHEN i IN (1,2,3) THEN NULL ELSE issued_at+interval '30 seconds' END,
                true,issued_at,
                CASE WHEN i=1 OR i=2 THEN 'acknowledged'
                     WHEN i=3 THEN 'retry_exhausted' ELSE 'result_received' END,
                'client',command_id,
                '11111111-1111-1111-1111-111111111111',
                CASE WHEN i=1 THEN 1 ELSE 0 END, i=1,
                CASE WHEN i IN (1,2,3) THEN 'COMMAND_EXPIRED' END,
                CASE WHEN i IN (1,2,3) THEN 'command expired' END
            FROM timed;""")
        self.sql("""WITH root AS (
            SELECT c.*,e.entity_id AS urn,e.gateway_id
            FROM twin_commands c JOIN twin_entities e ON e.id=c.entity_id
            WHERE c.command_id=md5('ret-root-1')::uuid
        ),
        payload AS (
            SELECT root.*,
                jsonb_build_object('protocol_version',1,'message_type','command',
                    'command_id',command_id::text,'gateway_id',gateway_id,'entity_id',urn,
                    'action',action,'parameters',parameters,'desired_version','1',
                    'issued_at',issued_at,'expires_at',expires_at) AS body
            FROM root
        )
        INSERT INTO twin_outbox(command_id,topic,payload,payload_bytes,status,next_attempt_at,created_at,published_at)
        SELECT command_id,'gateways/'||gateway_id||'/commands/'||command_id::text,body,
               convert_to(body::text,'UTF8'),
               CASE WHEN command_id IN (md5('ret-root-1')::uuid,md5('ret-root-2')::uuid,
                                        md5('ret-root-3')::uuid) THEN 'failed' ELSE 'published' END,
               issued_at,issued_at,completed_at FROM payload;
        WITH root AS (
            SELECT c.*,e.entity_id AS urn,e.gateway_id
            FROM twin_commands c JOIN twin_entities e ON e.id=c.entity_id
            WHERE c.command_id<>md5('ret-root-1')::uuid
              AND c.idempotency_key LIKE 'ret-root-%'
        ),
        payload AS (
            SELECT root.*,
                jsonb_build_object('protocol_version',1,'message_type','command',
                    'command_id',command_id::text,'gateway_id',gateway_id,'entity_id',urn,
                    'action',action,'parameters',parameters,'desired_version','1',
                    'issued_at',issued_at,'expires_at',expires_at) AS body
            FROM root
        )
         INSERT INTO twin_outbox(command_id,topic,payload,payload_bytes,status,next_attempt_at,created_at,published_at)
         SELECT command_id,'gateways/'||gateway_id||'/commands/'||command_id::text,body,
                convert_to(body::text,'UTF8'),
                CASE WHEN command_id IN (md5('ret-root-2')::uuid,md5('ret-root-3')::uuid)
                     THEN 'failed' ELSE 'published' END,
                issued_at,issued_at,completed_at FROM payload;""")
        self.sql("""UPDATE twin_outbox outbox SET attempt_count=command.attempt_count
            FROM twin_commands command WHERE command.command_id=outbox.command_id
              AND command.idempotency_key LIKE 'ret-root-%';
            WITH ack AS (
                SELECT c.command_id,c.entity_id,c.desired_version,c.issued_at,c.expires_at,
                       c.acknowledged_at,e.entity_id AS urn,e.gateway_id,o.payload_bytes
                FROM twin_commands c
                JOIN twin_entities e ON e.id=c.entity_id
                JOIN twin_outbox o USING(command_id)
                WHERE c.command_id IN (md5('ret-root-1')::uuid,md5('ret-root-2')::uuid)
            ),
            receipts AS (
                SELECT ack.*,
                    convert_to(jsonb_build_object('protocol_version',1,'message_type','command_receipt',
                        'command_id',command_id::text,'gateway_id',gateway_id,'entity_id',urn,
                        'desired_version',desired_version::text,'received_at',acknowledged_at,
                        'command_hash',encode(sha256(payload_bytes),'hex'))::text,'UTF8') AS bytes
                FROM ack
            )
            UPDATE twin_commands command SET receipt_bytes=receipts.bytes,
                receipt_hash=encode(sha256(receipts.bytes),'hex'),
                receipt_received_at=receipts.acknowledged_at
            FROM receipts WHERE command.command_id=receipts.command_id;""")
        self.sql("""WITH root AS (
                SELECT c.*,e.entity_id AS urn,e.gateway_id
                FROM twin_commands c JOIN twin_entities e ON e.id=c.entity_id
                WHERE c.command_id=md5('ret-root-1')::uuid
            ),
            late_result AS (
                SELECT root.*,
                    jsonb_build_object('protocol_version',1,'message_type','command_result',
                        'command_id',command_id::text,'gateway_id',gateway_id,'entity_id',urn,
                        'desired_version','1','status','succeeded',
                        'reported_state',jsonb_build_object('sample_interval_us',100000),
                        'completed_at',to_char((issued_at+interval '30 seconds') AT TIME ZONE 'UTC',
                                               'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')) AS body
                FROM root
            )
            UPDATE twin_commands c SET
                late_result_bytes=convert_to(late_result.body::text,'UTF8'),
                late_result_hash=encode(sha256(convert_to(late_result.body::text,'UTF8')),'hex'),
                late_result_received_at=late_result.issued_at+interval '5 minutes'
            FROM late_result WHERE c.command_id=late_result.command_id;
            UPDATE twin_outbox SET status='failed'
            WHERE command_id=md5('ret-child-1')::uuid;""")
        self.sql("""WITH roots AS (
            SELECT c.command_id,c.entity_id,c.issued_by,c.idempotency_key,c.issued_at,c.expires_at,
                   e.entity_id AS urn,e.gateway_id
            FROM twin_commands c JOIN twin_entities e ON e.id=c.entity_id
            WHERE c.idempotency_key LIKE 'ret-root-%'
        )
        INSERT INTO twin_command_requests(actor_id,entity_id,idempotency_key,fingerprint,
            status_code,response_bytes,command_id,created_at)
        SELECT issued_by,entity_id,idempotency_key,
               encode(sha256(convert_to('["update_configuration","10","0"]','UTF8')),'hex'),202,
                convert_to(jsonb_build_object('entity_id',urn,'command_id',command_id::text,
                     'status','pending','requested_frequency_hz',10,'effective_frequency_hz',10,
                     'sample_interval_us',100000,'desired_version','1','expires_at',expires_at,
                     'status_url','/v1/digital-twins/'||urn||'/commands/'||command_id::text)::text,'UTF8'),
               command_id,issued_at FROM roots;
        INSERT INTO twin_command_requests(actor_id,entity_id,idempotency_key,fingerprint,
            status_code,response_bytes,command_id,created_at)
        SELECT '11111111-1111-1111-1111-111111111111',md5('ret-entity-1')::uuid,
            'ret-noop-1',repeat('b',64),200,'{}'::bytea,NULL,
            clock_timestamp()-interval '40 days';""")
        self.sql("""WITH clock AS (SELECT statement_timestamp() AS captured),
            item AS (
                SELECT c.*,e.entity_id AS urn,e.gateway_id,
                        c.issued_at+interval '3 minutes' AS child_issued_at,
                        c.issued_at+interval '4 minutes' AS child_expires_at,
                        c.issued_at+interval '3 minutes 5 seconds' AS child_completed_at
                FROM twin_commands c JOIN twin_entities e ON e.id=c.entity_id
                WHERE c.command_id=md5('ret-root-1')::uuid
            )
            INSERT INTO twin_commands(command_id,entity_id,action,parameters,status,desired_version,
                issued_at,expires_at,cancelled_at,cancelled_by,completed_at,attempt_count,control_version,requested_frequency_hz,
                result_bytes,result_hash,result_received_at,stage6_managed,first_reserved_at,
                retry_stop_reason,origin,root_command_id,parent_command_id,original_actor_id,
                auto_successors_used,reconcile_blocked)
            SELECT md5('ret-child-1')::uuid,entity_id,'update_configuration',
                '{"sample_interval_us":100000}'::jsonb,'cancelled',2,child_issued_at,child_expires_at,
                child_completed_at,'11111111-1111-1111-1111-111111111111',
                child_completed_at,0,1,'10',
                NULL,NULL,NULL,true,NULL,'cancelled','reconciliation',
                command_id,command_id,'11111111-1111-1111-1111-111111111111',0,true FROM item;
            WITH child AS (
                SELECT c.*,e.entity_id AS urn,e.gateway_id
                FROM twin_commands c JOIN twin_entities e ON e.id=c.entity_id
                WHERE c.command_id=md5('ret-child-1')::uuid
            ),
            payload AS (
                SELECT child.*,
                    jsonb_build_object('protocol_version',1,'message_type','command',
                        'command_id',command_id::text,'gateway_id',gateway_id,'entity_id',urn,
                        'action',action,'parameters',parameters,'desired_version','2',
                        'issued_at',issued_at,'expires_at',expires_at) AS body
                FROM child
            )
            INSERT INTO twin_outbox(command_id,topic,payload,payload_bytes,status,next_attempt_at,created_at,published_at)
            SELECT command_id,'gateways/'||gateway_id||'/commands/'||command_id::text,body,
                convert_to(body::text,'UTF8'),'failed',issued_at,issued_at,completed_at FROM payload;""")
        self.sql("""WITH clock AS (SELECT statement_timestamp() AS captured),
            current_intents AS (
                SELECT i,md5('ret-current-'||i)::uuid AS command_id,
                       md5('ret-entity-'||i)::uuid AS entity_id,
                       'urn:ngsi-ld:Sensor:retention-fixture:r'||lpad(i::text,3,'0') AS urn,
                       CASE WHEN i=1 THEN 3 ELSE 2 END AS version,
                       CASE WHEN i=1 THEN clock.captured-interval '5 minutes'
                            ELSE clock.captured END AS issued_at
                FROM generate_series(1,101) AS i CROSS JOIN clock
            )
            INSERT INTO twin_commands(command_id,entity_id,action,parameters,status,desired_version,
                idempotency_key,issued_by,issued_at,expires_at,attempt_count,control_version,
                requested_frequency_hz,stage6_managed,origin,root_command_id,original_actor_id,
                auto_successors_used)
            SELECT command_id,entity_id,'update_configuration','{"sample_interval_us":200000}'::jsonb,
                'pending',version,'ret-current-'||i,'11111111-1111-1111-1111-111111111111',
                issued_at,issued_at+interval '1 hour',0,1,
                CASE WHEN i=101 THEN '5e0' ELSE '5' END,true,'client',command_id,
                '11111111-1111-1111-1111-111111111111',0 FROM current_intents;
            WITH intents AS (
                SELECT c.*,e.entity_id AS urn,e.gateway_id
                FROM twin_commands c JOIN twin_entities e ON e.id=c.entity_id
                WHERE c.idempotency_key LIKE 'ret-current-%'
            ),
            payload AS (
                SELECT intents.*,
                    jsonb_build_object('protocol_version',1,'message_type','command',
                        'command_id',command_id::text,'gateway_id',gateway_id,'entity_id',urn,
                        'action',action,'parameters',parameters,'desired_version',desired_version::text,
                        'issued_at',issued_at,'expires_at',expires_at) AS body
                FROM intents
            )
            INSERT INTO twin_outbox(command_id,topic,payload,payload_bytes,status,next_attempt_at,created_at)
            SELECT command_id,'gateways/'||gateway_id||'/commands/'||command_id::text,body,
                convert_to(body::text,'UTF8'),'pending',issued_at,issued_at FROM payload;
            INSERT INTO twin_command_requests(actor_id,entity_id,idempotency_key,fingerprint,
                status_code,response_bytes,command_id,created_at)
            SELECT c.issued_by,c.entity_id,c.idempotency_key,
                encode(sha256(convert_to(format('["update_configuration","5","%s"]',
                                               c.desired_version-1),'UTF8')),'hex'),202,
                convert_to(jsonb_build_object('entity_id',e.entity_id,'command_id',c.command_id::text,
                    'status','pending','requested_frequency_hz',5,'effective_frequency_hz',5,
                    'sample_interval_us',200000,'desired_version',c.desired_version::text,
                    'expires_at',c.expires_at,
                    'status_url','/v1/digital-twins/'||e.entity_id||'/commands/'||c.command_id::text)::text,'UTF8'),
                c.command_id,c.issued_at
            FROM twin_commands c JOIN twin_entities e ON e.id=c.entity_id
            WHERE c.idempotency_key LIKE 'ret-current-%';""")
        self.sql("""WITH clock AS (SELECT statement_timestamp() AS now_at),
            root_seed AS (
                SELECT md5('ret-root-102')::uuid AS command_id,
                       md5('ret-entity-102')::uuid AS entity_id,
                       'urn:ngsi-ld:Sensor:retention-fixture:r102'::text AS urn,
                       clock.now_at-interval '40 days' AS issued_at,
                       clock.now_at-interval '1 day' AS late_received_at,
                       clock.now_at
                FROM clock
            ),
            late AS (
                SELECT root_seed.*,
                    jsonb_build_object('protocol_version',1,'message_type','command_result',
                        'command_id',command_id::text,'gateway_id','retention-fixture','entity_id',urn,
                        'desired_version','1','status','succeeded',
                        'reported_state',jsonb_build_object('sample_interval_us',100000),
                        'completed_at',to_char((issued_at+interval '30 seconds') AT TIME ZONE 'UTC',
                                               'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')) AS body
                FROM root_seed
            )
            INSERT INTO twin_commands(command_id,entity_id,action,parameters,status,desired_version,
                idempotency_key,issued_by,issued_at,expires_at,acknowledged_at,completed_at,attempt_count,
                control_version,requested_frequency_hz,stage6_managed,first_reserved_at,retry_stop_reason,
                origin,root_command_id,original_actor_id,auto_successors_used,error_code,error_message,
                late_result_bytes,late_result_hash,late_result_received_at)
            SELECT command_id,entity_id,'update_configuration','{"sample_interval_us":100000}'::jsonb,
                'timeout',1,'ret-root-102','11111111-1111-1111-1111-111111111111',
                issued_at,issued_at+interval '1 minute',issued_at+interval '20 seconds',
                issued_at+interval '90 seconds',1,1,'10',true,issued_at,'acknowledged',
                'client',command_id,'11111111-1111-1111-1111-111111111111',0,
                'COMMAND_EXPIRED','command expired',convert_to(body::text,'UTF8'),
                encode(sha256(convert_to(body::text,'UTF8')),'hex'),late_received_at
            FROM late;
            WITH root AS (
                SELECT c.*,e.entity_id AS urn,e.gateway_id
                FROM twin_commands c JOIN twin_entities e ON e.id=c.entity_id
                WHERE c.command_id=md5('ret-root-102')::uuid
            ),
            payload AS (
                SELECT root.*,jsonb_build_object('protocol_version',1,'message_type','command',
                    'command_id',command_id::text,'gateway_id',gateway_id,'entity_id',urn,
                    'action',action,'parameters',parameters,'desired_version',desired_version::text,
                    'issued_at',issued_at,'expires_at',expires_at) AS body
                FROM root
            )
            INSERT INTO twin_outbox(command_id,topic,payload,payload_bytes,status,attempt_count,
                next_attempt_at,created_at,published_at)
            SELECT command_id,'gateways/'||gateway_id||'/commands/'||command_id::text,body,
                convert_to(body::text,'UTF8'),'failed',1,issued_at,issued_at,completed_at FROM payload;
            WITH clock AS (SELECT statement_timestamp() AS now_at),
            active AS (
                SELECT md5('ret-current-102')::uuid AS command_id,
                       md5('ret-entity-102')::uuid AS entity_id,
                       clock.now_at-interval '30 seconds' AS issued_at
                FROM clock
            )
            INSERT INTO twin_commands(command_id,entity_id,action,parameters,status,desired_version,
                idempotency_key,issued_by,issued_at,expires_at,attempt_count,control_version,
                requested_frequency_hz,stage6_managed,origin,root_command_id,original_actor_id,
                auto_successors_used)
            SELECT command_id,entity_id,'update_configuration','{"sample_interval_us":200000}'::jsonb,
                'pending',2,'ret-current-102','11111111-1111-1111-1111-111111111111',
                issued_at,issued_at+interval '1 hour',0,1,'5',true,'client',command_id,
                '11111111-1111-1111-1111-111111111111',0 FROM active;
            WITH active AS (
                SELECT c.*,e.entity_id AS urn,e.gateway_id
                FROM twin_commands c JOIN twin_entities e ON e.id=c.entity_id
                WHERE c.command_id=md5('ret-current-102')::uuid
            ),
            payload AS (
                SELECT active.*,jsonb_build_object('protocol_version',1,'message_type','command',
                    'command_id',command_id::text,'gateway_id',gateway_id,'entity_id',urn,
                    'action',action,'parameters',parameters,'desired_version',desired_version::text,
                    'issued_at',issued_at,'expires_at',expires_at) AS body
                FROM active
            )
            INSERT INTO twin_outbox(command_id,topic,payload,payload_bytes,status,next_attempt_at,created_at)
            SELECT command_id,'gateways/'||gateway_id||'/commands/'||command_id::text,body,
                convert_to(body::text,'UTF8'),'pending',issued_at,issued_at FROM payload;
            INSERT INTO twin_command_requests(actor_id,entity_id,idempotency_key,fingerprint,
                status_code,response_bytes,command_id,created_at)
            SELECT c.issued_by,c.entity_id,c.idempotency_key,
                encode(sha256(convert_to('["update_configuration","5","1"]','UTF8')),'hex'),202,
                convert_to(jsonb_build_object('entity_id',e.entity_id,'command_id',c.command_id::text,
                    'status','pending','requested_frequency_hz',5,'effective_frequency_hz',5,
                    'sample_interval_us',200000,'desired_version','2','expires_at',c.expires_at,
                    'status_url','/v1/digital-twins/'||e.entity_id||'/commands/'||c.command_id::text)::text,'UTF8'),
                c.command_id,c.issued_at
            FROM twin_commands c JOIN twin_entities e ON e.id=c.entity_id
            WHERE c.command_id=md5('ret-current-102')::uuid;
            INSERT INTO twin_command_requests(actor_id,entity_id,idempotency_key,fingerprint,
                status_code,response_bytes,command_id,created_at)
            SELECT '11111111-1111-1111-1111-111111111111',entity_id,'ret-root-102',
                encode(sha256(convert_to('["update_configuration","10","0"]','UTF8')),'hex'),202,
                convert_to(jsonb_build_object('entity_id','urn:ngsi-ld:Sensor:retention-fixture:r102',
                    'command_id',command_id::text,'status','pending','requested_frequency_hz',10,
                    'effective_frequency_hz',10,'sample_interval_us',100000,'desired_version','1',
                    'expires_at',expires_at,
                    'status_url','/v1/digital-twins/urn:ngsi-ld:Sensor:retention-fixture:r102/commands/'||command_id::text)::text,'UTF8'),
                command_id,issued_at
            FROM twin_commands WHERE command_id=md5('ret-root-102')::uuid;""")
        self.sql("""DELETE FROM twin_outbox WHERE command_id=md5('ret-current-1')::uuid;
            WITH parent AS (
                SELECT c.*,e.entity_id AS urn,e.gateway_id,
                    jsonb_build_object('protocol_version',1,'message_type','command_result',
                        'command_id',c.command_id::text,'gateway_id',e.gateway_id,'entity_id',e.entity_id,
                        'desired_version','2','status','succeeded',
                        'reported_state',jsonb_build_object('sample_interval_us',200000),
                        'completed_at',to_char((c.issued_at+interval '30 seconds') AT TIME ZONE 'UTC',
                                               'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')) AS result
                FROM twin_commands c JOIN twin_entities e ON e.id=c.entity_id
                WHERE c.command_id=md5('ret-current-1')::uuid
            )
            UPDATE twin_commands c SET desired_version=2,status='succeeded',
                first_reserved_at=p.issued_at+interval '1 second',attempt_count=1,
                auto_successors_used=1,
                completed_at=p.issued_at+interval '30 seconds',
                result_received_at=p.issued_at+interval '45 seconds',
                result_bytes=convert_to(p.result::text,'UTF8'),
                result_hash=encode(sha256(convert_to(p.result::text,'UTF8')),'hex')
            FROM parent p WHERE c.command_id=p.command_id;
            WITH parent AS (
                SELECT c.*,e.entity_id AS urn,e.gateway_id
                FROM twin_commands c JOIN twin_entities e ON e.id=c.entity_id
                WHERE c.command_id=md5('ret-current-1')::uuid
            ),
            payload AS (
                SELECT parent.*,
                    jsonb_build_object('protocol_version',1,'message_type','command',
                        'command_id',command_id::text,'gateway_id',gateway_id,'entity_id',urn,
                        'action',action,'parameters',parameters,'desired_version','2',
                        'issued_at',issued_at,'expires_at',expires_at) AS body
                FROM parent
            )
            INSERT INTO twin_outbox(command_id,topic,payload,payload_bytes,status,attempt_count,
                next_attempt_at,created_at,published_at)
            SELECT command_id,'gateways/'||gateway_id||'/commands/'||command_id::text,body,
                convert_to(body::text,'UTF8'),'published',1,issued_at,issued_at,completed_at
            FROM payload;
            UPDATE twin_command_requests SET fingerprint=
                    encode(sha256(convert_to('["update_configuration","5","1"]','UTF8')),'hex'),
                response_bytes=convert_to(
                jsonb_set(convert_from(response_bytes,'UTF8')::jsonb,'{desired_version}','\"2\"'::jsonb)::text,
                'UTF8')
            WHERE command_id=md5('ret-current-1')::uuid;
            WITH parent AS (
                SELECT c.*,e.entity_id AS urn,e.gateway_id
                FROM twin_commands c JOIN twin_entities e ON e.id=c.entity_id
                WHERE c.command_id=md5('ret-current-1')::uuid
            )
            INSERT INTO twin_commands(command_id,entity_id,action,parameters,status,desired_version,
                issued_at,expires_at,attempt_count,control_version,requested_frequency_hz,
                stage6_managed,origin,root_command_id,parent_command_id,original_actor_id,
                auto_successors_used)
            SELECT md5('ret-current-child-1')::uuid,entity_id,action,parameters,'pending',3,
                issued_at+interval '1 minute',expires_at+interval '1 minute',0,1,'5',true,
                'reconciliation',command_id,command_id,original_actor_id,0
            FROM parent;
            WITH child AS (
                SELECT c.*,e.entity_id AS urn,e.gateway_id
                FROM twin_commands c JOIN twin_entities e ON e.id=c.entity_id
                WHERE c.command_id=md5('ret-current-child-1')::uuid
            ),
            payload AS (
                SELECT child.*,
                    jsonb_build_object('protocol_version',1,'message_type','command',
                        'command_id',command_id::text,'gateway_id',gateway_id,'entity_id',urn,
                        'action',action,'parameters',parameters,'desired_version','3',
                        'issued_at',issued_at,'expires_at',expires_at) AS body
                FROM child
            )
            INSERT INTO twin_outbox(command_id,topic,payload,payload_bytes,status,next_attempt_at,created_at)
            SELECT command_id,'gateways/'||gateway_id||'/commands/'||command_id::text,body,
                convert_to(body::text,'UTF8'),'pending',issued_at,issued_at FROM payload;
            WITH captured AS (SELECT clock_timestamp() AS now_at)
            UPDATE twin_commands c SET status='cancelled',completed_at=captured.now_at,
                cancelled_at=captured.now_at,cancelled_by='11111111-1111-1111-1111-111111111111',
                retry_stop_reason='cancelled',reconcile_blocked=true
            FROM captured
            WHERE c.command_id=md5('ret-current-101')::uuid;
            UPDATE twin_outbox SET status='failed'
            WHERE command_id=md5('ret-current-101')::uuid;""")
        print('Retention 102 historical lineages and 102 current intents seeded.', flush=True)
        before = self.sql("""SELECT jsonb_build_object(
            'roots',(SELECT count(*) FROM twin_commands c JOIN twin_entities e ON e.id=c.entity_id
                     WHERE e.gateway_id='retention-fixture' AND c.idempotency_key LIKE 'ret-root-%'),
            'children',(SELECT count(*) FROM twin_commands c JOIN twin_entities e ON e.id=c.entity_id
                        WHERE e.gateway_id='retention-fixture' AND c.origin='reconciliation'),
            'current',(SELECT count(*) FROM twin_commands c JOIN twin_entities e ON e.id=c.entity_id
                       WHERE e.gateway_id='retention-fixture' AND c.idempotency_key LIKE 'ret-current-%'),
            'requests',(SELECT count(*) FROM twin_command_requests q JOIN twin_entities e ON e.id=q.entity_id
                        WHERE e.gateway_id='retention-fixture'),
            'outboxes',(SELECT count(*) FROM twin_outbox o JOIN twin_commands c USING(command_id)
                        JOIN twin_entities e ON e.id=c.entity_id WHERE e.gateway_id='retention-fixture'),
            'states',(SELECT jsonb_agg(jsonb_build_array(desired_version,reported_version,desired_state,reported_state)
                        ORDER BY s.entity_id) FROM twin_states s JOIN twin_entities e ON e.id=s.entity_id
                        WHERE e.gateway_id='retention-fixture'))::text""")
        if not before:
            raise Failure('Retention fixture baseline snapshot was empty.')
        state_before = self.sql("""SELECT jsonb_agg(to_jsonb(s) ORDER BY s.entity_id)::text
            FROM twin_states s JOIN twin_entities e ON e.id=s.entity_id
            WHERE e.gateway_id='retention-fixture'""")
        current_receipts_before = self.sql("""SELECT jsonb_agg(
            jsonb_build_array(q.command_id::text,encode(q.response_bytes,'hex')) ORDER BY q.command_id)::text
            FROM twin_command_requests q
            WHERE q.command_id IN (md5('ret-current-1')::uuid,md5('ret-current-101')::uuid,
                                   md5('ret-current-102')::uuid)""")
        dry = self.retention_sql()
        if not re.search(r'DRY-RUN: eligible lineages in this bounded batch =\s+100', dry):
            raise Failure('Retention dry-run did not report the first bounded 100-lineage batch: ' + dry[:200])
        if not re.search(r'DRY-RUN: temporary decoder absent after rollback =\s+t', dry):
            raise Failure('Retention dry-run did not roll back its temporary JSON decoder.')
        if not re.search(r'\nROLLBACK\n', dry):
            raise Failure('Retention dry-run did not end its helper transaction with ROLLBACK.')
        print('Owned dry-run transcript (temporary DDL rolled back):\n' + dry, flush=True)
        if self.sql("""SELECT jsonb_build_object(
            'roots',(SELECT count(*) FROM twin_commands c JOIN twin_entities e ON e.id=c.entity_id
                     WHERE e.gateway_id='retention-fixture' AND c.idempotency_key LIKE 'ret-root-%'),
            'children',(SELECT count(*) FROM twin_commands c JOIN twin_entities e ON e.id=c.entity_id
                        WHERE e.gateway_id='retention-fixture' AND c.origin='reconciliation'),
            'current',(SELECT count(*) FROM twin_commands c JOIN twin_entities e ON e.id=c.entity_id
                       WHERE e.gateway_id='retention-fixture' AND c.idempotency_key LIKE 'ret-current-%'),
            'requests',(SELECT count(*) FROM twin_command_requests q JOIN twin_entities e ON e.id=q.entity_id
                        WHERE e.gateway_id='retention-fixture'),
            'outboxes',(SELECT count(*) FROM twin_outbox o JOIN twin_commands c USING(command_id)
                        JOIN twin_entities e ON e.id=c.entity_id WHERE e.gateway_id='retention-fixture'),
            'states',(SELECT jsonb_agg(jsonb_build_array(desired_version,reported_version,desired_state,reported_state)
                        ORDER BY s.entity_id) FROM twin_states s JOIN twin_entities e ON e.id=s.entity_id
                        WHERE e.gateway_id='retention-fixture'))::text""") != before:
            raise Failure('Retention dry-run mutated fixture data.')
        applied = self.retention_sql('apply', confirm='stage2_test')
        if not re.search(r'APPLY complete: lineages removed =\s+100', applied):
            raise Failure('Retention apply did not remove exactly one bounded batch.')
        self.expect("""SELECT count(*) FROM twin_commands c JOIN twin_entities e ON e.id=c.entity_id
            WHERE e.gateway_id='retention-fixture' AND c.idempotency_key LIKE 'ret-root-%';""", '2')
        self.expect("""SELECT count(*) FROM twin_commands c JOIN twin_entities e ON e.id=c.entity_id
            WHERE e.gateway_id='retention-fixture' AND c.idempotency_key LIKE 'ret-current-%';""", '102')
        self.expect("""SELECT count(*) FROM twin_commands c JOIN twin_entities e ON e.id=c.entity_id
            WHERE e.gateway_id='retention-fixture' AND c.command_id=md5('ret-root-102')::uuid
              AND c.late_result_received_at > clock_timestamp()-interval '720 hours';""", '1')
        self.expect("""SELECT count(*) FROM twin_commands c JOIN twin_entities e ON e.id=c.entity_id
            WHERE e.gateway_id='retention-fixture' AND c.command_id=md5('ret-root-1')::uuid;""", '1')
        self.expect("""SELECT count(*) FROM twin_commands c JOIN twin_entities e ON e.id=c.entity_id
            WHERE e.gateway_id='retention-fixture'
              AND c.command_id IN (md5('ret-root-2')::uuid,md5('ret-root-3')::uuid,
                                   md5('ret-root-101')::uuid);""", '0')
        print('Retention accepted acknowledged/retry_exhausted timeout markers and exponent frequencies 1e1/5e0.', flush=True)
        self.expect("""SELECT count(*) FROM twin_commands c JOIN twin_entities e ON e.id=c.entity_id
            WHERE e.gateway_id='retention-fixture' AND c.command_id=md5('ret-child-1')::uuid;""", '1')
        self.expect("""SELECT count(*) FROM twin_commands c
            WHERE c.command_id=md5('ret-root-102')::uuid
              AND c.late_result_received_at > clock_timestamp()-interval '720 hours';""", '1')
        dry = self.retention_sql('dry-run')
        if not re.search(r'DRY-RUN: eligible lineages in this bounded batch =\s+1', dry):
            raise Failure('Retention did not expose the remaining eligible lineage after batch one: ' + dry[:240])
        print('Retention accepted post-successor late evidence older than 720 hours and held recent late evidence.', flush=True)

        remaining_root = self.sql("""SELECT command_id::text FROM twin_commands
            WHERE idempotency_key='ret-root-1' AND entity_id IN
              (SELECT id FROM twin_entities WHERE gateway_id='retention-fixture')""")
        remaining_entity = self.sql("""SELECT entity_id::text FROM twin_commands
            WHERE command_id='""" + remaining_root + """'""")
        self.expect("""SELECT count(*) FROM twin_commands c JOIN twin_entities e ON e.id=c.entity_id
            WHERE e.gateway_id='retention-fixture' AND c.command_id=md5('ret-current-child-1')::uuid
              AND c.status='pending' AND c.desired_version=3;""", '1')
        self.expect("""SELECT count(*) FROM twin_commands c JOIN twin_entities e ON e.id=c.entity_id
            WHERE e.gateway_id='retention-fixture' AND c.command_id=md5('ret-current-1')::uuid
              AND c.status='succeeded' AND c.auto_successors_used=1;""", '1')
        self.expect("""SELECT count(*) FROM twin_commands c JOIN twin_entities e ON e.id=c.entity_id
            WHERE e.gateway_id='retention-fixture' AND c.command_id=md5('ret-current-101')::uuid
              AND c.status='cancelled' AND c.reconcile_blocked
              AND c.cancelled_at=c.completed_at;""", '1')
        self.expect("""SELECT count(*) FROM twin_commands root
            JOIN twin_commands child ON child.parent_command_id=root.command_id
            WHERE root.command_id=md5('ret-root-1')::uuid
              AND root.status='timeout' AND root.retry_stop_reason='acknowledged'
              AND root.reconcile_blocked
              AND child.command_id=md5('ret-child-1')::uuid AND child.status='cancelled'
              AND child.retry_stop_reason='cancelled' AND child.reconcile_blocked
              AND child.first_reserved_at IS NULL AND child.attempt_count=0
              AND child.cancelled_at=child.completed_at
              AND root.completed_at < child.issued_at
              AND child.issued_at < root.late_result_received_at
              AND root.late_result_received_at < clock_timestamp()-interval '720 hours';""", '1')
        dry = self.retention_sql('dry-run')
        if not re.search(r'DRY-RUN: eligible lineages in this bounded batch =\s+1', dry):
            raise Failure('Retention rejected the old post-successor late result or ignored the recent-late holdout.')
        # Exercise the newer-current blocked-root shape produced by cancelling its
        # pending, never-reserved reconciliation child; dry-run must still pin it.
        self.sql("""UPDATE twin_commands SET reconcile_blocked=true
            WHERE command_id=md5('ret-current-1')::uuid;
            WITH captured AS (SELECT clock_timestamp() AS now_at)
            UPDATE twin_commands c SET status='cancelled',completed_at=captured.now_at,
                cancelled_at=captured.now_at,cancelled_by='11111111-1111-1111-1111-111111111111',
                retry_stop_reason='cancelled',reconcile_blocked=true
            FROM captured WHERE c.command_id=md5('ret-current-child-1')::uuid;
            UPDATE twin_outbox SET status='failed'
            WHERE command_id=md5('ret-current-child-1')::uuid""")
        dry = self.retention_sql('dry-run')
        if not re.search(r'DRY-RUN: eligible lineages in this bounded batch =\s+1', dry):
            raise Failure('Retention failed to recognize coherent blocked current-parent/cancelled-child evidence.')
        self.expect("""SELECT count(*) FROM twin_commands
            WHERE command_id IN (md5('ret-current-1')::uuid,md5('ret-current-child-1')::uuid);""", '2')
        self.expect("""SELECT count(*) FROM twin_commands root
            JOIN twin_commands child ON child.parent_command_id=root.command_id
            JOIN twin_outbox outbox ON outbox.command_id=child.command_id
            WHERE root.command_id=md5('ret-current-1')::uuid AND root.reconcile_blocked
              AND child.command_id=md5('ret-current-child-1')::uuid
              AND child.status='cancelled' AND child.reconcile_blocked
              AND child.first_reserved_at IS NULL AND child.attempt_count=0
              AND child.cancelled_at=child.completed_at AND outbox.status='failed';""", '1')
        print('Retention recognized both historical and current coherent blocked-root/cancelled-child pairs; current pair remains present.', flush=True)
        self.sql("""CREATE TABLE public.retention_saved_request AS
            SELECT * FROM twin_command_requests WHERE command_id=md5('ret-current-102')::uuid;
            UPDATE twin_command_requests SET response_bytes=convert_to(
                jsonb_set(convert_from(response_bytes,'UTF8')::jsonb,
                    '{requested_frequency_hz}','0'::jsonb)::text,'UTF8')
            WHERE command_id=md5('ret-current-102')::uuid""")
        dry = self.retention_sql('dry-run')
        if not re.search(r'DRY-RUN: eligible lineages in this bounded batch =\s+1', dry):
            raise Failure('A zero-frequency receipt caused an unsafe division or hid unrelated eligible work.')
        print('Retention safely ignored zero-frequency receipt and retained unrelated eligible work.', flush=True)
        self.sql("""UPDATE twin_command_requests q SET response_bytes=s.response_bytes
            FROM public.retention_saved_request s WHERE q.command_id=s.command_id;
            DROP TABLE public.retention_saved_request""")
        self.sql("""CREATE TABLE public.retention_saved_request AS
            SELECT * FROM twin_command_requests WHERE command_id=md5('ret-current-102')::uuid;
            UPDATE twin_command_requests SET response_bytes=convert_to(
                jsonb_set(convert_from(response_bytes,'UTF8')::jsonb,
                    '{requested_frequency_hz}','"not-a-number"'::jsonb)::text,'UTF8')
            WHERE command_id=md5('ret-current-102')::uuid""")
        dry = self.retention_sql('dry-run')
        if not re.search(r'DRY-RUN: eligible lineages in this bounded batch =\s+1', dry):
            raise Failure('Malformed frequency receipt aborted candidate selection or hid unrelated eligible work.')
        print('Retention rejected malformed numeric frequency in receipt without aborting valid unrelated work.', flush=True)
        self.sql("""UPDATE twin_command_requests q SET response_bytes=s.response_bytes
            FROM public.retention_saved_request s WHERE q.command_id=s.command_id;
            DROP TABLE public.retention_saved_request""")
        child_outbox_created = self.sql("""SELECT created_at::text FROM twin_outbox
            WHERE command_id=md5('ret-child-1')::uuid""")
        self.sql("""UPDATE twin_outbox SET created_at=clock_timestamp()
            WHERE command_id=md5('ret-child-1')::uuid""")
        dry = self.retention_sql('dry-run')
        if not re.search(r'DRY-RUN: eligible lineages in this bounded batch =\s+0', dry):
            raise Failure('Retention failed to honor child evidence newer than 30 days.')
        self.sql("""UPDATE twin_outbox SET created_at='""" + child_outbox_created + """'::timestamptz
            WHERE command_id=md5('ret-child-1')::uuid""")
        self.sql("""CREATE TABLE public.retention_saved_request AS
            SELECT * FROM twin_command_requests
            WHERE command_id=md5('ret-current-1')::uuid;
            DELETE FROM twin_command_requests
            WHERE command_id=md5('ret-current-1')::uuid""")
        dry = self.retention_sql('dry-run')
        if not re.search(r'DRY-RUN: eligible lineages in this bounded batch =\s+0', dry):
            raise Failure('Retention accepted a spent current successor without its proven user admission.')
        self.sql("""INSERT INTO twin_command_requests SELECT * FROM public.retention_saved_request;
            DROP TABLE public.retention_saved_request""")
        self.sql("""CREATE TABLE public.retention_saved_request AS
            SELECT * FROM twin_command_requests WHERE command_id=md5('ret-current-102')::uuid;
            UPDATE twin_command_requests SET response_bytes=convert_to('{','UTF8')
            WHERE command_id=md5('ret-current-102')::uuid""")
        dry = self.retention_sql('dry-run')
        if not re.search(r'DRY-RUN: eligible lineages in this bounded batch =\s+1', dry):
            raise Failure('Malformed admission bytes aborted the read or hid unrelated eligible work.')
        print('Retention safely decoded malformed admission bytes without aborting unrelated candidate selection.', flush=True)
        self.sql("""UPDATE twin_command_requests q SET response_bytes=s.response_bytes
            FROM public.retention_saved_request s WHERE q.command_id=s.command_id;
            DROP TABLE public.retention_saved_request""")
        self.sql("""UPDATE twin_states SET desired_state='{"sample_interval_us":300000}'::jsonb
            WHERE entity_id='""" + remaining_entity + """'::uuid""")
        dry = self.retention_sql('dry-run')
        if not re.search(r'DRY-RUN: eligible lineages in this bounded batch =\s+0', dry):
            raise Failure('Retention accepted a current successor inconsistent with Sensor desired state.')
        self.sql("""UPDATE twin_states SET desired_state='{"sample_interval_us":200000}'::jsonb
            WHERE entity_id='""" + remaining_entity + """'::uuid""")
        outbox_created = self.sql("""SELECT created_at::text FROM twin_outbox
            WHERE command_id='""" + remaining_root + """'""")
        self.sql("""UPDATE twin_outbox SET created_at=clock_timestamp()
            WHERE command_id='""" + remaining_root + """'""")
        dry = self.retention_sql('dry-run')
        if not re.search(r'DRY-RUN: eligible lineages in this bounded batch =\s+0', dry):
            raise Failure('Retention ignored recent outbox evidence when computing the 30-day cutoff.')
        self.sql("""UPDATE twin_outbox SET created_at='""" + outbox_created + """'::timestamptz
            WHERE command_id='""" + remaining_root + """'""")
        dry = self.retention_sql('dry-run')
        if not re.search(r'DRY-RUN: eligible lineages in this bounded batch =\s+1', dry):
            raise Failure('Retention did not restore eligibility after evidence was repaired.')
        self.sql("""CREATE FUNCTION public.retention_fixture_veto() RETURNS trigger
            LANGUAGE plpgsql AS $$ BEGIN
                IF OLD.command_id::text = '""" + remaining_root + """' THEN
                    RAISE EXCEPTION 'fixture retention delete veto';
                END IF;
                RETURN OLD;
            END $$;
            CREATE TRIGGER retention_fixture_veto BEFORE DELETE ON public.twin_commands
            FOR EACH ROW EXECUTE FUNCTION public.retention_fixture_veto();""")
        before_failed_apply = self.sql("""SELECT jsonb_build_object(
            'commands',(SELECT count(*) FROM twin_commands WHERE command_id='""" + remaining_root + """'),
            'request',(SELECT count(*) FROM twin_command_requests WHERE command_id='""" + remaining_root + """'),
            'outbox',(SELECT count(*) FROM twin_outbox WHERE command_id='""" + remaining_root + """'))::text""")
        try:
            self.retention_sql('apply', confirm='stage2_test')
            raise Failure('Injected delete veto did not fail the retention transaction.')
        except Failure as error:
            if 'fixture retention delete veto' not in str(error):
                raise
        if self.sql("""SELECT jsonb_build_object(
            'commands',(SELECT count(*) FROM twin_commands WHERE command_id='""" + remaining_root + """'),
            'request',(SELECT count(*) FROM twin_command_requests WHERE command_id='""" + remaining_root + """'),
            'outbox',(SELECT count(*) FROM twin_outbox WHERE command_id='""" + remaining_root + """'))::text""") != before_failed_apply:
            raise Failure('Failed retention transaction partially deleted a lineage.')
        self.sql("DROP TRIGGER retention_fixture_veto ON public.twin_commands; DROP FUNCTION public.retention_fixture_veto();")
        applied = self.retention_sql('apply', confirm='stage2_test')
        if not re.search(r'APPLY complete: lineages removed =\s+1', applied):
            raise Failure('Retention rerun did not remove the remaining eligible lineage.')
        applied = self.retention_sql('apply', confirm='stage2_test')
        if not re.search(r'APPLY complete: lineages removed =\s+0', applied):
            raise Failure('Retention rerun was not idempotent.')
        self.expect("""SELECT count(*) FROM twin_commands c JOIN twin_entities e ON e.id=c.entity_id
            WHERE e.gateway_id='retention-fixture' AND c.idempotency_key LIKE 'ret-root-%';""", '1')
        self.expect("""SELECT count(*) FROM twin_commands c JOIN twin_entities e ON e.id=c.entity_id
            WHERE e.gateway_id='retention-fixture' AND c.idempotency_key LIKE 'ret-current-%';""", '102')
        self.expect("""SELECT count(*) FROM twin_command_requests q JOIN twin_entities e ON e.id=q.entity_id
            WHERE e.gateway_id='retention-fixture' AND q.status_code=200 AND q.command_id IS NULL;""", '1')
        self.expect("""SELECT count(*) FROM twin_command_requests q JOIN twin_entities e ON e.id=q.entity_id
            WHERE e.gateway_id='retention-fixture' AND q.status_code=202
              AND q.idempotency_key LIKE 'ret-current-%';""", '102')
        if self.sql("""SELECT jsonb_agg(to_jsonb(s) ORDER BY s.entity_id)::text
            FROM twin_states s JOIN twin_entities e ON e.id=s.entity_id
            WHERE e.gateway_id='retention-fixture'""") != state_before:
            raise Failure('Retention changed desired/confirmed state, versions, or reconciliation decision.')
        if self.sql("""SELECT jsonb_agg(
            jsonb_build_array(q.command_id::text,encode(q.response_bytes,'hex')) ORDER BY q.command_id)::text
            FROM twin_command_requests q
            WHERE q.command_id IN (md5('ret-current-1')::uuid,md5('ret-current-101')::uuid,
                                   md5('ret-current-102')::uuid)""") != current_receipts_before:
            raise Failure('Retention changed or removed current-lineage receipt bytes.')
        print('Retention current cancelled/blocked and spent-successor lineages, state and receipt bytes preserved.', flush=True)
        print('Stage 6 retention native bounded dry-run/apply, lineage cascade, atomic failure and rerun passed.', flush=True)

        # Three realistic eligible lineages: a valid control, a root with an
        # inconsistent outbox, and a root/successor pair whose child outbox is
        # inconsistent. The pre-damage dry-run proves all three are candidates.
        self.sql("""INSERT INTO gateways(gateway_id,name) VALUES ('retention-review','retention review');
            INSERT INTO sensors(gateway_id,sensor_id,name)
            SELECT 'retention-review','s'||i,'retention review '||i FROM generate_series(1,3) i;
            INSERT INTO twin_entities(id,entity_id,entity_type,gateway_id)
            SELECT md5('ret-finding-entity-'||i)::uuid,
                   'urn:ngsi-ld:Sensor:retention-review:s'||i,'Sensor','retention-review'
            FROM generate_series(1,3) i;
            INSERT INTO twin_states(entity_id,desired_state,reported_state,desired_version,reported_version)
            SELECT md5('ret-finding-entity-'||i)::uuid,
                   '{"sample_interval_us":200000}'::jsonb,
                   '{"configuration":{"sample_interval_us":100000}}'::jsonb,
                   CASE WHEN i=3 THEN 3 ELSE 2 END,1
            FROM generate_series(1,3) i;
            WITH clock AS (SELECT statement_timestamp() AS now_at),
            roots AS (
                SELECT i,md5('ret-finding-root-'||i)::uuid AS command_id,
                       md5('ret-finding-entity-'||i)::uuid AS entity_id,
                       'urn:ngsi-ld:Sensor:retention-review:s'||i AS urn,
                       clock.now_at-interval '40 days' AS issued_at,
                       clock.now_at
                FROM generate_series(1,3) i CROSS JOIN clock
            )
            INSERT INTO twin_commands(command_id,entity_id,action,parameters,status,desired_version,
                idempotency_key,issued_by,issued_at,expires_at,completed_at,attempt_count,
                control_version,requested_frequency_hz,stage6_managed,first_reserved_at,
                retry_stop_reason,origin,root_command_id,original_actor_id,auto_successors_used,
                error_code,error_message)
            SELECT command_id,entity_id,'update_configuration','{"sample_interval_us":100000}'::jsonb,
                'timeout',1,'ret-finding-root-'||i,'11111111-1111-1111-1111-111111111111',
                issued_at,issued_at+interval '1 minute',issued_at+interval '61 seconds',1,1,'10',
                true,issued_at,'retry_exhausted','client',command_id,
                '11111111-1111-1111-1111-111111111111',CASE WHEN i=3 THEN 1 ELSE 0 END,
                'COMMAND_EXPIRED','command expired' FROM roots;
            WITH parent AS (
                SELECT c.*,e.entity_id AS urn,e.gateway_id
                FROM twin_commands c JOIN twin_entities e ON e.id=c.entity_id
                WHERE c.command_id=md5('ret-finding-root-3')::uuid
            )
            INSERT INTO twin_commands(command_id,entity_id,action,parameters,status,desired_version,
                issued_at,expires_at,completed_at,attempt_count,control_version,requested_frequency_hz,
                stage6_managed,first_reserved_at,retry_stop_reason,origin,root_command_id,
                parent_command_id,original_actor_id,auto_successors_used,error_code,error_message)
            SELECT md5('ret-finding-child-3')::uuid,entity_id,action,parameters,'timeout',2,
                completed_at+interval '1 minute',completed_at+interval '2 minutes',
                completed_at+interval '2 minutes 1 second',1,1,'10',true,
                completed_at+interval '1 minute','retry_exhausted','reconciliation',
                command_id,command_id,original_actor_id,0,'COMMAND_EXPIRED','command expired'
            FROM parent;
            WITH clock AS (SELECT statement_timestamp() AS now_at),
            currents AS (
                SELECT i,md5('ret-finding-current-'||i)::uuid AS command_id,
                       md5('ret-finding-entity-'||i)::uuid AS entity_id,
                       clock.now_at-interval '1 minute' AS issued_at
                FROM generate_series(1,3) i CROSS JOIN clock
            )
            INSERT INTO twin_commands(command_id,entity_id,action,parameters,status,desired_version,
                idempotency_key,issued_by,issued_at,expires_at,control_version,requested_frequency_hz,
                stage6_managed,origin,root_command_id,original_actor_id,auto_successors_used)
            SELECT command_id,entity_id,'update_configuration','{"sample_interval_us":200000}'::jsonb,
                'pending',CASE WHEN i=3 THEN 3 ELSE 2 END,'ret-finding-current-'||i,
                '11111111-1111-1111-1111-111111111111',issued_at,issued_at+interval '1 hour',
                1,'5',true,'client',command_id,
                '11111111-1111-1111-1111-111111111111',0 FROM currents;
            WITH commands AS (
                SELECT c.*,e.entity_id AS urn,e.gateway_id
                FROM twin_commands c JOIN twin_entities e ON e.id=c.entity_id
                WHERE e.gateway_id='retention-review'
            ),
            payloads AS (
                SELECT commands.*,
                    jsonb_build_object('protocol_version',1,'message_type','command',
                        'command_id',command_id::text,'gateway_id',gateway_id,'entity_id',urn,
                        'action',action,'parameters',parameters,'desired_version',desired_version::text,
                        'issued_at',issued_at,'expires_at',expires_at) AS body
                FROM commands
            )
            INSERT INTO twin_outbox(command_id,topic,payload,payload_bytes,status,attempt_count,
                next_attempt_at,created_at,published_at)
            SELECT command_id,'gateways/'||gateway_id||'/commands/'||command_id::text,body,
                convert_to(body::text,'UTF8'),
                CASE WHEN status='pending' THEN 'pending' ELSE 'failed' END,
                CASE WHEN status='pending' THEN 0 ELSE 1 END,issued_at,issued_at,
                CASE WHEN status='pending' THEN NULL ELSE completed_at END
            FROM payloads;
            WITH commands AS (
                SELECT c.*,e.entity_id AS urn
                FROM twin_commands c JOIN twin_entities e ON e.id=c.entity_id
                WHERE e.gateway_id='retention-review'
                  AND c.origin='client'
            ),
            receipts AS (
                SELECT commands.*,
                    CASE WHEN idempotency_key LIKE 'ret-finding-root-%' THEN
                        '["update_configuration","10","0"]'
                    ELSE format('["update_configuration","5","%s"]',
                                desired_version-1) END AS fingerprint_input,
                    convert_to(jsonb_build_object('entity_id',urn,'command_id',command_id::text,
                        'status',CASE WHEN status='pending' THEN 'pending' ELSE 'pending' END,
                        'requested_frequency_hz',CASE WHEN idempotency_key LIKE 'ret-finding-root-%' THEN 10 ELSE 5 END,
                        'effective_frequency_hz',CASE WHEN idempotency_key LIKE 'ret-finding-root-%' THEN 10 ELSE 5 END,
                        'sample_interval_us',CASE WHEN idempotency_key LIKE 'ret-finding-root-%' THEN 100000 ELSE 200000 END,
                        'desired_version',desired_version::text,'expires_at',expires_at,
                        'status_url','/v1/digital-twins/'||urn||'/commands/'||command_id::text)::text,
                        'UTF8') AS response_bytes
                FROM commands
            )
            INSERT INTO twin_command_requests(actor_id,entity_id,idempotency_key,fingerprint,
                status_code,response_bytes,command_id,created_at)
            SELECT issued_by,entity_id,idempotency_key,
                encode(sha256(convert_to(fingerprint_input,'UTF8')),'hex'),202,
                response_bytes,command_id,issued_at FROM receipts;""")
        dry = self.retention_sql()
        if not re.search(r'DRY-RUN: eligible lineages in this bounded batch =\s+3', dry):
            raise Failure('Retention evidence-safety fixture did not establish three eligible lineages before corruption: ' + dry[:240])
        self.sql("""UPDATE twin_commands SET requested_frequency_hz='5e00000'
            WHERE command_id=md5('ret-finding-current-1')::uuid""")
        dry = self.retention_sql()
        if not re.search(r'DRY-RUN: eligible lineages in this bounded batch =\s+3', dry):
            raise Failure('Retention rejected a valid exponent with leading zeros: ' + dry[:240])
        self.sql("""UPDATE twin_commands SET requested_frequency_hz='5e000129'
            WHERE command_id=md5('ret-finding-current-1')::uuid""")
        dry = self.retention_sql()
        if not re.search(r'DRY-RUN: eligible lineages in this bounded batch =\s+2', dry):
            raise Failure('Retention accepted an exponent outside the Go limit or hid unrelated work.')
        self.sql("""UPDATE twin_commands SET requested_frequency_hz='5'
            WHERE command_id=md5('ret-finding-current-1')::uuid""")
        for effective_hz, expected in ((1, 2), (5, 3)):
            self.sql(f"""UPDATE twin_command_requests
                SET response_bytes=convert_to(jsonb_set(convert_from(response_bytes,'UTF8')::jsonb,
                    '{{effective_frequency_hz}}',to_jsonb({effective_hz}))::text,'UTF8')
                WHERE command_id=md5('ret-finding-current-1')::uuid""")
            dry = self.retention_sql()
            if not re.search(rf'DRY-RUN: eligible lineages in this bounded batch =\s+{expected}\b', dry):
                raise Failure('Retention did not enforce the admission effective frequency.')
        self.sql("""WITH evidence AS (
            SELECT c.command_id, c.expires_at,
                convert_to(jsonb_build_object('protocol_version',1,'message_type','command_result',
                    'command_id',c.command_id::text,'gateway_id',e.gateway_id,'entity_id',e.entity_id,
                    'desired_version',c.desired_version::text,'status','succeeded',
                    'reported_state',jsonb_build_object('sample_interval_us','100000'::text),
                    'completed_at',to_char((c.issued_at+interval '30 seconds') AT TIME ZONE 'UTC',
                                           'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'))::text,'UTF8') AS body
            FROM twin_commands c JOIN twin_entities e ON e.id=c.entity_id
            WHERE c.command_id=md5('ret-finding-root-3')::uuid
        )
        UPDATE twin_commands c SET late_result_bytes=evidence.body,
            late_result_hash=encode(sha256(evidence.body),'hex'),
            late_result_received_at=evidence.expires_at+interval '5 minutes'
        FROM evidence WHERE c.command_id=evidence.command_id""")
        dry = self.retention_sql()
        if not re.search(r'DRY-RUN: eligible lineages in this bounded batch =\s+2', dry):
            raise Failure('Retention accepted a hashed late result with string interval: ' + dry[:240])
        self.sql("""UPDATE twin_outbox SET status='pending',attempt_count=0
            WHERE command_id=md5('ret-finding-root-2')::uuid;
            UPDATE twin_outbox SET attempt_count=0
            WHERE command_id=md5('ret-finding-child-3')::uuid""")
        dry = self.retention_sql()
        if not re.search(r'DRY-RUN: eligible lineages in this bounded batch =\s+1', dry):
            raise Failure('Retention dry-run selected contradictory root/successor outbox evidence: ' + dry[:240])
        applied = self.retention_sql('apply', confirm='stage2_test')
        if not re.search(r'APPLY complete: lineages removed =\s+1', applied):
            raise Failure('Retention did not purge the valid control lineage while holding contradictory evidence.')
        self.expect("""SELECT count(*) FROM twin_commands c
            JOIN twin_entities e ON e.id=c.entity_id
            WHERE e.gateway_id='retention-review'
              AND c.command_id IN (md5('ret-finding-root-2')::uuid,
                                   md5('ret-finding-root-3')::uuid,
                                   md5('ret-finding-child-3')::uuid);""", '3')
        self.expect("""SELECT count(*) FROM twin_outbox
            WHERE command_id IN (md5('ret-finding-root-2')::uuid,
                                 md5('ret-finding-child-3')::uuid);""", '2')
        self.expect("""SELECT count(*) FROM twin_command_requests
            WHERE command_id IN (md5('ret-finding-root-2')::uuid,
                                 md5('ret-finding-root-3')::uuid);""", '2')
        self.expect("""SELECT count(*) FROM twin_outbox
            WHERE command_id=md5('ret-finding-root-3')::uuid;""", '1')
        print('Retention held contradictory outbox, hashed string-interval late result and mismatched admission evidence; valid work remained eligible.', flush=True)

        # Stage 6 duplicate-key regression: prove that real apply purges a valid control
        # lineage while preserving invalid lineages whose result payload contains
        # duplicate JSON keys (which JSONB/PostgreSQL normalizes but Go rejects).
        self.sql("""INSERT INTO gateways(gateway_id,name) VALUES ('retention-duplicate','retention duplicate');
            INSERT INTO sensors(gateway_id,sensor_id,name)
            SELECT 'retention-duplicate','s'||i,'retention duplicate '||i FROM generate_series(1,4) i;
            INSERT INTO twin_entities(id,entity_id,entity_type,gateway_id)
            SELECT md5('ret-dup-entity-'||i)::uuid,
                   'urn:ngsi-ld:Sensor:retention-duplicate:s'||i,'Sensor','retention-duplicate'
            FROM generate_series(1,4) i;
            INSERT INTO twin_states(entity_id,desired_state,reported_state,desired_version,reported_version)
            SELECT md5('ret-dup-entity-'||i)::uuid,
                   '{"sample_interval_us":200000}'::jsonb,
                   '{"configuration":{"sample_interval_us":100000}}'::jsonb,
                   CASE WHEN i=4 THEN 3 ELSE 2 END,1
            FROM generate_series(1,4) i;
            WITH clock AS (SELECT statement_timestamp() AS now_at),
            roots AS (
                SELECT i,md5('ret-dup-root-'||i)::uuid AS command_id,
                       md5('ret-dup-entity-'||i)::uuid AS entity_id,
                       'urn:ngsi-ld:Sensor:retention-duplicate:s'||i AS urn,
                       clock.now_at-interval '40 days' AS issued_at,
                       clock.now_at
                FROM generate_series(1,4) i CROSS JOIN clock
            )
            INSERT INTO twin_commands(command_id,entity_id,action,parameters,status,desired_version,
                idempotency_key,issued_by,issued_at,expires_at,completed_at,attempt_count,
                control_version,requested_frequency_hz,stage6_managed,first_reserved_at,
                retry_stop_reason,origin,root_command_id,original_actor_id,auto_successors_used,
                error_code,error_message)
            SELECT command_id,entity_id,'update_configuration','{"sample_interval_us":100000}'::jsonb,
                CASE WHEN i=3 THEN 'succeeded' ELSE 'timeout' END,1,
                'ret-dup-root-'||i,'11111111-1111-1111-1111-111111111111',
                issued_at,issued_at+interval '1 minute',
                CASE WHEN i=3 THEN issued_at+interval '30 seconds' ELSE issued_at+interval '61 seconds' END,1,1,'10',
                true,issued_at,
                CASE WHEN i=3 THEN NULL ELSE 'retry_exhausted' END,'client',command_id,
                '11111111-1111-1111-1111-111111111111',CASE WHEN i=4 THEN 1 ELSE 0 END,
                CASE WHEN i=3 THEN NULL ELSE 'COMMAND_EXPIRED' END,
                CASE WHEN i=3 THEN NULL ELSE 'command expired' END FROM roots;
            WITH parent AS (
                SELECT c.*,e.entity_id AS urn,e.gateway_id
                FROM twin_commands c JOIN twin_entities e ON e.id=c.entity_id
                WHERE c.command_id=md5('ret-dup-root-4')::uuid
            )
            INSERT INTO twin_commands(command_id,entity_id,action,parameters,status,desired_version,
                issued_at,expires_at,completed_at,attempt_count,control_version,requested_frequency_hz,
                stage6_managed,first_reserved_at,retry_stop_reason,origin,root_command_id,
                parent_command_id,original_actor_id,auto_successors_used)
            SELECT md5('ret-dup-child-4')::uuid,entity_id,action,parameters,'succeeded',2,
                completed_at+interval '1 minute',completed_at+interval '2 minutes',
                completed_at+interval '90 seconds',1,1,'10',true,
                completed_at+interval '1 minute',NULL,'reconciliation',
                command_id,command_id,original_actor_id,0
            FROM parent;
            WITH clock AS (SELECT statement_timestamp() AS now_at),
            currents AS (
                SELECT i,md5('ret-dup-current-'||i)::uuid AS command_id,
                       md5('ret-dup-entity-'||i)::uuid AS entity_id,
                       clock.now_at-interval '1 minute' AS issued_at
                FROM generate_series(1,4) i CROSS JOIN clock
            )
            INSERT INTO twin_commands(command_id,entity_id,action,parameters,status,desired_version,
                idempotency_key,issued_by,issued_at,expires_at,control_version,requested_frequency_hz,
                stage6_managed,origin,root_command_id,original_actor_id,auto_successors_used)
            SELECT command_id,entity_id,'update_configuration','{"sample_interval_us":200000}'::jsonb,
                'pending',CASE WHEN i=4 THEN 3 ELSE 2 END,'ret-dup-current-'||i,
                '11111111-1111-1111-1111-111111111111',issued_at,issued_at+interval '1 hour',
                1,'5',true,'client',command_id,
                '11111111-1111-1111-1111-111111111111',0 FROM currents;
            WITH commands AS (
                SELECT c.*,e.entity_id AS urn,e.gateway_id
                FROM twin_commands c JOIN twin_entities e ON e.id=c.entity_id
                WHERE e.gateway_id='retention-duplicate'
            ),
            payloads AS (
                SELECT commands.*,
                    jsonb_build_object('protocol_version',1,'message_type','command',
                        'command_id',command_id::text,'gateway_id',gateway_id,'entity_id',urn,
                        'action',action,'parameters',parameters,'desired_version',desired_version::text,
                        'issued_at',issued_at,'expires_at',expires_at) AS body
                FROM commands
            )
            INSERT INTO twin_outbox(command_id,topic,payload,payload_bytes,status,attempt_count,
                next_attempt_at,created_at,published_at)
            SELECT command_id,'gateways/'||gateway_id||'/commands/'||command_id::text,body,
                convert_to(body::text,'UTF8'),
                CASE WHEN status='pending' THEN 'pending'
                     WHEN status='succeeded' THEN 'published'
                     ELSE 'failed' END,
                CASE WHEN status='pending' THEN 0 ELSE 1 END,issued_at,issued_at,
                CASE WHEN status='pending' THEN NULL ELSE completed_at END
            FROM payloads;
            WITH commands AS (
                SELECT c.*,e.entity_id AS urn
                FROM twin_commands c JOIN twin_entities e ON e.id=c.entity_id
                WHERE e.gateway_id='retention-duplicate'
                  AND c.origin='client'
            ),
            receipts AS (
                SELECT commands.*,
                    CASE WHEN idempotency_key LIKE 'ret-dup-root-%' THEN
                        '["update_configuration","10","0"]'
                    ELSE format('["update_configuration","5","%s"]',
                                desired_version-1) END AS fingerprint_input,
                    convert_to(jsonb_build_object('entity_id',urn,'command_id',command_id::text,
                        'status','pending',
                        'requested_frequency_hz',CASE WHEN idempotency_key LIKE 'ret-dup-root-%' THEN 10 ELSE 5 END,
                        'effective_frequency_hz',CASE WHEN idempotency_key LIKE 'ret-dup-root-%' THEN 10 ELSE 5 END,
                        'sample_interval_us',CASE WHEN idempotency_key LIKE 'ret-dup-root-%' THEN 100000 ELSE 200000 END,
                        'desired_version',desired_version::text,'expires_at',expires_at,
                        'status_url','/v1/digital-twins/'||urn||'/commands/'||command_id::text)::text,
                        'UTF8') AS response_bytes
                FROM commands
            )
            INSERT INTO twin_command_requests(actor_id,entity_id,idempotency_key,fingerprint,
                status_code,response_bytes,command_id,created_at)
            SELECT issued_by,entity_id,idempotency_key,
                encode(sha256(convert_to(fingerprint_input,'UTF8')),'hex'),202,
                response_bytes,command_id,issued_at FROM receipts;
            WITH evidence AS (
                SELECT c.command_id, c.expires_at,
                    convert_to(replace(jsonb_build_object('protocol_version',1,'message_type','command_result',
                        'command_id',c.command_id::text,'gateway_id',e.gateway_id,'entity_id',e.entity_id,
                        'desired_version',c.desired_version::text,'status','succeeded',
                        'reported_state',jsonb_build_object('sample_interval_us',100000),
                        'completed_at',to_char((c.issued_at+interval '30 seconds') AT TIME ZONE 'UTC',
                                               'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'))::text,
                        '"sample_interval_us": 100000',
                        '"sample_interval_us": "100000", "sample_interval_us": 100000'), 'UTF8') AS body
                FROM twin_commands c JOIN twin_entities e ON e.id=c.entity_id
                WHERE c.command_id=md5('ret-dup-root-2')::uuid
            )
            UPDATE twin_commands c SET late_result_bytes=evidence.body,
                late_result_hash=encode(sha256(evidence.body),'hex'),
                late_result_received_at=evidence.expires_at+interval '5 minutes'
            FROM evidence WHERE c.command_id=evidence.command_id;
            WITH evidence AS (
                SELECT c.command_id, c.completed_at,
                    convert_to(replace(jsonb_build_object('protocol_version',1,'message_type','command_result',
                        'command_id',c.command_id::text,'gateway_id',e.gateway_id,'entity_id',e.entity_id,
                        'desired_version',c.desired_version::text,'status','succeeded',
                        'reported_state',jsonb_build_object('sample_interval_us',100000),
                        'completed_at',to_char(c.completed_at AT TIME ZONE 'UTC',
                                               'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'))::text,
                        '"sample_interval_us": 100000',
                        '"sample_interval_us": "100000", "sample_interval_us": 100000'), 'UTF8') AS body
                FROM twin_commands c JOIN twin_entities e ON e.id=c.entity_id
                WHERE c.command_id=md5('ret-dup-root-3')::uuid
            )
            UPDATE twin_commands c SET result_bytes=evidence.body,
                result_hash=encode(sha256(evidence.body),'hex'),
                result_received_at=evidence.completed_at
            FROM evidence WHERE c.command_id=evidence.command_id;
            WITH evidence AS (
                SELECT c.command_id, c.completed_at,
                    convert_to(replace(jsonb_build_object('protocol_version',1,'message_type','command_result',
                        'command_id',c.command_id::text,'gateway_id',e.gateway_id,'entity_id',e.entity_id,
                        'desired_version',c.desired_version::text,'status','succeeded',
                        'reported_state',jsonb_build_object('sample_interval_us',100000),
                        'completed_at',to_char(c.completed_at AT TIME ZONE 'UTC',
                                               'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'))::text,
                        '"reported_state": {"sample_interval_us": 100000}',
                        '"reported_state": {"sample_interval_us": 100000}, "reported_state": {"sample_interval_us": 100000}'), 'UTF8') AS body
                FROM twin_commands c JOIN twin_entities e ON e.id=c.entity_id
                WHERE c.command_id=md5('ret-dup-child-4')::uuid
            )
            UPDATE twin_commands c SET result_bytes=evidence.body,
                result_hash=encode(sha256(evidence.body),'hex'),
                result_received_at=evidence.completed_at
            FROM evidence WHERE c.command_id=evidence.command_id;""")

        dup_snapshot_before = self.sql("""SELECT jsonb_build_object(
            'roots', (SELECT count(*) FROM twin_commands WHERE command_id IN (
                md5('ret-dup-root-2')::uuid, md5('ret-dup-root-3')::uuid, md5('ret-dup-root-4')::uuid)),
            'children', (SELECT count(*) FROM twin_commands WHERE command_id=md5('ret-dup-child-4')::uuid),
            'outbox', (SELECT count(*) FROM twin_outbox WHERE command_id IN (
                md5('ret-dup-root-2')::uuid, md5('ret-dup-root-3')::uuid,
                md5('ret-dup-root-4')::uuid, md5('ret-dup-child-4')::uuid)),
            'requests', (SELECT count(*) FROM twin_command_requests WHERE command_id IN (
                md5('ret-dup-root-2')::uuid, md5('ret-dup-root-3')::uuid, md5('ret-dup-root-4')::uuid)),
            'root2_late_bytes', (SELECT encode(late_result_bytes, 'hex') FROM twin_commands WHERE command_id=md5('ret-dup-root-2')::uuid),
            'root2_late_hash', (SELECT late_result_hash FROM twin_commands WHERE command_id=md5('ret-dup-root-2')::uuid),
            'root3_result_bytes', (SELECT encode(result_bytes, 'hex') FROM twin_commands WHERE command_id=md5('ret-dup-root-3')::uuid),
            'root3_result_hash', (SELECT result_hash FROM twin_commands WHERE command_id=md5('ret-dup-root-3')::uuid),
            'child4_result_bytes', (SELECT encode(result_bytes, 'hex') FROM twin_commands WHERE command_id=md5('ret-dup-child-4')::uuid),
            'child4_result_hash', (SELECT result_hash FROM twin_commands WHERE command_id=md5('ret-dup-child-4')::uuid)
        )::text""")

        dry = self.retention_sql()
        if not re.search(r'DRY-RUN: eligible lineages in this bounded batch =\s+1', dry):
            raise Failure('Retention dry-run selected invalid duplicate-key lineages: ' + dry[:240])

        applied = self.retention_sql('apply', confirm='stage2_test')
        if not re.search(r'APPLY complete: lineages removed =\s+1', applied):
            raise Failure('Retention apply purged invalid duplicate-key lineages: ' + applied[:240])
        self.expect("""SELECT count(*) FROM twin_commands
            WHERE command_id=md5('ret-dup-root-1')::uuid;""", '0')
        self.expect("""SELECT count(*) FROM twin_outbox
            WHERE command_id=md5('ret-dup-root-1')::uuid;""", '0')
        self.expect("""SELECT count(*) FROM twin_command_requests
            WHERE command_id=md5('ret-dup-root-1')::uuid;""", '0')
        dup_snapshot_after = self.sql("""SELECT jsonb_build_object(
            'roots', (SELECT count(*) FROM twin_commands WHERE command_id IN (
                md5('ret-dup-root-2')::uuid, md5('ret-dup-root-3')::uuid, md5('ret-dup-root-4')::uuid)),
            'children', (SELECT count(*) FROM twin_commands WHERE command_id=md5('ret-dup-child-4')::uuid),
            'outbox', (SELECT count(*) FROM twin_outbox WHERE command_id IN (
                md5('ret-dup-root-2')::uuid, md5('ret-dup-root-3')::uuid,
                md5('ret-dup-root-4')::uuid, md5('ret-dup-child-4')::uuid)),
            'requests', (SELECT count(*) FROM twin_command_requests WHERE command_id IN (
                md5('ret-dup-root-2')::uuid, md5('ret-dup-root-3')::uuid, md5('ret-dup-root-4')::uuid)),
            'root2_late_bytes', (SELECT encode(late_result_bytes, 'hex') FROM twin_commands WHERE command_id=md5('ret-dup-root-2')::uuid),
            'root2_late_hash', (SELECT late_result_hash FROM twin_commands WHERE command_id=md5('ret-dup-root-2')::uuid),
            'root3_result_bytes', (SELECT encode(result_bytes, 'hex') FROM twin_commands WHERE command_id=md5('ret-dup-root-3')::uuid),
            'root3_result_hash', (SELECT result_hash FROM twin_commands WHERE command_id=md5('ret-dup-root-3')::uuid),
            'child4_result_bytes', (SELECT encode(result_bytes, 'hex') FROM twin_commands WHERE command_id=md5('ret-dup-child-4')::uuid),
            'child4_result_hash', (SELECT result_hash FROM twin_commands WHERE command_id=md5('ret-dup-child-4')::uuid)
        )::text""")
        if dup_snapshot_after != dup_snapshot_before:
            raise Failure('Retention apply modified or deleted invalid duplicate-key lineages: ' + dup_snapshot_after)
        print('Retention preserved duplicate-key late/canonical results and duplicate reported_state wrapper; purged valid control.', flush=True)

        # Reproduce the bounded-work path at the independently observed 1,000
        # historical roots / 10 current Sensors scale using the real psql file.
        self.sql("""INSERT INTO gateways(gateway_id,name) VALUES ('retention-backlog','retention backlog');
            INSERT INTO sensors(gateway_id,sensor_id,name)
            SELECT 'retention-backlog','s'||i,'retention backlog '||i FROM generate_series(1,10) i;
            INSERT INTO twin_entities(id,entity_id,entity_type,gateway_id)
            SELECT md5('ret-backlog-entity-'||i)::uuid,
                   'urn:ngsi-ld:Sensor:retention-backlog:s'||i,'Sensor','retention-backlog'
            FROM generate_series(1,10) i;
            INSERT INTO twin_states(entity_id,desired_state,reported_state,desired_version,reported_version)
            SELECT md5('ret-backlog-entity-'||i)::uuid,
                   '{"sample_interval_us":200000}'::jsonb,
                   '{"configuration":{"sample_interval_us":100000}}'::jsonb,101,1
            FROM generate_series(1,10) i;
            WITH clock AS (SELECT statement_timestamp() AS now_at),
            history AS (
                SELECT sensor,version,md5('ret-backlog-command-'||sensor||':'||version)::uuid AS command_id,
                       md5('ret-backlog-entity-'||sensor)::uuid AS entity_id,
                       'urn:ngsi-ld:Sensor:retention-backlog:s'||sensor AS urn,
                       clock.now_at-interval '40 days'-make_interval(secs=>version) AS issued_at
                FROM generate_series(1,10) sensor
                CROSS JOIN generate_series(1,100) version CROSS JOIN clock
            )
            INSERT INTO twin_commands(command_id,entity_id,action,parameters,status,desired_version,
                idempotency_key,issued_by,issued_at,expires_at,completed_at,control_version,
                requested_frequency_hz,stage6_managed,origin,root_command_id,original_actor_id,error_code)
            SELECT command_id,entity_id,'update_configuration','{"sample_interval_us":100000}'::jsonb,
                'timeout',version,'ret-backlog-root-'||sensor||'-'||version,
                '11111111-1111-1111-1111-111111111111',issued_at,issued_at+interval '90 seconds',
                issued_at+interval '91 seconds',1,'10',true,'client',command_id,
                '11111111-1111-1111-1111-111111111111','COMMAND_EXPIRED'
            FROM history;
            WITH clock AS (SELECT statement_timestamp() AS now_at),
            current_rows AS (
                SELECT sensor,md5('ret-backlog-current-'||sensor)::uuid AS command_id,
                       md5('ret-backlog-entity-'||sensor)::uuid AS entity_id,
                       'urn:ngsi-ld:Sensor:retention-backlog:s'||sensor AS urn,
                       clock.now_at-interval '1 minute' AS issued_at
                FROM generate_series(1,10) sensor CROSS JOIN clock
            )
            INSERT INTO twin_commands(command_id,entity_id,action,parameters,status,desired_version,
                idempotency_key,issued_by,issued_at,expires_at,control_version,requested_frequency_hz,
                stage6_managed,origin,root_command_id,original_actor_id)
            SELECT command_id,entity_id,'update_configuration','{"sample_interval_us":200000}'::jsonb,
                'pending',101,'ret-backlog-current-'||sensor,
                '11111111-1111-1111-1111-111111111111',issued_at,issued_at+interval '1 hour',
                1,'5',true,'client',command_id,'11111111-1111-1111-1111-111111111111'
            FROM current_rows;
            WITH commands AS (
                SELECT c.*,e.entity_id AS urn,e.gateway_id
                FROM twin_commands c JOIN twin_entities e ON e.id=c.entity_id
                WHERE e.gateway_id='retention-backlog'
            ),
            payloads AS (
                SELECT commands.*,
                    jsonb_build_object('protocol_version',1,'message_type','command',
                        'command_id',command_id::text,'gateway_id',gateway_id,'entity_id',urn,
                        'action',action,'parameters',parameters,'desired_version',desired_version::text,
                        'issued_at',issued_at,'expires_at',expires_at) AS body
                FROM commands
            )
            INSERT INTO twin_outbox(command_id,topic,payload,payload_bytes,status,attempt_count,
                next_attempt_at,created_at,published_at)
            SELECT command_id,'gateways/'||gateway_id||'/commands/'||command_id::text,body,
                convert_to(body::text,'UTF8'),CASE WHEN status='pending' THEN 'pending' ELSE 'failed' END,
                0,issued_at,issued_at,NULL FROM payloads;
            WITH commands AS (
                SELECT c.*,e.entity_id AS urn
                FROM twin_commands c JOIN twin_entities e ON e.id=c.entity_id
                WHERE e.gateway_id='retention-backlog' AND c.origin='client'
            ),
            receipts AS (
                SELECT commands.*,
                    CASE WHEN status='pending' THEN
                        format('["update_configuration","5","%s"]',desired_version-1)
                    ELSE format('["update_configuration","10","%s"]',desired_version-1)
                    END AS fingerprint_input,
                    convert_to(jsonb_build_object('entity_id',urn,'command_id',command_id::text,
                        'status','pending','requested_frequency_hz',
                            CASE WHEN status='pending' THEN 5 ELSE 10 END,
                        'effective_frequency_hz',CASE WHEN status='pending' THEN 5 ELSE 10 END,
                        'sample_interval_us',CASE WHEN status='pending' THEN 200000 ELSE 100000 END,
                        'desired_version',desired_version::text,'expires_at',expires_at,
                        'status_url','/v1/digital-twins/'||urn||'/commands/'||command_id::text)::text,
                        'UTF8') AS response_bytes
                FROM commands
            )
            INSERT INTO twin_command_requests(actor_id,entity_id,idempotency_key,fingerprint,
                status_code,response_bytes,command_id,created_at)
            SELECT issued_by,entity_id,idempotency_key,
                encode(sha256(convert_to(fingerprint_input,'UTF8')),'hex'),202,
                response_bytes,command_id,issued_at FROM receipts;
            ANALYZE public.twin_commands,public.twin_outbox,public.twin_command_requests,
                    public.twin_states,public.twin_entities;""")
        started = time.monotonic()
        try:
            backlog_dry = self.retention_sql()
        except Failure as error:
            elapsed = time.monotonic() - started
            print(f'RETENTION_BACKLOG_DRY baseline wall={elapsed:.3f}s outcome={error}', flush=True)
            raise Failure('Real 1,000-root retention dry-run failed its 10-second budget.') from None
        dry_elapsed = time.monotonic() - started
        if dry_elapsed >= 10 or not re.search(
                r'DRY-RUN: eligible lineages in this bounded batch =\s+100', backlog_dry):
            raise Failure(f'Real 1,000-root retention dry-run missed its 10-second/100-candidate gate: '
                          f'{dry_elapsed:.3f}s {backlog_dry[:240]}')
        print(f'RETENTION_BACKLOG_DRY wall={dry_elapsed:.3f}s candidates=100', flush=True)
        self.expect("""SELECT count(*) FROM twin_commands c JOIN twin_entities e ON e.id=c.entity_id
            WHERE e.gateway_id='retention-backlog' AND c.idempotency_key LIKE 'ret-backlog-root-%';""", '1000')
        started = time.monotonic()
        backlog_apply = self.retention_sql('apply', confirm='stage2_test')
        apply_elapsed = time.monotonic() - started
        if apply_elapsed >= 30 or not re.search(
                r'APPLY complete: lineages removed =\s+100', backlog_apply):
            raise Failure(f'Real 1,000-root apply missed its 30-second/100-lineage gate: '
                          f'{apply_elapsed:.3f}s {backlog_apply[:240]}')
        self.expect("""SELECT count(*) FROM twin_commands c JOIN twin_entities e ON e.id=c.entity_id
            WHERE e.gateway_id='retention-backlog' AND c.idempotency_key LIKE 'ret-backlog-root-%';""", '900')
        self.expect("""SELECT count(*) FROM twin_outbox o JOIN twin_commands c USING(command_id)
            JOIN twin_entities e ON e.id=c.entity_id
            WHERE e.gateway_id='retention-backlog' AND c.idempotency_key LIKE 'ret-backlog-root-%';""", '900')
        self.expect("""SELECT count(*) FROM twin_command_requests q JOIN twin_entities e ON e.id=q.entity_id
            WHERE e.gateway_id='retention-backlog'
              AND q.idempotency_key LIKE 'ret-backlog-root-%';""", '900')
        self.expect("""SELECT count(*) FROM twin_commands c JOIN twin_entities e ON e.id=c.entity_id
            WHERE e.gateway_id='retention-backlog' AND c.idempotency_key LIKE 'ret-backlog-current-%';""", '10')
        next_batch = self.retention_sql()
        if not re.search(r'DRY-RUN: eligible lineages in this bounded batch =\s+100', next_batch):
            raise Failure('Real retention backlog did not expose the next 100-lineage batch after apply.')
        print(f'RETENTION_BACKLOG_APPLY wall={apply_elapsed:.3f}s removed=100 remaining=900 '
              'next_batch=100 current=10', flush=True)

    def cleanup(self):
        errors = []
        if self.id is not None:
            try:
                owner = self.command(['docker', 'inspect', '-f', '{{index .Config.Labels "' + LABEL + '"}}', self.id])
                if owner != self.nonce:
                    errors.append('Cleanup refused incorrect ownership.')
                else:
                    self.command(['docker', 'rm', '-fv', self.id])
                    # Successful daemon query is required; command errors are NOT absence.
                    if self.command(['docker', 'container', 'ls', '-aq', '--no-trunc', '--filter', 'id=' + self.id]):
                        errors.append('Cleanup verification failed.')
                    else:
                        print('Owned compatibility cleanup verified: ' + self.id, flush=True)
                        self.id = None
            except Failure as error:
                errors.append(str(error))
        if self.init_dir is not None:
            try:
                info = self.init_dir.lstat()
                identity = (info.st_dev, info.st_ino, info.st_uid)
                if (self.init_root is None or self.init_identity is None
                        or self.init_dir.parent != self.init_root
                        or self.init_dir.is_symlink() or not self.init_dir.is_dir()
                        or identity != self.init_identity or info.st_uid != os.getuid()):
                    errors.append('Staged migration directory ownership changed; cleanup refused.')
                else:
                    shutil.rmtree(self.init_dir)
                    self.init_dir = None
                    print('Owned migration init directory cleanup verified.', flush=True)
            except OSError:
                errors.append('Owned migration init directory cleanup failed.')
        if errors:
            raise Failure('; '.join(errors))


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
        fixture.verify_retention()
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
