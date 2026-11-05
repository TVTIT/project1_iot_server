import importlib.util
import hashlib
import base64
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import time
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import Mock, patch

SPEC = importlib.util.spec_from_file_location('e4', Path(__file__).with_name('paper_p8_e4_support.py'))
e4 = importlib.util.module_from_spec(SPEC); SPEC.loader.exec_module(e4)


def sample(mode='on'):
    unique = {'status': 'COMPLETE', 'telemetry': {
        'generated_unique': 20, 'offered_unique': 20, 'application_acks': 20,
        'pending_messages': 0, 'pending_bytes': 0, 'ignored_valid_acks': 1,
        'retries': 1, 'broker_pubacks': 21},
        'e4': {'mode': 'unique', 'journal_entry_count': 20,
               'safe_header_samples': ['header-' + str(i) for i in range(20)]}}
    replay = {'status': 'COMPLETE', 'telemetry': {
        'generated_unique': 0, 'offered_unique': 0, 'replay_offered': 10,
        'replay_application_acks': 10, 'application_acks': 0,
        'pending_messages': 0, 'pending_bytes': 0, 'broker_pubacks': 10},
        'e4': {'mode': 'replay', 'journal_entry_count': 10,
               'safe_header_samples': ['header-' + str(i) for i in range(10)]}}
    cache = ({'warm_lookup_hits': 10, 'repository_calls_avoided': 10,
              'database_duplicates': 0, 'confirmed_new_rows': 0, 'copy_calls': 0,
              'transaction_commits': 0, 'attempts': 0} if mode == 'on'
             else {'database_duplicates': None, 'confirmed_new_rows': 0, 'copy_calls': 0,
                   'transaction_commits': 10, 'attempts': 10})
    def window(delta):
        before = {k: 0 for k in ('Committed', 'SamplesNew', 'DuplicateCommitted', 'ACKPublished')}
        after = {k: before[k] + delta.get(k, 0) for k in before}
        return {'available': True, 'before': before, 'after': after, 'delta': delta, 'cache': dict(cache)}
    uw, rw = window({'Committed': 20, 'SamplesNew': 2000,
        'DuplicateCommitted': (0 if mode == 'on' else 1), 'ACKPublished': 21}), window({'Committed': 0,
        'SamplesNew': 0, 'DuplicateCommitted': 10, 'ACKPublished': 10})
    fingerprints = [{'ledger': 20, 'raw': 2000, 'temporal': 20, 'configuration': 0,
                     'fingerprint': 'a' * 32}] * 2
    rw['database_proof'] = {'unique_ack': fingerprints, 'replay_end': fingerprints,
                            'exact_equal': True}
    return unique, replay, uw, rw


class E4SupportTests(unittest.TestCase):
    def test_expiry_requires_confirmed_commit_age_same_process_and_same_journal_bytes(self):
        self.assertEqual(e4.e4_expiry_evidence(
            commit_time=100.0, now=701.0, ttl_seconds=600, process_before='p1',
            process_after='p1', journal_sha_before='a' * 64, journal_sha_after='a' * 64,
            commit_confirmed=True)['status'], 'PASS')
        for changes, reason in [
            ({'now': 699.0}, 'expiry_age_unconfirmed'),
            ({'process_after': 'p2'}, 'expiry_process_changed'),
            ({'journal_sha_after': 'b' * 64}, 'expiry_journal_changed'),
            ({'commit_confirmed': False}, 'expiry_commit_unconfirmed')]:
            args = dict(commit_time=100.0, now=701.0, ttl_seconds=600,
                process_before='p1', process_after='p1', journal_sha_before='a' * 64,
                journal_sha_after='a' * 64, commit_confirmed=True)
            args.update(changes)
            self.assertIn(reason, e4.e4_expiry_evidence(**args)['failures'])

    def test_expiry_budget_rejects_short_session_and_token_before_workload(self):
        with self.assertRaisesRegex(ValueError, 'expiry_session_budget_insufficient'):
            e4.validate_e4_expiry_budget(session_seconds=700, token_seconds=1200,
                workload_seconds=200, drain_seconds=100, import_seconds=30,
                cleanup_seconds=30)
        with self.assertRaisesRegex(ValueError, 'expiry_token_lifetime_insufficient'):
            e4.validate_e4_expiry_budget(session_seconds=1400, token_seconds=650,
                workload_seconds=200, drain_seconds=100, import_seconds=30,
                cleanup_seconds=30)

    def test_expiry_manifest_is_optional_and_reuses_journal_with_production_ttl(self):
        common = {'run_id': 'e4-run', 'profile': 'official',
            'phases': {'measure_seconds': 4}, 'topology': {'gateways': 2}}
        expiry = e4.e4_phase_manifest(common, 'outputs/e4-journal.sqlite', 'expiry-replay',
            unique_count=1000, replay_count=100, official_opt_in=True)
        self.assertEqual(expiry['e4']['journal_path'], 'outputs/e4-journal.sqlite')
        self.assertEqual(expiry['e4']['replay_count'], 100)
        self.assertEqual(expiry['e4']['expiry_ttl_seconds'], 600)
        self.assertNotIn('wait_for_expiry_seconds', expiry['phases'])

    def test_e4_counts_are_explicit_and_flow_into_phase_manifests(self):
        common = {'run_id': 'e4-run', 'phases': {'measure_seconds': 4},
                  'topology': {'gateways': 2}}
        unique = e4.e4_phase_manifest(common, 'journal', 'unique', unique_count=38, replay_count=11)
        replay = e4.e4_phase_manifest(common, 'journal', 'replay', unique_count=38, replay_count=11)
        self.assertEqual(unique['e4']['max_messages'], 38)
        self.assertEqual(replay['e4']['replay_count'], 11)
        self.assertEqual(replay['e4']['max_messages'], 38)
        self.assertEqual(unique['phases']['measure_seconds'], 4)
        self.assertEqual(unique['traffic']['rate_per_second'] * 4, 38)
        self.assertEqual(replay['phases']['measure_seconds'], 2)
        self.assertEqual(replay['traffic']['rate_per_second'] * 2, 11)
        self.assertEqual(e4.validate_e4_dataset_counts(1000, 100, profile='official', official_opt_in=True),
                         {'unique_count': 1000, 'replay_count': 100})
        official = e4.e4_phase_manifest({**common, 'profile': 'official'},
            'outputs/e4-journal.sqlite', 'replay', unique_count=1000, replay_count=100,
            official_opt_in=True)
        self.assertEqual(official['e4']['max_messages'], 1000)
        self.assertEqual(official['e4']['replay_count'], 100)
        self.assertLessEqual(official['e4']['max_bytes'], 64 << 20)
        with self.assertRaisesRegex(ValueError, 'official_profile_requires_opt_in'):
            e4.validate_e4_dataset_counts(1000, 100, profile='official', official_opt_in=False)

    def test_e4_replay_validator_rejects_expected_count_mismatch(self):
        unique, replay, uw, rw = sample()
        result = e4.validate_e4_results(unique, replay, db_delta={'ledger':20,'raw':2000,'temporal':20,'configuration':0},
            scoped_delta={'available': {'a':{'ledger':10,'raw':1000,'temporal':10},'b':{'ledger':10,'raw':1000,'temporal':10}}},
            raw_checks=[{'verified_exact':True,'ledger_count':10}]*2, unique_window=uw, replay_window=rw,
            cache_mode='on', unique_count=20, replay_count=9, samples_per_message=100)
        self.assertFalse(result['valid'])
        self.assertIn('replay_replay_offered', result['failures'])

    def test_durable_a_side_sql_validators_require_committed_and_exact_raw_evidence(self):
        pending={'gateway_id':'gw1','sensor_id':'p8-e4-durable','message_id':'m1',
            'sample_count':100,'payload_sha256':'a'*64}
        committed={'gateway_id':'gw1','sensor_id':'p8-e4-durable','hash':'a'*64,
            'raw':100,'ledger_fingerprint':'b'*32}
        self.assertEqual(e4.validate_durable_fault_sql(pending=pending,committed=committed)['status'],'PASS')
        with self.assertRaisesRegex(ValueError,'p8_e4_durable_backend_commit_mismatch'):
            e4.validate_durable_fault_sql(pending=pending,committed={**committed,'raw':99})
        first=datetime(2026,1,1,tzinfo=timezone.utc)
        rows=[{'sequence':i,'measured_at':(first+timedelta(microseconds=10000*(i-7))).isoformat()}
              for i in range(7,107)]
        identity={'message_id':'m1','boot_id':'boot1','sequence':7,'interval_us':10000,
            'payload_sha256':'a'*64,'ledger_fingerprint':'b'*32,'measured_at':first.isoformat()}
        answer=e4.validate_durable_reopen_sql(before=identity,reopened={**identity,'generated':1},
            raw_rows=rows,aux_delta={'ledger':2,'raw':200,'temporal':2},
            main_before={'fingerprint':'main'},main_after={'fingerprint':'main'})
        self.assertEqual(answer['status'],'PASS')
        rows[0]['sequence']=8
        with self.assertRaisesRegex(ValueError,'p8_e4_durable_reopen_raw_rows_mismatch'):
            e4.validate_durable_reopen_sql(before=identity,reopened={**identity,'generated':1},
                raw_rows=rows,aux_delta={'ledger':2,'raw':200,'temporal':2},
                main_before={'fingerprint':'main'},main_after={'fingerprint':'changed'})

    def test_cold_manifest_is_distinct_but_reuses_replay_workload_and_journal(self):
        common = {'run_id': 'e4-run', 'phases': {'measure_seconds': 4},
                  'topology': {'gateways': 2}}
        manifest = e4.e4_phase_manifest(common, 'outputs/e4-journal.sqlite', 'cold-replay')
        self.assertEqual(manifest['run_id'], 'e4-run-cold-replay')
        self.assertEqual(manifest['e4']['mode'], 'replay')
        self.assertEqual(manifest['e4']['replay_count'], 10)
        self.assertEqual(manifest['e4']['journal_path'], 'outputs/e4-journal.sqlite')
        self.assertEqual(manifest['phases']['measure_seconds'], 2)
        with self.assertRaisesRegex(ValueError, 'manifest_phase_rejected'):
            e4.e4_phase_manifest(common, 'journal', 'warm')

    def test_cold_window_requires_new_process_empty_cache_and_duplicate_commits(self):
        before = {'process_instance': 'new', 'ram_dedup': {'entries': 0,
            'lookup_miss_total': 0, 'lookup_hit_total': 0},
            'persistence': {'begin_attempts': 0, 'confirmed_commits': 0,
                'confirmed_new_rows': 0, 'copy_calls': 0},
            'ingress': {'Committed': 0, 'SamplesNew': 0}}
        after = {'process_instance': 'new', 'ram_dedup': {'entries': 10,
            'lookup_miss_total': 10, 'lookup_hit_total': 0},
            'persistence': {'begin_attempts': 10, 'confirmed_commits': 10,
                'confirmed_new_rows': 0, 'copy_calls': 0},
            'ingress': {'Committed': 0, 'SamplesNew': 0}}
        result = e4._cold.cold_window_evidence({'process_instance': 'old'}, before, after,
            before_db=[{'fingerprint': 'a'}], after_db=[{'fingerprint': 'a'}],
            application_ack_count=10)
        self.assertEqual(result['status'], 'PASS')
        wrong = {**before, 'ram_dedup': {'entries': 2, 'lookup_miss_total': 0,
            'lookup_hit_total': 1}}
        rejected = e4._cold.cold_window_evidence({'process_instance': 'old'}, wrong, after,
            before_db=[{'fingerprint': 'a'}], after_db=[{'fingerprint': 'b'}],
            application_ack_count=9)
        self.assertEqual(rejected['status'], 'FAIL')
        self.assertIn('replacement_cache_not_empty', rejected['reasons'])
        self.assertIn('cold_lookup_hit_observed', rejected['reasons'])

    def test_cold_report_is_distinct_and_requires_all_phase_evidence(self):
        zero_ws = {'submissions': 0, 'new_events': 0, 'deliveries_enqueued': 0,
                   'telemetry_writes_completed': 0, 'drops': 0}
        cold = {'status': 'PASS', 'old_process_instance': 'old', 'process_instance': 'new',
            'cache_entries_before': 0, 'cache_miss_delta': 10, 'cache_hit_delta': 0,
            'persistence_delta': {'begin_attempts': 10, 'confirmed_commits': 10,
                'confirmed_new_rows': 0, 'copy_calls': 0}, 'application_ack_count': 10,
            'database_unchanged': True}
        phases = ['unique', 'replay', 'cold-replay']
        digest = 'a' * 64
        results = {phase: {'validation_valid': True} for phase in phases}
        windows = {'unique': {'database_delta': {'ledger': 20, 'raw': 2000,
                'temporal': 20, 'configuration': 0}, 'ws_delta': {
                'submissions': 20, 'new_events': 20, 'deliveries_enqueued': 20,
                'telemetry_writes_completed': 20, 'drops': 0}, 'journal_sha256': digest},
            'replay': {'database_delta': {'ledger': 0, 'raw': 0, 'temporal': 0,
                'configuration': 0}, 'ws_delta': zero_ws, 'journal_sha256': digest,
                'cache': {'warm_lookup_hits': 10, 'repository_calls_avoided': 10},
                'database_proof': {'exact_equal': True}},
            'cold-replay': {'database_delta': {'ledger': 0, 'raw': 0, 'temporal': 0,
                'configuration': 0}, 'ws_delta': zero_ws, 'journal_sha256': digest,
                'cold_evidence': cold}}
        prepared = {'include_cold_replay': True, 'cache_mode': 'on', 'phase_results': results,
            'phase_windows': windows, 'overall_validation': {'valid': True},
            'cold_replacement_proof': {'metrics_readback': True}, 'phase_run_ids': {},
            'arm': 'warm', 'session_id': 's', 'run_id': 'r', 'schema': '24:x',
            'quota': {}, 'effective_flags': {}, 'image_ids': {}, 'unique_database_snapshot': []}
        report = e4.finalize_remote_report(prepared,
            {'phase_history': phases, 'workload_outcome': 'VALIDATED'},
            cleanup_verified=True, cleanup_counts={})
        self.assertEqual(report['lane_marker'], 'PAPER_P8_E4_COLD_REPLAY_SUBSET')
        self.assertEqual(report['coldrestart'], 'PASS')
        self.assertEqual(report['status'], 'PASS')
        self.assertFalse(report['full_p8_accepted'])
        cold['application_ack_count'] = 9
        cold['status'] = 'FAIL'
        failed = e4.finalize_remote_report(prepared,
            {'phase_history': phases, 'workload_outcome': 'VALIDATED'},
            cleanup_verified=True, cleanup_counts={})
        self.assertNotEqual(failed['status'], 'PASS')

    def test_remote_report_source_uses_prepared_image_metadata(self):
        images = {'backend': 'sha256:backend', 'mosquitto': 'sha256:broker',
                  'gateway': 'sha256:gateway', 'unrelated': 'unused'}
        smoke = SimpleNamespace(ROOT=Path('/source'), paper_p0_git=Mock(return_value='revision'))
        fixture = object()
        with patch.object(e4._support, '_source_fingerprint', return_value='fingerprint'):
            metadata = e4.remote_report_source(smoke, fixture, {'image_ids': images})
        self.assertEqual(metadata['image_ids'], {
            key: images[key] for key in ('backend', 'mosquitto', 'gateway')})
        self.assertEqual(metadata['source_revision'], 'revision')
        self.assertEqual(metadata['source_fingerprint'], 'fingerprint')
        smoke.paper_p0_git.assert_called_once_with(fixture, 'rev-parse', 'HEAD')

    def test_captured_unique_result_shape_passes_current_phase_validator(self):
        captured = Path('/var/tmp/opencode/e4-warm-slice-20261030/attempt-02/fixture/private/incoming/unique/results/phases/unique/workloads')
        if not captured.is_dir():
            self.skipTest('captured native UNIQUE result is not present on this host')
        telemetry = json.loads((captured / 'unique/workload.json').read_text())
        viewer = json.loads((captured / 'viewer/workload.json').read_text())
        result = e4._validate_e4_phase_samples('unique', telemetry, viewer)
        self.assertTrue(result['valid'], result['failures'])
        self.assertEqual(viewer['unique_events'], 20)

    def test_phase_counters_are_required_integer_values_not_bool_or_missing(self):
        telemetry = {'experiment': 'E4', 'e4': {'mode': 'replay'}, 'telemetry': {
            'replay_offered': 10, 'replay_application_acks': 10, 'generated_unique': 0,
            'offered_unique': 0, 'application_acks': 0, 'pending_messages': 0, 'pending_bytes': 0}}
        viewer = {'status': 'COMPLETE', 'connected_distinct_viewers': 2,
            'subscribed_distinct_viewers': 2, 'receiving_distinct_viewers': 0,
            'unique_events': 0, 'identity_samples': []}
        self.assertTrue(e4._validate_e4_phase_samples('replay', telemetry, viewer)['valid'])
        del viewer['unique_events']
        self.assertIn('replay_viewer_no_new_events',
            e4._validate_e4_phase_samples('replay', telemetry, viewer)['failures'])
        viewer['unique_events'] = False
        self.assertIn('replay_viewer_no_new_events',
            e4._validate_e4_phase_samples('replay', telemetry, viewer)['failures'])
        del telemetry['telemetry']['replay_offered']
        self.assertIn('replay_telemetry_result',
            e4._validate_e4_phase_samples('replay', telemetry, viewer)['failures'])

    def test_viewer_verification_uses_real_event_totals_and_two_header_hashes(self):
        telemetry_samples = []
        headers = []
        viewer_samples = []
        for index in range(10):
            viewer_id = 'e4-viewer-a' if index % 2 == 0 else 'e4-viewer-b'
            row = {'viewer_id': '', 'gateway_id': f'gw-{index}', 'sensor_id': f'sensor-{index}',
                'message_id': f'00000000-0000-0000-0000-{index + 1:012d}', 'boot_id': f'boot-{index}',
                'first_sequence': '0', 'sample_count': 100}
            canonical = json.dumps({key: row[key] for key in
                ('gateway_id', 'sensor_id', 'message_id', 'boot_id', 'first_sequence', 'sample_count')},
                separators=(',', ':'), ensure_ascii=False).encode()
            row['header_sha256'] = hashlib.sha256(canonical).hexdigest()
            telemetry_samples.append(row)
            headers.append({'topic': f"gateways/{row['gateway_id']}/telemetry/{row['sensor_id']}",
                'gateway_id': row['gateway_id'], 'sensor_id': row['sensor_id'],
                'message_id': row['message_id'], 'boot_id': row['boot_id'],
                'first_sequence': '0', 'sample_count': 100, 'payload_sha256': 'a' * 64})
            if index < 2:
                viewer_samples.append({**row, 'viewer_id': viewer_id})
        telemetry = {'status': 'COMPLETE', 'experiment': 'E4', 'identity_samples': telemetry_samples,
            'e4': {'mode': 'unique', 'safe_header_samples': headers},
            'telemetry': {'generated_unique': 20, 'offered_unique': 20,
                'application_acks': 20, 'ignored_valid_acks': 1, 'retries': 1,
                'pending_messages': 0, 'pending_bytes': 0}}
        viewer = {'status': 'COMPLETE', 'connected_distinct_viewers': 2,
            'subscribed_distinct_viewers': 2, 'receiving_distinct_viewers': 2,
            'unique_events': 20, 'identity_samples': viewer_samples}
        self.assertTrue(e4._validate_e4_phase_samples('unique', telemetry, viewer)['valid'])
        viewer['unique_events'] = 0
        self.assertIn('unique_viewer_subscribed_and_received',
            e4._validate_e4_phase_samples('unique', telemetry, viewer)['failures'])

    def test_remote_result_import_checks_run_identity_inventory_and_digest(self):
        with tempfile.TemporaryDirectory(prefix='p8-e4-import-', dir='/var/tmp/opencode') as tmp:
            root = Path(tmp); root.chmod(0o700)
            phase = 'unique'
            rels = {
                f'results/phases/{phase}/runner/run.json': {'status': 'COMPLETE'},
                f'results/phases/{phase}/runner/provenance.json': {
                    'binary_sha256': 'a' * 64, 'binary_os': 'linux', 'binary_arch': 'amd64'},
                f'results/phases/{phase}/workloads/{phase}/workload.json': {
                    'status': 'COMPLETE', 'experiment': 'E4', 'e4': {'mode': 'unique'}},
                f'results/phases/{phase}/workloads/viewer/workload.json': {
                    'status': 'COMPLETE', 'experiment': 'E1'},
            }
            inventory = []
            for rel, payload in rels.items():
                path = root / rel; path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                data = json.dumps(payload, sort_keys=True, separators=(',', ':')).encode()
                path.write_bytes(data); path.chmod(0o600)
                inventory.append({'path': rel, 'size_bytes': len(data), 'sha256': hashlib.sha256(data).hexdigest()})
            summary = {'schema_version': 1, 'phase': phase, 'phase_run_id': 'e4-run-unique',
                'source_bundle_sha256': 'b' * 64, 'session': {'experiment': 'E4', 'session_id': 'session-1'},
                'runner_status': 'COMPLETE', 'full_p8_accepted': False, 'physical_ab': 'NOT RUN',
                'expected_workload_roles': ['unique', 'viewer'], 'present_workload_roles': ['unique', 'viewer'],
                'missing_workload_roles': [], 'files': inventory}
            summary_path = root / 'results.json'; summary_path.write_text(json.dumps(summary)); summary_path.chmod(0o600)
            got, files = e4._read_e4_result_bundle(root, phase=phase, session_id='session-1',
                run_id='e4-run-unique', bundle_sha256='b' * 64)
            self.assertEqual(got['phase'], 'unique')
            self.assertEqual(files[f'results/phases/{phase}/workloads/{phase}/workload.json']['e4']['mode'], 'unique')
            summary['session']['session_id'] = 'other-session'
            summary_path.write_text(json.dumps(summary)); summary_path.chmod(0o600)
            with self.assertRaisesRegex(ValueError, 'identity_rejected'):
                e4._read_e4_result_bundle(root, phase=phase, session_id='session-1',
                    run_id='e4-run-unique', bundle_sha256='b' * 64)

    def test_prepare_extracts_bounded_manifests_without_starting_processes(self):
        scratch = Path('/var/tmp/opencode')
        scratch.mkdir(mode=0o700, parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix='p8-e4-prepare-', dir=scratch) as tmp:
            private = Path(tmp) / 'private'
            private.mkdir(mode=0o700)
            certs = private / 'certs'; certs.mkdir(mode=0o700)
            specs = [SimpleNamespace(gateway_id='gw-a', sensor_id='sensor-a',
                         sensor_urn='urn:ngsi-ld:Sensor:gw-a:sensor-a'),
                     SimpleNamespace(gateway_id='gw-b', sensor_id='sensor-b',
                         sensor_urn='urn:ngsi-ld:Sensor:gw-b:sensor-b')]
            endpoint = SimpleNamespace(url='https://127.0.0.1:9443', ca_file=certs / 'https-ca.pem')
            fixture = SimpleNamespace(private=private, project='ncs_0123456789ab')
            registry = Mock()
            registry.resolve.return_value = {'NetworkSettings': {'Ports': {
                '8883/tcp': [{'HostIp': '127.0.0.1', 'HostPort': '18883'}]}}}

            class Smoke:
                def prepare_paper_workload(self, *args):
                    def jwt(sub):
                        enc = lambda value: base64.urlsafe_b64encode(value).decode().rstrip('=')
                        return enc(b'{"alg":"none"}') + '.' + enc(json.dumps(
                            {'sub': sub, 'exp': time.time() + 3600}).encode()) + '.sig'
                    env = {'WEBSOCKET_ENABLED': 'true', 'STAGE5_ENABLED': 'false',
                        'STAGE5_WAKEUP_ENABLED': 'false', 'MQTT_SHAPER_ENABLED': 'false',
                        'MQTT_MICROBATCH_ENABLED': 'true', 'MQTT_RAM_DEDUP_ENABLED': 'true',
                        'MQTT_MICROBATCH_MAX_MESSAGES': '32', 'MQTT_MICROBATCH_MAX_ROWS': '500',
                        'MQTT_MICROBATCH_MAX_PAYLOAD_BYTES': str(4 << 20),
                        'MQTT_MICROBATCH_MAX_WAIT': '50ms'}
                    return ('pg', {'cpus': 2}, {'services': {'backend': {'environment': env}}},
                        {'fixture': 1}, 'storage-ready', {'a': jwt('a'), 'b': jwt('b'), 'admin': 'admin'},
                        specs, ['pass-a', 'pass-b'], 'sensor')

                def db_psql(self, _registry, _pg, sql, _params=None):
                    if 'schema_migrations' in sql:
                        return '24:telemetry_ledger_timestamp'
                    if 'SELECT json_build_object' in sql:
                        return json.dumps({'ledger': 0, 'raw': 0, 'temporal': 0,
                                           'configuration': 0})
                    return 'a' * 32

                @staticmethod
                def http_request(_endpoint, method, path, *, token, body):
                    gateway, sensor = path.split('/')[3], path.split('/')[-1]
                    return 201, {}, {'entity_id': f'urn:ngsi-ld:Sensor:{gateway}:{sensor}'}

                @staticmethod
                def private_write(path, text):
                    Path(path).write_text(text, encoding='utf-8')
                    Path(path).chmod(0o600)

            containers = {'mosquitto': {'receipt': {'id': 'owned-broker'}}}
            with patch.object(e4.subprocess, 'Popen', side_effect=AssertionError('prepare launched process')):
                context = e4.prepare(Smoke(), fixture, endpoint, registry, containers,
                                     {'backend': 'image'}, object(), 'on', session_id='b' * 32)

            unique = json.loads(context['manifests'][0].read_text())
            replay = json.loads(context['manifests'][1].read_text())
            viewer = json.loads(context['viewer_manifest_path'].read_text())
            self.assertEqual(unique['e4']['mode'], 'unique')
            self.assertEqual(replay['e4']['mode'], 'replay')
            self.assertEqual(unique['e4']['journal_path'], replay['e4']['journal_path'])
            self.assertEqual(unique['e4']['max_messages'], 20)
            self.assertEqual(unique['e4']['max_bytes'], 1 << 20)
            self.assertEqual(replay['e4']['replay_count'], 10)
            self.assertEqual(unique['roles'], ['telemetry'])
            self.assertEqual(unique['targets'], [
                {'gateway_id': 'gw-a', 'sensor_id': 'sensor-a'},
                {'gateway_id': 'gw-b', 'sensor_id': 'sensor-b'}])
            self.assertEqual(unique['samples_per_message'], 100)
            self.assertEqual(unique['limits'], {'retries': 4, 'workers': 2, 'connections': 2,
                'pending_messages': 64, 'pending_bytes': 1 << 20,
                'disk_growth_bytes': 1 << 20, 'wall_seconds': 30})
            self.assertEqual(viewer['experiment'], 'E1')
            self.assertEqual(viewer['roles'], ['viewer'])
            self.assertEqual(viewer['topology']['viewers'], 2)
            self.assertNotIn('gateway_identity_files', viewer)
            self.assertEqual(context['effective_flags']['MQTT_RAM_DEDUP_ENABLED'], 'true')
            self.assertEqual(context['image_ids'], {'backend': 'image'})
            self.assertEqual(len(context['before']), 2)
            self.assertEqual(context['subscriptions'][0]['sensor_id'], 'sensor-a')

    def test_e4_remote_preparation_uses_advertised_tls_endpoints_and_exact_mappings(self):
        scratch = Path('/var/tmp/opencode')
        scratch.mkdir(mode=0o700, parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix='p8-e4-remote-prepare-', dir=scratch) as tmp:
            root = Path(tmp); private = root / 'private'; private.mkdir(mode=0o700)
            (private / 'certs').mkdir(mode=0o700)
            specs = [SimpleNamespace(gateway_id='gw-a', sensor_id='sensor-a', sensor_urn='urn:ngsi-ld:Sensor:gw-a:sensor-a'),
                     SimpleNamespace(gateway_id='gw-b', sensor_id='sensor-b', sensor_urn='urn:ngsi-ld:Sensor:gw-b:sensor-b')]
            env = {'WEBSOCKET_ENABLED': 'true', 'STAGE5_ENABLED': 'false', 'STAGE5_WAKEUP_ENABLED': 'false',
                'MQTT_SHAPER_ENABLED': 'false', 'MQTT_MICROBATCH_ENABLED': 'true',
                'MQTT_RAM_DEDUP_ENABLED': 'true', 'MQTT_MICROBATCH_MAX_MESSAGES': '32',
                'MQTT_MICROBATCH_MAX_ROWS': '500', 'MQTT_MICROBATCH_MAX_PAYLOAD_BYTES': str(4 << 20),
                'MQTT_MICROBATCH_MAX_WAIT': '50ms'}
            class Smoke:
                def __init__(self, remote_support): self.remote_support = remote_support
                def prepare_paper_workload(self, *args):
                    def jwt(sub):
                        enc = lambda value: base64.urlsafe_b64encode(value).decode().rstrip('=')
                        return enc(b'{"alg":"none"}') + '.' + enc(json.dumps(
                            {'sub': sub, 'exp': time.time() + 3600}).encode()) + '.sig'
                    return ('pg', {'quota': 1}, {'services': {'backend': {'environment': env}}}, {},
                        'ready', {'a': jwt('a'), 'b': jwt('b'), 'admin': 'admin'}, specs, ['pa', 'pb'], None)
                def db_psql(self, _r, _p, sql, _v=None):
                    return '24:telemetry_ledger_timestamp' if 'schema_migrations' in sql else json.dumps(
                        {'ledger': 0, 'raw': 0, 'temporal': 0, 'configuration': 0}) if 'SELECT json_build_object' in sql else 'a' * 32
                @staticmethod
                def http_request(_endpoint, method, path, *, token, body):
                    gateway, sensor = path.split('/')[3], path.split('/')[-1]
                    return 201, {}, {'entity_id': f'urn:ngsi-ld:Sensor:{gateway}:{sensor}'}
                def private_write(self, path, text):
                    Path(path).write_text(text, encoding='utf-8'); Path(path).chmod(0o600)
                def paper_p8_remote_support(self): return self.remote_support
            remote = Mock()
            profile = {'advertised_https': 'https://192.168.50.10:9443',
                       'advertised_mqtt_tls': 'mqtts://192.168.50.10:9883',
                       'advertised_host': '192.168.50.10', 'bind_ip': '192.168.50.10',
                       'https_port': 9443, 'mqtt_tls_port': 9883}
            broker = {'NetworkSettings': {'Ports': {'8883/tcp': [
                {'HostIp': '192.168.50.10', 'HostPort': '9883'}]}}}
            nginx = {'NetworkSettings': {'Ports': {'443/tcp': [
                {'HostIp': '192.168.50.10', 'HostPort': '9443'}]}}}
            registry = Mock()
            registry.resolve.side_effect = [broker, nginx]
            fixture = SimpleNamespace(private=private, project='ncs_0123456789ab')
            endpoint = SimpleNamespace(url='https://127.0.0.1:80', ca_file=private / 'certs/https-ca.pem')
            context = e4.prepare(Smoke(remote), fixture, endpoint, registry, {'mosquitto': 'broker', 'nginx': 'nginx'},
                {}, object(), 'on', remote_profile=profile, session_id='a' * 32)
            unique = json.loads(context['manifests'][0].read_text())
            self.assertEqual(context['mode'], 'remote')
            self.assertEqual(unique['tls']['https_endpoint'], profile['advertised_https'])
            self.assertEqual(unique['tls']['mqtt_tls_endpoint'], profile['advertised_mqtt_tls'])
            self.assertEqual(unique['tls']['mqtt_tls_server_name'], profile['advertised_host'])
            self.assertEqual(unique['run_id'], 'p8e4-' + 'a' * 32 + '-warm-unique')
            remote.validate_runtime_mapping.assert_any_call('mosquitto', broker['NetworkSettings']['Ports'], profile)
            remote.validate_runtime_mapping.assert_any_call('nginx', nginx['NetworkSettings']['Ports'], profile)
            for path in (endpoint.ca_file, private / 'certs/ca.crt'):
                path.write_text('test-ca', encoding='utf-8'); path.chmod(0o600)
            bundle = e4.export_prepared_bundle(context, output_leaf=private / 'portable-bundle')
            from scripts import paper_p8_bundle
            workspace = private / 'bound-workspace'
            paper_p8_bundle.bind_bundle(bundle_root=bundle['bundle_root'], workspace_leaf=workspace)
            bound_manifests = [json.loads(path.read_text()) for path in
                sorted((workspace / 'runtime/manifests').glob('*.json'))]
            journal_paths = {manifest['e4']['journal_path'] for manifest in bound_manifests
                             if isinstance(manifest.get('e4'), dict)}
            self.assertEqual(journal_paths, {str((workspace / 'outputs/e4-journal.sqlite').resolve())})
            self.assertNotIn(str(context['journal']), journal_paths)
            viewer = next(manifest for manifest in bound_manifests if manifest['roles'] == ['viewer'])
            self.assertEqual(viewer['experiment'], 'E1')
            from scripts import paper_p8_client
            self.assertEqual(paper_p8_client.main(['run', '--workspace', str(workspace),
                '--binary', '/bin/true']), 1)

    def test_local_runner_consumes_prepared_context_once(self):
        import inspect
        source = inspect.getsource(e4.run)
        self.assertIn('prepared = prepare(', source)
        self.assertNotIn('smoke.prepare_paper_workload(', source)
        self.assertNotIn("'unique', 'unique', 4", source)

    def test_two_viewer_manifest_is_e1_viewer_only_and_bounded(self):
        common = {'topology': {'gateways': 2}, 'durability_mode': 'synthetic-load',
                  'gateway_identity_files': {'gw': 'secret'}}
        manifest = e4.e4_viewer_manifest(common, subscriptions=[{}, {}],
                                         viewer_identity='/private/viewer.json', root='/private')
        self.assertEqual(manifest['experiment'], 'E1')
        self.assertEqual(manifest['roles'], ['viewer'])
        self.assertEqual(manifest['limits']['connections'], 2)
        self.assertEqual(manifest['limits']['workers'], 2)
        self.assertNotIn('durability_mode', manifest)
        self.assertNotIn('gateway_identity_files', manifest)
        self.assertNotIn('telemetry', manifest['identity_files'])
        with self.assertRaises(ValueError):
            e4.e4_viewer_manifest(common, subscriptions=[{}], viewer_identity='/private/v.json', root='/private')

    def test_ws_proof_uses_real_viewer_result_and_server_metric_fields(self):
        counters = {'submissions': 20, 'new_events': 20, 'deliveries_enqueued': 20,
                    'telemetry_writes_completed': 20, 'drops': 0}
        peer = {'unique_events': 10, 'duplicates': 0, 'close_reason': 'measurement_complete',
                'id': 'e4-viewer-a'}
        state = {'status': 'COMPLETE', 'connected_distinct_viewers': 2,
            'subscribed_distinct_viewers': 2, 'receiving_distinct_viewers': 2,
            'unique_events': 20, 'duplicates': 0, 'wrong_namespace': 0,
            'viewers': [peer, {**peer, 'id': 'e4-viewer-b'}]}
        good = e4.validate_e4_ws_proof(unique_delta=counters,
            replay_delta={'submissions': 0, 'new_events': 0, 'deliveries_enqueued': 0,
                'telemetry_writes_completed': 0, 'drops': 0}, state=state)
        self.assertEqual(good['status'], 'PASS')
        bad = e4.validate_e4_ws_proof(unique_delta=counters,
            replay_delta={'submissions': 1, 'new_events': 1, 'deliveries_enqueued': 0,
                'telemetry_writes_completed': 0, 'drops': 0}, state=state)
        self.assertIn('replay_ws_submissions', bad['failures'])

    def test_viewer_runtime_preflights_launches_waits_and_reads_completion(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest, output, log = root / 'viewer.json', root / 'out', root / 'viewer.log'
            manifest.write_text('{}')
            proc = Mock()
            proc.poll.return_value = None
            order = []
            with patch('scripts.paper_p8.invoke_paperload', side_effect=lambda *a, **k: order.append('preflight')) as preflight, \
                    patch('scripts.paper_p8._wait_viewer_ready', side_effect=lambda *a: order.append('ready')) as ready, \
                    patch.object(e4.subprocess, 'Popen', side_effect=lambda *a, **k: (order.append('launch'), proc)[1]) as popen:
                started = e4.start_e4_viewer(paperload_binary='/bin/true', manifest_path=manifest,
                    output_dir=output, log_path=log)
            self.assertIs(started, proc)
            preflight.assert_called_once()
            ready.assert_called_once_with(proc, output, 2, 15)
            self.assertEqual(popen.call_args.args[0][-1], '--run')
            self.assertEqual(order, ['preflight', 'launch', 'ready'])
            self.assertTrue(log.exists())
            (output).mkdir()
            (output / 'workload.json').write_text(json.dumps({'status': 'COMPLETE'}))
            proc.poll.return_value = 0; proc.returncode = 0
            self.assertEqual(e4.finish_e4_viewer(proc, output)['status'], 'COMPLETE')

    def test_viewer_readiness_error_joins_child(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = root / 'viewer.json'; manifest.write_text('{}')
            proc = Mock(); proc.poll.return_value = None
            with patch('scripts.paper_p8.invoke_paperload'), \
                    patch('scripts.paper_p8._wait_viewer_ready', side_effect=RuntimeError('no ready')), \
                    patch.object(e4.subprocess, 'Popen', return_value=proc):
                with self.assertRaisesRegex(RuntimeError, 'no ready'):
                    e4.start_e4_viewer(paperload_binary='/bin/true', manifest_path=manifest,
                        output_dir=root / 'out', log_path=root / 'viewer.log')
            proc.terminate.assert_called_once()
            proc.wait.assert_called_once_with(timeout=2)

    def validate(self, mode='on'):
        unique, replay, uw, rw = sample(mode)
        return e4.validate_e4_results(unique, replay,
            db_delta={'ledger': 20, 'raw': 2000, 'temporal': 20, 'configuration': 0},
            scoped_delta={'available': {'gw1/s1': {'ledger': 10, 'raw': 1000, 'temporal': 10},
                                        'gw2/s2': {'ledger': 10, 'raw': 1000, 'temporal': 10}}},
            raw_checks=[{'verified_exact': True, 'ledger_count': 10}] * 2,
            unique_window=uw, replay_window=rw, cache_mode=mode)

    def test_actual_runresult_shape_and_warm_replay_accounting(self):
        result = self.validate()
        self.assertTrue(result['valid'], result['failures'])

    def test_cache_off_requires_actual_database_duplicate_path(self):
        self.assertTrue(self.validate('off')['valid'])
        unique, replay, uw, rw = sample('off')
        rw['cache']['attempts'] = 0
        result = e4.validate_e4_results(unique, replay,
            db_delta={'ledger': 20, 'raw': 2000, 'temporal': 20, 'configuration': 0},
            scoped_delta={'available': {'gw1/s1': {'ledger': 10, 'raw': 1000, 'temporal': 10},
                                        'gw2/s2': {'ledger': 10, 'raw': 1000, 'temporal': 10}}},
            raw_checks=[{'verified_exact': True, 'ledger_count': 10}] * 2,
            unique_window=uw, replay_window=rw, cache_mode='off')
        self.assertIn('cache_off_database_path_unproven', result['failures'])

    def test_remote_cache_off_finish_persists_snapshot_proof_through_final_acceptance(self):
        from scripts.tests import native_compose_smoke as smoke
        unique, replay, unique_window, replay_window = sample('off')
        initial = [{'ledger': 0, 'raw': 0, 'temporal': 0, 'configuration': 0,
                    'fingerprint': '0' * 32} for _ in range(2)]
        unique_ack = replay_window['database_proof']['unique_ack']
        after = json.loads(json.dumps(unique_ack))
        unique_hash = 'a' * 64
        metrics_before = {'process_instance': 'proc', 'ingress': unique_window['before'],
                          'websocket_metrics': {'deliveries_enqueued': 20}}
        metrics_after = {'process_instance': 'proc', 'ingress': replay_window['after'],
                         'websocket_metrics': {'deliveries_enqueued': 20}}
        phase_result = {'results': {'replay': {'telemetry': replay, 'viewer': {},
                                               'validation_valid': True}}}
        with tempfile.TemporaryDirectory(dir='/var/tmp/opencode') as tmp:
            root = Path(tmp)
            incoming = root / 'incoming'; result_dir = incoming / 'replay'
            result_dir.mkdir(parents=True)
            (result_dir / 'results.json').write_text('{}')
            prepared = {
                'phase_results': {'unique': {'telemetry': unique, 'viewer': {},
                                              'validation_valid': True}},
                'phase_windows': {'unique': {'journal_sha256': unique_hash,
                    'database_delta': {'ledger': 20, 'raw': 2000, 'temporal': 20, 'configuration': 0},
                    'database_before': initial, 'database_after': unique_ack,
                    'ingress_delta': unique_window['delta'],
                    'admin_before': {'ingress': unique_window['before']},
                    'admin_after': {'ingress': unique_window['after']},
                    'ws_delta': {'submissions': 20, 'new_events': 20,
                        'deliveries_enqueued': 20, 'telemetry_writes_completed': 20, 'drops': 0},
                    'raw': [{'verified_exact': True, 'ledger_count': 10}] * 2}},
                'unique_database_snapshot': unique_ack,
                'phase_baselines': {'replay': {'database': unique_ack,
                    'admin_metrics': metrics_before}},
                'remote_collectors': {'replay': Mock()}, 'tokens': {'admin': 'token'},
                'pg': 'pg', 'specs': [SimpleNamespace(gateway_id='g1', sensor_id='s1'),
                    SimpleNamespace(gateway_id='g2', sensor_id='s2')],
                'session_id': 'session', 'phase_run_ids': {'replay': 'run'},
                'bundle_sha256': 'b' * 64, 'cache_mode': 'off',
                'arm': 'warm', 'run_id': 'run', 'schema': 'schema', 'quota': {},
                'effective_flags': {}, 'image_ids': {'backend': 'b', 'mosquitto': 'm', 'gateway': 'g'},
                'overall_validation': None}
            fixture = SimpleNamespace(private=root)
            smoke_runtime = Mock(); smoke_runtime.paper_p0_stats.return_value = {}
            telemetry_path = 'results/phases/replay/workloads/replay/workload.json'
            viewer_path = 'results/phases/replay/workloads/viewer/workload.json'
            with patch.object(e4, '_read_e4_result_bundle', return_value=(
                    {'journal_sha256': unique_hash, 'source_bundle_sha256': 'b' * 64},
                    {telemetry_path: replay, viewer_path: {}})), \
                 patch.object(e4, '_validate_e4_phase_samples', return_value={'valid': True, 'failures': []}), \
                 patch.object(e4._support, '_admin_metrics', return_value=metrics_after), \
                 patch.object(e4, '_wait_e4_ws_drain', return_value=metrics_after), \
                 patch.object(e4, '_counts', side_effect=after), \
                 patch.object(e4, '_database_delta', return_value={'ledger': 0, 'raw': 0,
                     'temporal': 0, 'configuration': 0}), \
                 patch.object(e4._support, 'calculate_ingress_delta', return_value={
                     'available': True, 'delta': replay_window['delta']}), \
                 patch.object(e4, '_e4_ws_window', return_value={'submissions': 0, 'new_events': 0,
                     'deliveries_enqueued': 0, 'telemetry_writes_completed': 0, 'drops': 0}), \
                 patch.object(e4, '_cache_counter_window', return_value=dict(replay_window['cache'])), \
                 patch.object(e4, '_replay_headers_match_manifest_prefix', return_value=True), \
                 patch.object(e4, 'validate_e4_results', return_value={
                     'valid': True, 'failures': [], 'status': 'PASS', 'cache_mode': 'off'}):
                result = e4.finish_remote_phase(smoke_runtime, fixture, 'endpoint', 'registry',
                    'containers', prepared, result_dir, phase='replay')
            saved = prepared['phase_windows']['replay']['database_proof']
            self.assertEqual(saved, {'unique_ack': unique_ack, 'replay_end': after,
                                     'exact_equal': True})
            self.assertTrue(result['validation_valid'])
            prepared['phase_windows']['replay']['ws_delta'] = {'submissions': 0, 'new_events': 0,
                'deliveries_enqueued': 0, 'telemetry_writes_completed': 0, 'drops': 0}
            session = {'phase_history': ['unique', 'replay'], 'workload_outcome': 'VALIDATED'}
            report = e4.finalize_remote_report(prepared, session,
                cleanup_verified=True, cleanup_counts={'container': 12, 'network': 1, 'volume': 5})
            self.assertEqual(report['counter_windows']['replay']['database_proof'], saved)
            self.assertEqual(report['status'], 'PASS')
            report['cleanup_verified'] = True
            self.assertTrue(smoke.paper_p8_e4_warm_subset_acceptance(report, True))
            prepared['unique_database_snapshot'] = None
            prepared['phase_results']['unique']['validation_valid'] = True
            with patch.object(e4, '_counts', side_effect=after), \
                 patch.object(e4, '_read_e4_result_bundle', return_value=(
                     {'journal_sha256': unique_hash, 'source_bundle_sha256': 'b' * 64},
                     {telemetry_path: replay, viewer_path: {}})), \
                 patch.object(e4, '_validate_e4_phase_samples', return_value={'valid': True, 'failures': []}), \
                 patch.object(e4._support, '_admin_metrics', return_value=metrics_after), \
                 patch.object(e4, '_wait_e4_ws_drain', return_value=metrics_after), \
                 patch.object(e4, '_database_delta', return_value={'ledger': 0, 'raw': 0,
                     'temporal': 0, 'configuration': 0}), \
                 patch.object(e4._support, 'calculate_ingress_delta', return_value={
                     'available': True, 'delta': replay_window['delta']}), \
                 patch.object(e4, '_e4_ws_window', return_value={'submissions': 0, 'new_events': 0,
                     'deliveries_enqueued': 0, 'telemetry_writes_completed': 0, 'drops': 0}), \
                 patch.object(e4, '_cache_counter_window', return_value=dict(replay_window['cache'])), \
                 patch.object(e4, '_replay_headers_match_manifest_prefix', return_value=True):
                e4.finish_remote_phase(smoke_runtime, fixture, 'endpoint', 'registry',
                    'containers', prepared, result_dir, phase='replay')
            self.assertFalse(prepared['phase_windows']['replay']['database_proof']['exact_equal'])
            prepared['phase_windows']['replay'].pop('database_proof')
            failed = e4.finalize_remote_report(prepared, session,
                cleanup_verified=True, cleanup_counts={'container': 12, 'network': 1, 'volume': 5})
            self.assertNotEqual(failed['status'], 'PASS')

    def test_wrong_cache_evidence_is_rejected(self):
        unique, replay, uw, rw = sample('off')
        result = e4.validate_e4_results(unique, replay,
            db_delta={'ledger': 20, 'raw': 2000, 'temporal': 20, 'configuration': 0},
            scoped_delta={'available': {'a': {'ledger': 10, 'raw': 1000, 'temporal': 10},
                                        'b': {'ledger': 10, 'raw': 1000, 'temporal': 10}}},
            raw_checks=[{'verified_exact': True, 'ledger_count': 10}] * 2,
            unique_window=uw, replay_window=rw, cache_mode='on')
        self.assertIn('cache_warm_hit_unproven', result['failures'])

    def test_missing_counters_and_counter_reset_fail_closed(self):
        unique, replay, uw, rw = sample()
        del replay['telemetry']['replay_offered']
        uw['available'] = None
        result = e4.validate_e4_results(unique, replay,
            db_delta={'ledger': 20, 'raw': 2000, 'temporal': 20, 'configuration': 0},
            scoped_delta={'available': {}}, raw_checks=[], unique_window=uw,
            replay_window=rw, cache_mode='on')
        self.assertIn('replay_replay_offered', result['failures'])
        self.assertIn('unique_counter_window_unavailable', result['failures'])

    def test_empty_snapshot_is_not_equal_database_proof(self):
        unique, replay, uw, rw = sample()
        rw['database_proof'] = {'unique_ack': {}, 'replay_end': {}, 'exact_equal': True}
        result = e4.validate_e4_results(unique, replay,
            db_delta={'ledger': 20, 'raw': 2000, 'temporal': 20, 'configuration': 0},
            scoped_delta={'available': {'a': {'ledger': 10, 'raw': 1000, 'temporal': 10},
                                        'b': {'ledger': 10, 'raw': 1000, 'temporal': 10}}},
            raw_checks=[{'verified_exact': True, 'ledger_count': 10}] * 2,
            unique_window=uw, replay_window=rw, cache_mode='on')
        self.assertIn('replay_database_fingerprint_changed', result['failures'])

    def test_cache_on_e4_runtime_lane_replaces_then_runs_cold_replay(self):
        sentinel = {'replacement': True}
        replace, replay = Mock(return_value=sentinel), Mock(return_value={'status': 'PASS'})
        with unittest.mock.patch.object(e4._cold, 'replace_owned_backend', replace), \
             unittest.mock.patch.object(e4, '_run_cold_replay', replay):
            result = e4._execute_cold_lane('smoke', 'fixture', 'endpoint', 'registry',
                'containers', 'binary', 'root', 'manifest', 'journal', 'pg', 'tokens',
                'specs', 'old-process', {'warm': True}, 'state', {})
        self.assertEqual(result, {'status': 'PASS'})
        replace.assert_called_once_with('smoke', 'fixture', 'registry', 'containers', 'state')
        self.assertEqual(replay.call_args.args[14], sentinel)

    def test_replay_headers_must_match_unique_journal_prefix(self):
        unique, replay, uw, rw = sample()
        replay['e4']['safe_header_samples'] = ['different'] * 10
        result = e4.validate_e4_results(unique, replay,
            db_delta={'ledger': 20, 'raw': 2000, 'temporal': 20, 'configuration': 0},
            scoped_delta={'available': {'a': {'ledger': 10, 'raw': 1000, 'temporal': 10},
                                        'b': {'ledger': 10, 'raw': 1000, 'temporal': 10}}},
            raw_checks=[{'verified_exact': True, 'ledger_count': 10}] * 2,
            unique_window=uw, replay_window=rw, cache_mode='on')
        self.assertIn('replay_journal_content_mismatch', result['failures'])

    def test_finish_remote_replay_checks_declared_manifest_prefix_and_keeps_proofs_strict(self):
        with tempfile.TemporaryDirectory(prefix='p8-e4-finish-', dir='/var/tmp/opencode') as tmp:
            root = Path(tmp); incoming = root / 'incoming'; result_root = incoming / 'replay'
            result_root.mkdir(parents=True); (result_root / 'results.json').write_text('{}')
            manifest = root / 'replay.json'
            manifest.write_text(json.dumps({'e4': {'replay_count': 10}}))
            unique, replay, _, _ = sample()
            unique['e4']['safe_header_samples'] = [{'id': i} for i in range(20)]
            replay['e4']['safe_header_samples'] = [{'id': i} for i in range(10)]
            telemetry = {'e4': replay['e4']}
            viewer = {}
            snapshot = [{'ledger': 20, 'raw': 2000, 'temporal': 20,
                         'configuration': 0, 'fingerprint': 'a' * 32}]
            metrics = {'websocket_metrics': {}, 'process_instance': 'same'}
            prepared = {'phase_baselines': {'replay': {'admin_metrics': metrics,
                'database': snapshot, 'server': {}, 'ws_process_instance': 'same'}},
                'remote_collectors': {'replay': Mock()}, 'manifests': [manifest, manifest],
                'session_id': 's', 'phase_run_ids': {'replay': 'r'}, 'bundle_sha256': 'b' * 64,
                'cache_mode': 'on', 'tokens': {'admin': 'admin'},
                'specs': [SimpleNamespace(gateway_id='gw', sensor_id='sensor')], 'pg': 'pg',
                'phase_results': {'unique': {'telemetry': unique, 'validation_valid': True}},
                'phase_windows': {'unique': {'journal_sha256': 'c' * 64}},
                'unique_database_snapshot': snapshot}
            fixture = SimpleNamespace(private=root)
            smoke = Mock()
            smoke._unused = None
            with patch.object(e4, '_read_e4_result_bundle', return_value=(
                    {'journal_sha256': 'c' * 64, 'source_bundle_sha256': 'b' * 64},
                    {'results/phases/replay/workloads/replay/workload.json': telemetry,
                     'results/phases/replay/workloads/viewer/workload.json': viewer})), \
                 patch.object(e4, '_validate_e4_phase_samples', return_value={'valid': True, 'failures': []}), \
                 patch.object(e4._support, '_admin_metrics', return_value=metrics), \
                 patch.object(e4, '_wait_e4_ws_drain', return_value=metrics), \
                 patch.object(e4, '_counts', return_value=snapshot[0]), \
                 patch.object(e4, '_database_delta', return_value={'ledger': 0, 'raw': 0, 'temporal': 0, 'configuration': 0}), \
                 patch.object(e4._support, 'calculate_ingress_delta', return_value={'available': True,
                     'delta': {'Committed': 0, 'SamplesNew': 0, 'DuplicateCommitted': 10, 'ACKPublished': 10}}), \
                 patch.object(e4, '_cache_counter_window', return_value={'warm_lookup_hits': 10,
                     'repository_calls_avoided': 10}), \
                 patch.object(e4, '_e4_ws_window', return_value={'submissions': 0, 'new_events': 0,
                     'deliveries_enqueued': 0, 'telemetry_writes_completed': 0, 'drops': 0}), \
                 patch.object(e4, 'sensor_scoped_delta', return_value={}), \
                 patch.object(e4, 'validate_e4_results', return_value={'valid': True, 'failures': []}):
                answer = e4.finish_remote_phase(smoke, fixture, 'endpoint', 'registry', {}, prepared,
                                                result_root, phase='replay')
                self.assertTrue(answer['validation_valid'], answer['validation_failures'])
                # Changed journal proof still fails, despite matching prefix and unchanged DB.
                prepared['phase_windows']['unique']['journal_sha256'] = 'd' * 64
                answer = e4.finish_remote_phase(smoke, fixture, 'endpoint', 'registry', {}, prepared,
                                                result_root, phase='replay')
                self.assertIn('replay_journal_continuity_mismatch', answer['validation_failures'])
                prepared['phase_windows']['unique']['journal_sha256'] = 'c' * 64
                prepared['unique_database_snapshot'] = [{**snapshot[0], 'fingerprint': 'd' * 32}]
                answer = e4.finish_remote_phase(smoke, fixture, 'endpoint', 'registry', {}, prepared,
                                                result_root, phase='replay')
                self.assertIn('replay_unique_snapshot_mismatch', answer['validation_failures'])

    def test_declared_replay_prefix_rejects_count_order_and_foreign_headers(self):
        with tempfile.TemporaryDirectory(prefix='p8-e4-prefix-', dir='/var/tmp/opencode') as tmp:
            manifest = Path(tmp) / 'replay.json'
            manifest.write_text(json.dumps({'e4': {'replay_count': 10}}))
            prepared = {'manifests': [manifest, manifest]}
            full = {'e4': {'safe_header_samples': [{'id': i} for i in range(20)]}}
            prefix = {'e4': {'safe_header_samples': [{'id': i} for i in range(10)]}}
            self.assertTrue(e4._replay_headers_match_manifest_prefix(prepared, full, prefix))
            prefix['e4']['safe_header_samples'][0] = {'id': 'foreign'}
            self.assertFalse(e4._replay_headers_match_manifest_prefix(prepared, full, prefix))
            prefix['e4']['safe_header_samples'] = [{'id': i} for i in range(9)]
            self.assertFalse(e4._replay_headers_match_manifest_prefix(prepared, full, prefix))
            manifest.write_text(json.dumps({'e4': {'replay_count': True}}))
            self.assertFalse(e4._replay_headers_match_manifest_prefix(prepared, full, prefix))

    def test_cache_on_accepts_zero_unique_db_duplicates_but_requires_replay_no_db_work(self):
        unique, replay, uw, rw = sample()
        uw['delta']['DuplicateCommitted'] = 0
        self.assertTrue(self.validate('on')['valid'])
        rw['cache']['copy_calls'] = 1
        result = e4.validate_e4_results(unique, replay,
            db_delta={'ledger': 20, 'raw': 2000, 'temporal': 20, 'configuration': 0},
            scoped_delta={'available': {'a': {'ledger': 10, 'raw': 1000, 'temporal': 10},
                                        'b': {'ledger': 10, 'raw': 1000, 'temporal': 10}}},
            raw_checks=[{'verified_exact': True, 'ledger_count': 10}] * 2,
            unique_window=uw, replay_window=rw, cache_mode='on')
        self.assertIn('cache_on_database_work_unexpected', result['failures'])

    def test_effective_flags_require_boolean_strings_and_normalize_ram_mode(self):
        env = {'WEBSOCKET_ENABLED': 'true', 'STAGE5_ENABLED': 'false',
               'STAGE5_WAKEUP_ENABLED': 'false', 'MQTT_SHAPER_ENABLED': 'false',
               'MQTT_MICROBATCH_ENABLED': 'true', 'MQTT_RAM_DEDUP_ENABLED': 'true',
               'MQTT_MICROBATCH_MAX_MESSAGES': '32', 'MQTT_MICROBATCH_MAX_ROWS': '500',
               'MQTT_MICROBATCH_MAX_PAYLOAD_BYTES': str(4 << 20), 'MQTT_MICROBATCH_MAX_WAIT': '50ms'}
        self.assertEqual(e4._flag_profile({'services': {'backend': {'environment': env}}}, 'on'), env)
        env.pop('STAGE5_ENABLED')
        with self.assertRaises(ValueError):
            e4._flag_profile({'services': {'backend': {'environment': env}}}, 'on')

    def test_actual_admin_toplevel_groups_and_counter_reset(self):
        before = {'process_instance': 'backend-1', 'ram_dedup': {'lookup_hit_total': 4, 'lookup_miss_total': 2,
                                'database_calls_avoided_total': 3},
                  'persistence': {'begin_attempts': 8, 'confirmed_commits': 5,
                                  'copy_calls': 2, 'confirmed_new_rows': 200}}
        after = {'process_instance': 'backend-1', 'ram_dedup': {'lookup_hit_total': 7, 'lookup_miss_total': 3,
                               'database_calls_avoided_total': 6},
                 'persistence': {'begin_attempts': 8, 'confirmed_commits': 5,
                                 'copy_calls': 2, 'confirmed_new_rows': 200}}
        window = e4._cache_counter_window(before, after)
        self.assertEqual(window['warm_lookup_hits'], 3)
        self.assertEqual(window['repository_calls_avoided'], 3)
        self.assertEqual(window['attempts'], 0)
        after['ram_dedup']['lookup_hit_total'] = 1
        self.assertIsNone(e4._cache_counter_window(before, after)['warm_lookup_hits'])
        after['process_instance'] = 'backend-2'
        self.assertIsNone(e4._cache_counter_window(before, after))
        self.assertIsNone(e4._cache_counter_window(before, {}) )

    def test_counts_with_fingerprint_only_delta_numeric_database_counts(self):
        before = [{'ledger': 0, 'raw': 0, 'temporal': 0, 'configuration': 0, 'fingerprint': 'a' * 32}]
        after = [{'ledger': 20, 'raw': 2000, 'temporal': 20, 'configuration': 0, 'fingerprint': 'b' * 32}]
        self.assertEqual(e4._database_delta(before, after),
                         {'ledger': 20, 'raw': 2000, 'temporal': 20, 'configuration': 0})

    def test_collector_stop_error_does_not_skip_process_stop(self):
        collector = Mock()
        collector.stop.side_effect = RuntimeError('scrape close failed')
        proc = Mock(pid=123)
        proc.poll.return_value = None
        error = e4._stop_collector_and_process(collector, proc)
        self.assertIsInstance(error, RuntimeError)
        proc.terminate.assert_called_once()
        proc.wait.assert_called_once_with(timeout=2)

    def test_workload_and_cleanup_failure_are_not_accepted(self):
        unique, replay, uw, rw = sample()
        unique['cleanup_errors'] = ['mqtt_transport_stop_failed']
        failed = e4.validate_e4_results(unique, replay,
            db_delta={'ledger': 20, 'raw': 2000, 'temporal': 20, 'configuration': 0},
            scoped_delta={'available': {'gw1/s1': {'ledger': 10, 'raw': 1000, 'temporal': 10},
                                        'gw2/s2': {'ledger': 10, 'raw': 1000, 'temporal': 10}}},
            raw_checks=[{'verified_exact': True, 'ledger_count': 10}] * 2,
            unique_window=uw, replay_window=rw, cache_mode='on')
        self.assertIn('unique_cleanup_failure', failed['failures'])
        unique['cleanup_errors'] = []
        unique['status'] = 'PARTIAL'
        failed = e4.validate_e4_results(unique, replay,
            db_delta={'ledger': 20, 'raw': 2000, 'temporal': 20, 'configuration': 0},
            scoped_delta={'available': {'gw1/s1': {'ledger': 10, 'raw': 1000, 'temporal': 10},
                                        'gw2/s2': {'ledger': 10, 'raw': 1000, 'temporal': 10}}},
            raw_checks=[{'verified_exact': True, 'ledger_count': 10}] * 2,
            unique_window=uw, replay_window=rw, cache_mode='on')
        self.assertIn('unique_result_incomplete', failed['failures'])

    def test_conflict_requires_schema_guard_backend_reason_zero_ack_and_unchanged_db(self):
        good = e4.validate_representative_conflict(schema_valid=True, guard_precondition=True,
            rejection_reason='sequence_overlap', expected_reason='sequence_overlap', ack_count=0,
            before_db={'fingerprint': 'a'}, after_db={'fingerprint': 'a'})
        self.assertEqual(good['status'], 'PASS')
        missing_guard = e4.validate_representative_conflict(schema_valid=True,
            guard_precondition=False, rejection_reason=None, expected_reason='sequence_overlap',
            ack_count=0, before_db={'fingerprint': 'a'}, after_db={'fingerprint': 'a'})
        self.assertIn('conflict_guard_precondition_missing', missing_guard['failures'])
        self.assertIn('conflict_backend_rejection_unproven', missing_guard['failures'])
        ack_only = e4.validate_representative_conflict(schema_valid=True,
            guard_precondition=True, rejection_reason=None, expected_reason='sequence_overlap',
            ack_count=0, before_db={'fingerprint': 'a'}, after_db={'fingerprint': 'a'})
        self.assertNotEqual(ack_only['status'], 'PASS')

    def test_representative_conflicts_publish_real_journal_bytes_and_stop_observer(self):
        from types import SimpleNamespace
        from datetime import datetime, timezone
        from unittest.mock import Mock

        import paper_p5a_support as p5a
        message_id = 'a6f9a8e0-c223-4bce-9aab-1ebf62901fb3'
        payload = json.dumps({'protocol_version': 1, 'message_id': message_id,
            'message_type': 'telemetry_batch', 'gateway_id': 'gw-a', 'sensor_id': 'sensor-a',
            'boot_id': '2c285db6-eef6-4ca0-b613-9f1663f1d95a', 'first_sequence': 42,
            'sample_count': 100, 'measured_at': '2026-10-26T10:00:00Z',
            'sample_interval_us': 10000, 'samples': [21.5 + i / 100 for i in range(100)]},
            separators=(',', ':')).encode()
        with tempfile.TemporaryDirectory() as tmp:
            journal = Path(tmp) / 'dataset.jsonl'
            journal.write_text(json.dumps({'gateway_id': 'gw-a', 'sensor_id': 'sensor-a',
                'message_id': message_id, 'first_sequence': '42', 'sample_count': 100,
                'payload_sha256': hashlib.sha256(payload).hexdigest(),
                'payload': base64.b64encode(payload).decode()}) + '\n')
            spec = SimpleNamespace(gateway_id='gw-a', sensor_id='sensor-a', sensor_urn='urn:gw-a:sensor-a')
            fixture = SimpleNamespace(private=Path(tmp))
            registry = Mock()
            registry.command.side_effect = [
                SimpleNamespace(returncode=0, stdout=b'', stderr=b''),
                SimpleNamespace(returncode=0, stdout=b'', stderr=b''),
                SimpleNamespace(returncode=0, stdout=b'', stderr=b''),
                SimpleNamespace(returncode=0, stdout=b'reason_code=message_conflict reason_code=sequence_overlap', stderr=b''),
            ] * 2
            smoke = Mock()
            smoke.db_psql.side_effect = lambda *args: (
                't' if 'SELECT EXISTS' in args[2] else
                json.dumps({'ledger': 1, 'raw': 100, 'temporal': 1, 'configuration': 0})
                if 'SELECT json_build_object' in args[2] else 'a' * 32)
            ack = json.dumps({'protocol_version': 1, 'message_type': 'telemetry_ack',
                'message_id': message_id, 'status': 'committed',
                'committed_at': datetime.now(timezone.utc).isoformat()}).encode()
            observer_log = Path(tmp) / 'observer.log'
            observer_log.write_bytes(b'')
            observer = (Mock(), Mock(), observer_log, None)
            observer[0].poll.return_value = None
            smoke.paper_p1_subscriber.return_value = observer
            smoke.paper_p1_capture_subscription.side_effect = [ack, None, ack, None]
            def observed_test_messages(_data):
                calls = smoke.paper_p1_capture_subscription.call_args_list[-2:]
                base = len(smoke.paper_p1_capture_subscription.call_args_list) - 2
                return [(call.args[1], b'') for index, call in enumerate(calls, base)
                        if call.args[1].startswith('gateways/gw-a/acks/') and
                           [ack, None, ack, None][index] is not None]
            smoke.paper_p1_subscription_messages.side_effect = observed_test_messages
            smoke.paper_p1_stop_subscription.return_value = None
            containers = {'mosquitto': {'receipt': {'id': 'broker-id'}},
                          'backend': {'receipt': {'id': 'backend-id'}}}
            with patch.object(p5a, 'conflict_payloads', wraps=p5a.conflict_payloads):
                result = e4.run_representative_conflicts(smoke, fixture, registry, containers,
                    'pg-id', [spec], ['gateway-secret'], journal)
        self.assertEqual(result['status'], 'PASS')
        self.assertEqual(set(result['cases']), {'same_id_changed_payload', 'different_first_sequence_overlap'})
        self.assertEqual(smoke.paper_p1_subscriber.call_count, 2)
        self.assertEqual(smoke.paper_p1_stop_subscription.call_count, 2)
        calls = smoke.paper_p1_capture_subscription.call_args_list
        self.assertEqual([call.kwargs.get('after_count') for call in calls], [None, 1, None, 0])
        publish_calls = [call for call in registry.command.call_args_list
                         if call.args[0][0] == 'exec' and call.kwargs.get('input')]
        self.assertEqual(len(publish_calls), 4)
        self.assertTrue(publish_calls[0].kwargs['input'].endswith(payload))
        self.assertTrue(publish_calls[2].kwargs['input'].endswith(payload))

    def test_representative_conflict_missing_backend_reason_fails_even_without_ack(self):
        result = e4.validate_representative_conflict(schema_valid=True, guard_precondition=True,
            rejection_reason=None, expected_reason='sequence_overlap', ack_count=0,
            before_db={'fingerprint': 'a' * 32}, after_db={'fingerprint': 'a' * 32})
        self.assertEqual(result['status'], 'FAIL')
        self.assertIn('conflict_backend_rejection_unproven', result['failures'])

    def test_durable_ignore_and_reopen_requires_exact_pending_identity_and_join(self):
        before = {'payload_sha256': 'abc', 'payload_bytes': 12, 'header': {'v': 1},
                  'pending_messages': 1, 'ack_deleted': 0, 'message_id': 'id',
                  'sequence': 7, 'interval_us': 10000}
        replay = {'status': 'COMPLETE', 'old_payload_sha256': 'abc', 'old_message_id': 'id',
                  'old_sequence': 7, 'old_interval_us': 10000, 'old_boot_id': 'boot',
                  'old_raw_count': 100, 'old_sample_count': 100,
                  'old_ledger_fingerprint': 'fp', 'generated': 1,
                  'application_acks': 2}
        before['header']['boot_id'] = 'boot'; before['ledger_fingerprint'] = 'fp'
        args = dict(intentional_stop=True, ignored_valid_acks=1, ack_deleted=0, before=before,
                    committed_payload_sha256='abc', replay=replay, final_pending=0,
                    final_quarantine=0, joined=True,
                    main_dataset_counts={'unchanged': True, 'before': {'fingerprint': 'x'}, 'after': {'fingerprint': 'x'}})
        result = e4.validate_durable_reopen(**args)
        self.assertEqual(result['status'], 'PASS')
        self.assertEqual(result['extra_generated'], 1)
        self.assertEqual(result['extra_acks'], 2)
        args['joined'] = False
        self.assertIn('durable_reopen_process_not_joined', e4.validate_durable_reopen(**args)['failures'])
        args['joined'] = True
        args['replay'] = dict(replay, old_sequence=8)
        self.assertIn('durable_replay_identity_changed', e4.validate_durable_reopen(**args)['failures'])

    def run_probe(self, *, mismatch=None, unexpected_fault=False, timeout=False):
        from types import SimpleNamespace
        message_id = 'a6f9a8e0-c223-4bce-9aab-1ebf62901fb3'
        payload = json.dumps({'protocol_version': 1, 'message_id': message_id, 'message_type': 'telemetry_batch',
            'gateway_id': 'gw', 'sensor_id': 'p8-e4-durable', 'boot_id': 'boot-a',
            'first_sequence': 11, 'sample_count': 100, 'measured_at': '2026-10-28T00:00:00+00:00',
            'sample_interval_us': 10000, 'samples': list(range(100))}, separators=(',', ':')).encode()
        digest = hashlib.sha256(payload).hexdigest()
        identity = {'message_id':message_id,'hash': digest, 'ledger_fingerprint': 'ledger-fp', 'raw': 100,
                    'boot': 'boot-a', 'first_sequence': 11, 'interval_us': 10000,
                    'sample_count': 100, 'gateway_id':'gw','sensor_id':'p8-e4-durable',
                    'first_timestamp':'2026-10-28T00:00:00+00:00'}
        if mismatch:
            identity[mismatch] = 99 if isinstance(identity[mismatch], int) else 'wrong'
        aux_reads = iter(({'ledger': 0, 'raw': 0, 'temporal': 0},
                          {'ledger': 2, 'raw': 200, 'temporal': 2}))

        class Smoke:
            ROOT = Path.cwd()
            def http_request(self, *args, **kwargs):
                return 201, {}, {'entity_id': 'urn:ngsi-ld:Sensor:gw:p8-e4-durable'}
            def private_write(self, path, text): Path(path).write_text(text)
            def process_env(self, fixture): return {}
            def db_psql(self, registry, pg, sql, params):
                if "'ledger',(SELECT count(*)" in sql:
                    return json.dumps(next(aux_reads))
                if "'last_sequence'" in sql:
                    return json.dumps({'message_id':message_id,'hash': digest, 'ledger_fingerprint': 'ledger-fp',
                        'raw': identity.get('raw'), 'boot': 'boot-a', 'first_sequence': 11,
                        'last_sequence': 110, 'sample_count': 100,
                        'first_timestamp':'2026-10-28T00:00:00+00:00'})
                if "'ledger_fingerprint'" in sql and 'p.payload_hash' in sql:
                    return json.dumps(identity)
                if 'json_agg(json_build_object' in sql:
                    from datetime import datetime, timedelta, timezone
                    start = datetime(2026, 10, 28, tzinfo=timezone.utc)
                    return json.dumps([{'sequence': 11 + i,
                        'measured_at': (start + timedelta(microseconds=10000 * i)).isoformat()}
                        for i in range(100)])
                return json.dumps({'hash': digest, 'ledger_fingerprint': 'ledger-fp', 'raw': 100,
                    'gateway_id': 'gw', 'sensor_id': 'p8-e4-durable',
                    'boot': 'boot-a', 'first_sequence': 11, 'last_sequence': 110, 'sample_count': 100})

        class Proc:
            calls = 0
            instances = []
            def __init__(self, args, **kwargs):
                self.assert_env = kwargs['env']
                if self.assert_env.get('GATEWAY_CONTROL_ENABLED') != 'false' or 'CONTROL_ENABLED' in self.assert_env:
                    raise AssertionError('wrong gateway control environment key')
                type(self).calls += 1; self.phase = 'fault' if type(self).calls == 1 else 'reopen'
                self.stdout = kwargs['stdout']; self.returncode = None
                self.wait_calls = 0
                type(self).instances.append(self)
                self.dbfile = Path(root) / 'durable.sqlite'
                if self.phase == 'fault':
                    db = sqlite3.connect(self.dbfile)
                    db.execute('CREATE TABLE outbox (message_id TEXT,boot_id TEXT,gateway_id TEXT,sensor_id TEXT,first_sequence INTEGER,sample_interval_us INTEGER,sample_count INTEGER,payload BLOB,byte_size INTEGER,status TEXT)')
                    db.execute('INSERT INTO outbox VALUES (?,?,?,?,?,?,?,?,?,?)',
                        (message_id,'boot-a','gw','p8-e4-durable',11,10000,100,payload,len(payload),'pending'))
                    db.commit(); db.close()
                self.pid = 7
            def wait(self, timeout=None):
                self.wait_calls += 1
                if self.phase == 'fault' and os.environ.get('_durable_unexpected') == 'True':
                    self.returncode = 8
                    self.stdout.write(b'SUMMARY fault\n'); return 8
                if timeout and os.environ.get('_durable_timeout') == 'True' and self.returncode is None:
                    raise __import__('subprocess').TimeoutExpired('gateway', timeout)
                if self.phase == 'reopen':
                    db = sqlite3.connect(self.dbfile); db.execute("UPDATE outbox SET status='acked'"); db.commit(); db.close()
                self.returncode = 1 if self.phase == 'fault' else 0
                self.stdout.write(('SUMMARY ' + self.phase + '\n').encode()); self.stdout.flush()
                return self.returncode
            def poll(self): return self.returncode
            def terminate(self): self.returncode = -15
            def kill(self): self.returncode = -9

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            smoke = Smoke()
            fixture = SimpleNamespace(private=root / 'private')
            fixture.private.mkdir()
            spec = SimpleNamespace(gateway_id='gw', sensor_unit='C')
            summaries = {'fault': {'intentional_fault_stop': 'true', 'ignored_valid_acks': '1',
                'completed': 'false', 'app_acknowledged': '0', 'pending_rows': '1'},
                'reopen': {'completed': 'true', 'enqueued': '1', 'app_acknowledged': '2'}}
            module = SimpleNamespace(gateway_env=lambda *a: {}, parse_run_summary=lambda text: summaries['fault' if 'fault' in text else 'reopen'])
            with patch.dict('sys.modules', {'stage3_uplink_runner': module}), \
                 patch.object(e4.subprocess, 'Popen', Proc), \
                 patch.dict('os.environ', {'_durable_unexpected': str(unexpected_fault), '_durable_timeout': str(timeout)}):
                if unexpected_fault:
                    with self.assertRaisesRegex(ValueError, 'intentional_stop_unproven'):
                        e4.run_durable_sqlite_probe(smoke, fixture, 'endpoint', 'registry', 'pg', spec,
                            'password', 'token', 'gateway', root, lambda: {'fingerprint': 'main'}, mqtt_endpoint='mqtt')
                    return None
                if timeout:
                    with self.assertRaisesRegex(ValueError, 'fault_timeout'):
                        e4.run_durable_sqlite_probe(smoke, fixture, 'endpoint', 'registry', 'pg', spec,
                            'password', 'token', 'gateway', root, lambda: {'fingerprint': 'main'}, mqtt_endpoint='mqtt')
                    self.assertGreaterEqual(Proc.instances[0].wait_calls, 2)
                    self.assertIsNotNone(Proc.instances[0].poll())
                    return None
                return e4.run_durable_sqlite_probe(smoke, fixture, 'endpoint', 'registry', 'pg', spec,
                    'password', 'token', 'gateway', root, lambda: {'fingerprint': 'main'}, mqtt_endpoint='mqtt')

    def test_durable_runtime_probe_orders_both_processes_and_reads_back_after_reopen(self):
        result = self.run_probe()
        self.assertEqual(result['status'], 'PASS', result['failures'])
        self.assertEqual(result['process_join_states'], [True, True])
        self.assertEqual(result['aux_dataset_counts']['delta'], {'ledger': 2, 'raw': 200, 'temporal': 2})

    def test_durable_runtime_probe_refuses_unexpected_nonzero_fault_exit(self):
        self.run_probe(unexpected_fault=True)

    def test_durable_runtime_probe_refuses_post_reopen_hash_or_raw_mismatch(self):
        with self.assertRaisesRegex(ValueError, 'post_reopen_backend_readback_mismatch'):
            self.run_probe(mismatch='raw')

    def test_durable_runtime_probe_timeout_joins_child(self):
        self.run_probe(timeout=True)

    def test_extract_e4_journal_first_record_and_generate_conflicts(self):
        with tempfile.TemporaryDirectory() as tmp:
            journal_path = Path(tmp) / 'journal.jsonl'
            mid = '550e8400-e29b-41d4-a716-446655440000'
            payload_dict = {
                'protocol_version': 1, 'message_id': mid, 'message_type': 'telemetry_batch',
                'gateway_id': 'gw-1', 'sensor_id': 'sensor-1', 'boot_id': 'boot-1',
                'first_sequence': 10, 'sample_count': 100, 'measured_at': '2026-10-28T00:00:00Z',
                'sample_interval_us': 10000, 'samples': [float(i) for i in range(100)]
            }
            raw_payload = json.dumps(payload_dict, separators=(',', ':')).encode()
            digest = hashlib.sha256(raw_payload).hexdigest()
            record = {
                'topic': 'gateways/gw-1/telemetry/sensor-1',
                'gateway_id': 'gw-1',
                'sensor_id': 'sensor-1',
                'message_id': mid,
                'boot_id': 'boot-1',
                'first_sequence': '10',
                'sample_count': 100,
                'payload_sha256': digest,
                'payload': base64.b64encode(raw_payload).decode('ascii')
            }
            journal_path.write_text(json.dumps(record) + '\n', encoding='utf-8')
            header, extracted_payload = e4.extract_e4_journal_first_record(journal_path)
            self.assertEqual(header['message_id'], mid)
            self.assertEqual(extracted_payload, raw_payload)

            cases = e4.generate_e4_conflict_payloads(extracted_payload, gateway_id='gw-1', sensor_id='sensor-1')
            self.assertEqual(set(cases), {'same_id_changed_payload', 'different_first_sequence_overlap'})

    def test_begin_and_finish_remote_conflicts_phase_lifecycle(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fixture = SimpleNamespace(private=root / 'private')
            fixture.private.mkdir(mode=0o700, parents=True)
            incoming = fixture.private / 'incoming'
            incoming.mkdir(mode=0o700, parents=True)
            probe_dir = incoming / 'conflict-probe'
            probe_dir.mkdir(mode=0o700, parents=True)

            mid = '550e8400-e29b-41d4-a716-446655440000'
            payload_dict = {
                'protocol_version': 1, 'message_id': mid, 'message_type': 'telemetry_batch',
                'gateway_id': 'gw-1', 'sensor_id': 'sensor-1', 'boot_id': 'boot-1',
                'first_sequence': 10, 'sample_count': 100, 'measured_at': '2026-10-28T00:00:00Z',
                'sample_interval_us': 10000, 'samples': [float(i) for i in range(100)]
            }
            raw_payload = json.dumps(payload_dict, separators=(',', ':')).encode()
            cases = e4.generate_e4_conflict_payloads(raw_payload, gateway_id='gw-1', sensor_id='sensor-1')

            probe_data = {
                'schema_version': 1, 'experiment': 'E4', 'phase': 'conflicts',
                'session_id': 'a' * 32, 'run_id': 'run-1',
                'original_header': {'gateway_id': 'gw-1', 'sensor_id': 'sensor-1', 'message_id': mid,
                                    'boot_id': 'boot-1', 'first_sequence': '10', 'sample_count': 100,
                                    'payload_sha256': hashlib.sha256(raw_payload).hexdigest()},
                'original_payload_sha256': hashlib.sha256(raw_payload).hexdigest(),
                'original_payload_b64': base64.b64encode(raw_payload).decode(),
                'cases': {
                    name: {
                        'message_id': json.loads(case_bytes)['message_id'],
                        'payload_sha256': hashlib.sha256(case_bytes).hexdigest(),
                        'raw_payload_b64': base64.b64encode(case_bytes).decode()
                    }
                    for name, case_bytes in cases.items()
                }
            }
            probe_bytes = json.dumps(probe_data, sort_keys=True, separators=(',', ':')).encode()
            (probe_dir / 'conflict-probe.json').write_bytes(probe_bytes)
            bundle_binding = {'source_bundle_sha256': 'bundle-hash', 'source_inventory': [],
                              'session': {'session_id': 'a' * 32, 'run_id': 'run-1'}}
            probe_summary = {'schema_version': 1, 'experiment': 'E4', 'phase': 'conflicts',
                'session_id': 'a' * 32, 'run_id': 'run-1', 'source_bundle_sha256': 'bundle-hash',
                'source_inventory': [], 'binding': bundle_binding,
                'binding_sha256': hashlib.sha256(json.dumps(bundle_binding, sort_keys=True,
                    separators=(',', ':')).encode()).hexdigest(),
                'proof': {'path': 'conflict-probe.json', 'size_bytes': len(probe_bytes),
                    'sha256': hashlib.sha256(probe_bytes).hexdigest()},
                'full_p8_accepted': False, 'physical_ab': 'NOT RUN'}
            (probe_dir / 'results.json').write_text(json.dumps(probe_summary))
            for path in probe_dir.iterdir(): path.chmod(0o600)

            spec = SimpleNamespace(gateway_id='gw-1', sensor_id='sensor-1', sensor_urn='urn:s1')
            prepared = {
                'specs': [spec],
                'pg': 'pg-info',
                'bundle_sha256': 'bundle-hash',
                'bundle_inventory': [],
                'bundle_session': {'session_id': 'a' * 32},
                'session_id': 'a' * 32,
                'bundle_session': bundle_binding['session'],
                'phase_results': {'unique': {'telemetry': {'e4': {'safe_header_samples':
                    [probe_data['original_header']]}}}},
                'run_id': 'run-1',
                'containers': {'backend': {'receipt': {'id': 'backend-id'}}}
            }

            class MockSmoke:
                def db_psql(self, registry, pg, query, params=None):
                    if 'SELECT EXISTS' in query:
                        return 't'
                    if 'json_build_object' in query:
                        return json.dumps({'ledger': 10, 'raw': 1000, 'temporal': 10, 'configuration': 0})
                    if 'SELECT md5' in query:
                        return 'd' * 32
                    return 't'

            class MockRegistry:
                def command(self, args):
                    return SimpleNamespace(returncode=0, stdout=b'reason_code=message_conflict\nreason_code=sequence_overlap\n', stderr=b'')

            smoke = MockSmoke()
            registry = MockRegistry()

            (probe_dir / 'results.json').write_text(json.dumps(dict(probe_summary, session_id='wrong-session')))
            with self.assertRaisesRegex(ValueError, 'probe_provenance_rejected'):
                e4.read_conflict_probe_bundle(probe_dir, prepared=prepared)
            (probe_dir / 'results.json').write_text(json.dumps(probe_summary))
            (probe_dir / 'results.json').chmod(0o600)

            # Begin
            baseline = e4.begin_remote_conflicts_phase(smoke, fixture, registry, prepared)
            self.assertIn('main', baseline)
            self.assertEqual(prepared['conflict_baseline'], baseline)

            # Finish setup
            results_dir = incoming / 'conflicts'
            results_dir.mkdir(mode=0o700, parents=True)
            proof = {
                'status': 'PASS',
                'joined': True,
                'phase': 'conflicts',
                'cases': {
                    name: {
                        'message_id': probe_data['cases'][name]['message_id'],
                        'payload_sha256': probe_data['cases'][name]['payload_sha256'],
                        'schema_valid': True,
                        'positive_control_ack_count': 1,
                        'conflict_ack_count': 0,
                        'raw_payload_b64': probe_data['cases'][name]['raw_payload_b64'],
                        'status': 'EXPECTED_REJECTED_NO_ACK',
                        'positive_control': {'status': 'COMPLETE', 'replay_offered': 1,
                            'replay_application_acks': 1, 'pending_messages': 0, 'joined': True,
                            'publish_attempts': 1, 'broker_pubacks': 1,
                            'binary_sha256': 'b' * 64, 'binary_os': 'linux', 'binary_arch': 'x86_64',
                            'returncode': 0, 'workload_sha256': 'a' * 64},
                        'transport': {'status': 'PARTIAL', 'returncode': 4, 'replay_offered': 1,
                            'replay_application_acks': 0, 'pending_messages': 1, 'joined': True,
                            'publish_attempts': 1, 'broker_pubacks': 1, 'retries': 1,
                            'observation_seconds': 4, 'binary_sha256': 'b' * 64,
                            'binary_os': 'linux', 'binary_arch': 'x86_64', 'workload_sha256': 'c' * 64}
                    }
                    for name in ('same_id_changed_payload', 'different_first_sequence_overlap')
                }
            }
            proof_bytes = json.dumps(proof).encode()
            (results_dir / 'conflicts-proof.json').write_bytes(proof_bytes)
            (results_dir / 'conflicts-proof.json').chmod(0o600)

            binding = bundle_binding
            binding_sha256 = hashlib.sha256(json.dumps(binding, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
            summary = {
                'schema_version': 1, 'experiment': 'E4', 'phase': 'conflicts',
                'session_id': 'a' * 32, 'run_id': 'run-1',
                'source_bundle_sha256': 'bundle-hash',
                'source_inventory': [],
                'binding': binding,
                'binding_sha256': binding_sha256,
                'proof': {'path': 'conflicts-proof.json', 'size_bytes': len(proof_bytes), 'sha256': hashlib.sha256(proof_bytes).hexdigest()},
                'full_p8_accepted': False, 'physical_ab': 'NOT RUN'
            }
            (results_dir / 'results.json').write_text(json.dumps(summary))
            (results_dir / 'results.json').chmod(0o600)

            # Recomputed transport/result digests cannot replace the exact A-guarded case bytes.
            forged_dir = incoming / 'forged-results'; forged_dir.mkdir(mode=0o700)
            forged = json.loads(json.dumps(proof))
            forged['cases']['same_id_changed_payload']['raw_payload_b64'] = base64.b64encode(raw_payload).decode()
            forged_bytes = json.dumps(forged).encode()
            (forged_dir / 'conflicts-proof.json').write_bytes(forged_bytes)
            (forged_dir / 'conflicts-proof.json').chmod(0o600)
            forged_summary = dict(summary, proof={'path': 'conflicts-proof.json',
                'size_bytes': len(forged_bytes), 'sha256': hashlib.sha256(forged_bytes).hexdigest()})
            (forged_dir / 'results.json').write_text(json.dumps(forged_summary))
            (forged_dir / 'results.json').chmod(0o600)
            with self.assertRaisesRegex(ValueError, 'payload_changed_after_guard'):
                e4.finish_remote_conflicts_phase(smoke, fixture, registry, prepared, forged_dir)

            class EmptyLogs:
                def command(self, args):
                    return SimpleNamespace(returncode=0, stdout=b'', stderr=b'')
            with self.assertRaisesRegex(ValueError, 'backend_logs_unavailable'):
                e4.finish_remote_conflicts_phase(smoke, fixture, EmptyLogs(), prepared, results_dir)

            class MockRegistryFinish:
                def command(self, args):
                    # Extra rejection log entries
                    return SimpleNamespace(returncode=0, stdout=b'reason_code=message_conflict\nreason_code=message_conflict\nreason_code=sequence_overlap\nreason_code=sequence_overlap\n', stderr=b'')

            finish_result = e4.finish_remote_conflicts_phase(smoke, fixture, MockRegistryFinish(), prepared, results_dir)
            self.assertEqual(finish_result['workload_outcome'], 'VALIDATED')
            self.assertEqual(prepared['conflict_verified']['status'], 'PASS')
            self.assertEqual(prepared['conflict_verified']['database_before'],
                             prepared['conflict_verified']['database_after'])
            self.assertIn('transport_proof', prepared['conflict_verified']['cases']['same_id_changed_payload'])


if __name__ == '__main__':
    unittest.main()
