import json
import re
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock
from urllib.parse import parse_qs

import paper_p5a_support as p5a
import native_compose_smoke as smoke


class PaperP5bComparableReplayTests(unittest.TestCase):
    def _replay_metrics(self, *, duplicate=24, committed=0, samples=0, hits=0,
                        avoided=0, begins=9, commits=9, copy=0, rows=0):
        return {'process_instance': 'p',
            'ingress': {'DuplicateCommitted': duplicate, 'Committed': committed,
                        'Processed': duplicate + committed, 'SamplesNew': samples,
                        'ACKPublished': duplicate + committed},
            'ram_dedup': {'lookup_hit_total': hits, 'database_calls_avoided_total': avoided},
            'persistence': {'begin_attempts': begins, 'confirmed_commits': commits,
                            'copy_calls': copy, 'confirmed_new_rows': rows}}

    def test_duplicate_replay_allows_duplicate_only_transactions_grouped_into_batches(self):
        before = self._replay_metrics(duplicate=0, begins=0, commits=0)
        for transactions in (8, 9):
            after = self._replay_metrics(begins=transactions, commits=transactions)
            evidence = p5a.duplicate_replay_evidence(mode='off', offered_attempts=24,
                received=24, acked=24, before_metrics=before, after_metrics=after,
                before_db={'rows': 1, 'version': 1}, after_db={'rows': 1, 'version': 1},
                exact_bytes=True, fresh_acks=24)
            self.assertEqual(evidence['status'], 'PASS', evidence['reasons'])
            self.assertEqual(evidence['persistence_deltas']['begin_attempts'], transactions)
            self.assertEqual(evidence['persistence_deltas']['confirmed_commits'], transactions)

        cached = self._replay_metrics(duplicate=24, hits=24, avoided=24, begins=0, commits=0)
        evidence = p5a.duplicate_replay_evidence(mode='on', offered_attempts=24,
            received=24, acked=24, before_metrics=before, after_metrics=cached,
            before_db={'rows': 1}, after_db={'rows': 1}, exact_bytes=True, fresh_acks=24)
        self.assertEqual(evidence['status'], 'PASS', evidence['reasons'])

    def test_duplicate_replay_rejects_incomplete_stale_ack_new_samples_and_data_changes(self):
        before = self._replay_metrics(duplicate=0, begins=0, commits=0)
        good = self._replay_metrics(begins=8, commits=8)
        mutations = [
            {'received': 23},
            {'acked': 23},
            {'fresh_acks': 23},
            {'after_metrics': self._replay_metrics(samples=1, begins=8, commits=8)},
            {'after_db': {'rows': 2}},
        ]
        for mutation in mutations:
            args = {'received': 24, 'acked': 24, 'fresh_acks': 24, 'after_metrics': good,
                 'after_db': {'rows': 1}}
            args.update(mutation)
            evidence = p5a.duplicate_replay_evidence(mode='off', offered_attempts=24,
                before_metrics=before, before_db={'rows': 1}, exact_bytes=True, **args)
            self.assertEqual(evidence['status'], 'FAIL', mutation)

    def test_subscription_capture_uses_a_fresh_matching_message_cursor(self):
        import threading
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'subscription.log'
            topic = 'gateways/g/acks/id'
            first = b'{"status":"committed","n":1}'
            second = b'{"status":"committed","n":2}'
            path.write_bytes(b"Client received PUBLISH (d0, q1, r0, m1, '" + topic.encode()
                             + b"')\n" + first + b"\n")
            proc = SimpleNamespace(poll=lambda: None)
            subscription = (proc, None, path)
            self.assertEqual(smoke.paper_p1_capture_subscription(subscription, topic,
                timeout=0.01), first)  # default retains legacy first-match semantics

            done = threading.Event()
            def produce_second():
                with path.open('ab') as stream:
                    stream.write(b"Client received PUBLISH (d0, q1, r0, m2, 'other/topic')\n{}\n")
                    stream.flush()
                    stream.write(b"Client received PUBLISH (d0, q1, r0, m3, '" + topic.encode()
                                 + b"')\n" + second + b"\n")
                    stream.flush()
                done.set()

            producer = threading.Thread(target=produce_second)
            producer.start()
            observed = smoke.paper_p1_capture_subscription(subscription, topic,
                timeout=2, after_count=1)
            producer.join(timeout=2)
            self.assertTrue(done.is_set())
            self.assertEqual(observed, second)
            self.assertIsNone(smoke.paper_p1_capture_subscription(subscription, topic,
                timeout=0.05, after_count=2))
            self.assertIsNone(smoke.paper_p1_capture_subscription(subscription, topic,
                timeout=0.01, after_count=3))
            with self.assertRaisesRegex(ValueError, 'paper_p1_subscription_failed'):
                smoke.paper_p1_capture_subscription(subscription, topic,
                    timeout=0.01, after_count=-1)

    def test_duplicate_replay_evidence_requires_fixed_attempts_ack_db_stability_and_mode_work(self):
        evidence = p5a.duplicate_replay_evidence(
            mode='on', offered_attempts=24, received=24, acked=24,
            before_metrics={'process_instance': 'p', 'ingress': {'DuplicateCommitted': 0, 'Committed': 0, 'Processed': 0,
                'SamplesNew': 0, 'ACKPublished': 0},
                'ram_dedup': {'lookup_hit_total': 0, 'database_calls_avoided_total': 0},
                'persistence': {'begin_attempts': 0, 'confirmed_commits': 0,
                                'copy_calls': 0, 'confirmed_new_rows': 0}},
            after_metrics={'process_instance': 'p', 'ingress': {'DuplicateCommitted': 24, 'Committed': 0, 'Processed': 24,
                'SamplesNew': 0, 'ACKPublished': 24},
                'ram_dedup': {'lookup_hit_total': 24, 'database_calls_avoided_total': 24},
                'persistence': {'begin_attempts': 0, 'confirmed_commits': 0,
                                'copy_calls': 0, 'confirmed_new_rows': 0}},
            before_db={'ledger': 24, 'raw': 2400, 'temporal': 24, 'version': '24'},
            after_db={'ledger': 24, 'raw': 2400, 'temporal': 24, 'version': '24'},
            exact_bytes=True, fresh_acks=24)
        self.assertEqual(evidence['status'], 'PASS')
        self.assertEqual(evidence['denominators']['offered'], 24)
        self.assertEqual(evidence['database_duplicate_delta'], 0)
        self.assertEqual(evidence['manual_outbox_readback'], 'not_applicable_manual_replay')
        self.assertNotEqual(evidence['status'], p5a.duplicate_replay_evidence(
            mode='on', offered_attempts=24, received=24, acked=23, fresh_acks=23,
            before_metrics={}, after_metrics={}, before_db={}, after_db={}, exact_bytes=True)['status'])

    def test_duplicate_replay_rejects_cacheoff_hits_and_unavailable_transaction_metrics(self):
        metrics = {'process_instance': 'p', 'ingress': {'DuplicateCommitted': 24, 'Committed': 0, 'Processed': 24,
                                                        'SamplesNew': 0, 'ACKPublished': 24},
                   'ram_dedup': {'lookup_hit_total': 0, 'database_calls_avoided_total': 0},
                   'persistence': {'begin_attempts': 8, 'confirmed_commits': 8,
                                   'copy_calls': 0, 'confirmed_new_rows': 0}}
        baseline = {**metrics, 'ingress': {'DuplicateCommitted': 0, 'Committed': 0, 'Processed': 0,
                                          'SamplesNew': 0, 'ACKPublished': 0},
                    'persistence': {'begin_attempts': 0, 'confirmed_commits': 0,
                                    'copy_calls': 0, 'confirmed_new_rows': 0}}
        good = p5a.duplicate_replay_evidence(mode='off', offered_attempts=24, received=24,
            acked=24, before_metrics=baseline,
            after_metrics=metrics, before_db={'rows': 1}, after_db={'rows': 1}, exact_bytes=True, fresh_acks=24)
        self.assertEqual(good['status'], 'PASS')
        bad = p5a.duplicate_replay_evidence(mode='off', offered_attempts=24, received=24,
            acked=24, before_metrics=metrics, after_metrics=metrics, fresh_acks=24,
            before_db={'rows': 1}, after_db={'rows': 1}, exact_bytes=True)
        self.assertEqual(bad['status'], 'FAIL')
        missing = p5a.duplicate_replay_evidence(mode='off', offered_attempts=24, received=24,
            acked=24, before_metrics={}, after_metrics={}, before_db={}, after_db={}, exact_bytes=True, fresh_acks=24)
        self.assertIn('metric_unavailable_or_reset:persistence.begin_attempts', missing['reasons'])

    def test_replay_diagnostics_are_safe_and_must_be_persisted_before_assertion(self):
        evidence = {'status': 'FAIL', 'reasons': ['ack_count_mismatch'], 'payload_sha256': 'a' * 64,
                    'payload': 'private bytes'}
        run_data, writes = {}, []
        persist = lambda: writes.append(json.dumps(run_data))
        p5a.persist_duplicate_replay_evidence(run_data, evidence, persist)
        self.assertTrue(writes)
        self.assertNotIn('private bytes', writes[0])
        self.assertEqual(json.loads(writes[0])['duplicate_replay']['status'], 'FAIL')

    def test_p5b_profile_marker_requires_comparable_replay_and_exact_cleanup(self):
        source = __import__('inspect').getsource(p5a.run)
        self.assertIn('duplicate_replay', source)
        self.assertIn('duplicate_replay', source)
        runner = Path(__file__).with_name('native_compose_smoke.py').read_text()
        self.assertIn("get('duplicate_replay', {}).get('status') == 'PASS'", runner)

    def test_replay_path_uses_captured_envelope_real_psql_bindings_and_pre_publish_observers(self):
        import inspect
        source = inspect.getsource(p5a.run)
        self.assertIn('exact_payload, message_id = captured[index]', source)
        self.assertIn("message_count=12", source)
        self.assertLess(source.index("message_count=12,\n                    message_timeout=60"),
                        source.index("p5b_before = scrape('p5b_duplicate_replay_before')"))
        self.assertIn("gateway_id IN (:'a',:'b')", source)
        self.assertIn("sql(\"SELECT count(*) FROM processed_messages", source)
        self.assertIn("input=(spec.gateway_id + '\\n' + passwords[index]", source)

    def test_process_identity_change_fails_even_when_duplicate_counters_are_monotonic(self):
        base = {'process_instance': 'old', 'ingress': {'DuplicateCommitted': 0, 'Committed': 0, 'Processed': 0,
                'SamplesNew': 0, 'ACKPublished': 0},
                'ram_dedup': {'lookup_hit_total': 0, 'database_calls_avoided_total': 0},
                'persistence': {'begin_attempts': 0, 'confirmed_commits': 0,
                                'copy_calls': 0, 'confirmed_new_rows': 0}}
        after = {**base, 'process_instance': 'new', 'ingress': {'DuplicateCommitted': 24, 'Committed': 0, 'Processed': 24,
                'SamplesNew': 0, 'ACKPublished': 24},
                 'persistence': {'begin_attempts': 8, 'confirmed_commits': 8,
                                 'copy_calls': 0, 'confirmed_new_rows': 0}}
        evidence = p5a.duplicate_replay_evidence(mode='off', offered_attempts=24,
            received=24, acked=24, before_metrics=base, after_metrics=after,
            before_db={'rows': 1}, after_db={'rows': 1}, exact_bytes=True, fresh_acks=24)
        self.assertIn('process_snapshot_unavailable_or_changed', evidence['reasons'])


class PaperP5aWorkloadContractTests(unittest.TestCase):
    def test_p5b_workload_profiles_are_cache_only_variants_with_fixed_p5a_on_bounds(self):
        for dataset, messages, samples in (('100', 24, 2400), ('1', 24, 24)):
            off = p5a.p5b_profile('off', dataset)
            on = p5a.p5b_profile('on', dataset)
            self.assertEqual(off['MQTT_RAM_DEDUP_ENABLED'], 'false')
            self.assertEqual(on['MQTT_RAM_DEDUP_ENABLED'], 'true')
            self.assertEqual({k: v for k, v in off.items() if k != 'MQTT_RAM_DEDUP_ENABLED'},
                             {k: v for k, v in on.items() if k != 'MQTT_RAM_DEDUP_ENABLED'})
            self.assertEqual(off['MQTT_MICROBATCH_ENABLED'], 'true')
            self.assertEqual(off['MQTT_MICROBATCH_MAX_MESSAGES'], '32')
            self.assertEqual(off['MQTT_MICROBATCH_MAX_WAIT'], '1s')
            self.assertEqual(p5a.workload_config(dataset)['offered_unique_messages'], messages)
            self.assertEqual(p5a.workload_config(dataset)['offered_unique_samples'], samples)
            metadata = p5a.p5b_run_metadata('off', dataset)
            self.assertEqual(metadata['database_schema_version'], 24)
            self.assertEqual(metadata['comparison']['microbatch_mode'], 'on')
            self.assertEqual(metadata['comparison']['cache_mode'], 'off')
            self.assertEqual(metadata['phases']['new_messages']['status'], 'configured_not_run')
            self.assertEqual(metadata['phases']['duplicate_replay']['status'], 'configured_not_run')
            self.assertEqual(metadata['phases']['duplicate_replay']['offered_attempts'], 24)
            self.assertEqual(metadata['phases']['duplicate_replay']['denominator'], '24 duplicate deliveries')
            self.assertEqual(metadata['phases']['drain']['status'], 'configured_not_run')
        # The P5b workload validates its cache-on profile against the dedicated
        # schema-24 metadata contract, rather than the older P5a default-off profile.
        on_metadata = p5a.p5b_run_metadata(
            'on', '100', effective_environment=p5a.p5b_profile('on', '100'))
        self.assertEqual(on_metadata['effective_profile_source'], 'validated_compose_environment')
        self.assertEqual(on_metadata['effective_profile']['MQTT_RAM_DEDUP_ENABLED'], 'true')
        with self.assertRaisesRegex(ValueError, '^paper_p5b_cache_mode_rejected$'):
            p5a.p5b_profile('invalid', '100')
        with self.assertRaisesRegex(ValueError, '^paper_p5b_dataset_rejected$'):
            p5a.p5b_profile('off', '10')

    def test_p5b_version_snapshot_query_joins_internal_uuid_and_filters_external_urn(self):
        query = p5a.VERSION_SNAPSHOT_SQL
        self.assertRegex(query, r'JOIN\s+twin_entities\s+e\s+ON\s+e\.id\s*=\s*s\.entity_id')
        self.assertRegex(query, r"e\.entity_id\s*=\s*:'urn'")
        self.assertNotRegex(query, r'USING\s*\(\s*entity_id\s*\)')
        self.assertRegex(p5a.STATE_FINGERPRINT_SQL,
                         r'JOIN\s+twin_entities\s+e\s+ON\s+e\.id\s*=\s*s\.entity_id')
        self.assertRegex(p5a.STATE_FINGERPRINT_SQL, r"e\.entity_id\s*=\s*:'urn'")

    def test_p5b_conflict_guard_sql_respects_schema_uuid_and_text_types_static(self):
        """Static contract check; this does not prove the queries on native PostgreSQL."""
        migration_1 = Path(__file__).parents[2] / 'migrations/000001_init_schema.up.sql'
        migration_20 = Path(__file__).parents[2] / 'migrations/000020_stage3_telemetry_ingestion.up.sql'
        schema = migration_1.read_text() + migration_20.read_text()
        self.assertRegex(schema, r'gateway_id\s+TEXT\s+NOT NULL')
        self.assertRegex(schema, r'message_id\s+UUID\s+NOT NULL')
        self.assertRegex(schema, r'sensor_id\s+TEXT,')
        self.assertRegex(schema, r'boot_id\s+UUID,')

        same_id = p5a.conflict_guard_query('same_id_changed_payload')
        overlap = p5a.conflict_guard_query('different_first_sequence_overlap')
        self.assertRegex(same_id, r'gateway_id\s*=\s*:\'gw\'\s+AND\s+message_id\s*=\s*:\'mid\'::uuid')
        self.assertRegex(same_id, r"payload_hash\s*<>\s*:'hash'")
        self.assertNotRegex(same_id, r"gateway_id\s*=\s*:'gw'::uuid")
        self.assertRegex(overlap, r"gateway_id\s*=\s*:'gw'\s+AND\s+sensor_id\s*=\s*:'sensor'")
        self.assertRegex(overlap, r"boot_id\s*=\s*:'boot'::uuid")
        self.assertRegex(overlap, r"message_id\s*<>\s*:'mid'::uuid")
        self.assertRegex(overlap, r'first_sequence\s*<=\s*:\'last\'::bigint')
        self.assertNotRegex(overlap, r"(?:gateway_id|sensor_id)\s*=\s*:[^ ]+::uuid")

    def test_p5b_cache_smoke_is_opt_in_and_keeps_p5a_settings_fixed(self):
        off = p5a.profile('on', '1')
        enabled = p5a.profile('on', '1', cache_smoke=True)
        self.assertEqual(enabled['MQTT_RAM_DEDUP_ENABLED'], 'true')
        self.assertEqual({k: v for k, v in enabled.items() if k != 'MQTT_RAM_DEDUP_ENABLED'},
                         {k: v for k, v in off.items() if k != 'MQTT_RAM_DEDUP_ENABLED'})
        self.assertTrue(p5a.run_metadata('on', '1', cache_smoke=True)['cache_smoke'])
        with self.assertRaisesRegex(ValueError, '^paper_p5b_cache_profile_rejected$'):
            p5a.profile('off', '1', cache_smoke=True)
        with self.assertRaisesRegex(ValueError, '^paper_p5b_cache_profile_rejected$'):
            p5a.profile('on', '100', cache_smoke=True)

    def test_p5b_cache_phase_evidence_requires_exact_replay_and_db_fallback(self):
        phases = p5a.cache_smoke_phases()
        self.assertEqual([phase['name'] for phase in phases], ['warm', 'replay', 'cold_replay'])
        good = {'warm': {'new_committed': True},
                'replay': {'payload_sha256_matches': True, 'cache_hit_delta': 1,
                           'repository_calls_avoided_delta': 1, 'application_ack': True,
                           'database_unchanged': True, 'outbox_readback': 'not_applicable_manual_replay'},
                'cold_replay': {'process_identity_changed': True, 'cache_entries_before': 0,
                                'cache_entries_after': 0, 'cache_miss_delta': 1, 'db_duplicate_delta': 1,
                                'database_work_observed': True,
                                'cache_hit_delta': 0, 'application_ack': True,
                                'database_unchanged': True}}
        self.assertTrue(p5a.cache_smoke_evidence_ready(good))
        bad = {**good, 'replay': {**good['replay'], 'application_ack': False}}
        self.assertFalse(p5a.cache_smoke_evidence_ready(bad))

    def test_p5b_warm_replay_requires_same_process_exact_cache_db_and_ack_proof(self):
        before = {'process_instance': 'p', 'elapsed_seconds': 10,
                  'ingress': {'DuplicateCommitted': 0},
                  'ram_dedup': {'lookup_hit_total': 2, 'database_calls_avoided_total': 2},
                  'persistence': {'begin_attempts': 4, 'confirmed_commits': 4,
                                  'copy_calls': 4, 'confirmed_new_rows': 48}}
        after = {'process_instance': 'p', 'elapsed_seconds': 11,
                 'ingress': {'DuplicateCommitted': 1},
                 'ram_dedup': {'lookup_hit_total': 3, 'database_calls_avoided_total': 3},
                 'persistence': {'begin_attempts': 4, 'confirmed_commits': 4,
                                 'copy_calls': 4, 'confirmed_new_rows': 48}}
        db = {'ledger': 12, 'raw': 12, 'temporal': 12,
              'version': '12', 'fingerprint': 'stable'}
        evidence = p5a.warm_replay_evidence(
            before, after, db, db, selected={'message_id': 'm', 'gateway_id': 'gw', 'topic': 't',
                'payload_sha256': 'a' * 64, 'original_bytes_sha256': 'a' * 64},
            ack_topic='gateways/gw/acks/m', ack_payload=b'{"protocol_version":1,"message_type":"telemetry_ack","message_id":"m","status":"committed","committed_at":"2026-10-26T10:00:00Z"}')
        self.assertEqual(evidence['status'], 'PASS')
        self.assertEqual(evidence['cache_hit_delta'], 1)
        self.assertEqual(evidence['database_calls_avoided_delta'], 1)
        for changed in (
            {'process_instance': 'other'},
            {'ram_dedup': {'lookup_hit_total': 2}},
        ):
            bad = {**after, **changed}
            self.assertEqual(p5a.warm_replay_evidence(before, bad, db, db,
                selected={'message_id': 'm', 'gateway_id': 'gw', 'topic': 't', 'payload_sha256': 'a' * 64,
                          'original_bytes_sha256': 'a' * 64}, ack_topic='gateways/gw/acks/m',
                ack_payload=b'{"protocol_version":1,"message_type":"telemetry_ack","message_id":"m","status":"committed","committed_at":"2026-10-26T10:00:00Z"}')['status'], 'FAIL')
        changed_db = {**db, 'version': '13'}
        failed = p5a.warm_replay_evidence(before, after, db, changed_db,
            selected={'message_id': 'm', 'gateway_id': 'gw', 'topic': 't', 'payload_sha256': 'a' * 64,
                      'original_bytes_sha256': 'a' * 64}, ack_topic='gateways/gw/acks/m',
            ack_payload=b'{"protocol_version":1,"message_type":"telemetry_ack","message_id":"m","status":"committed","committed_at":"2026-10-26T10:00:00Z"}')
        self.assertFalse(failed['database_unchanged'])

    def test_p5b_cache_acceptance_marker_requires_warm_conflicts_cold_and_cleanup(self):
        self.assertFalse(p5a.cache_smoke_acceptance_ready({}))
        phase = {'status': 'PASS', 'process_identity_same': True,
                 'payload_sha256_matches': True, 'application_ack': True,
                 'database_unchanged': True, 'cache_hit_delta': 1,
                 'database_calls_avoided_delta': 1, 'duplicate_committed_delta': 1}
        conflict_names = {'same_id_changed_payload', 'same_id_moved_sensor_time_key',
                          'same_ram_key_different_id', 'different_first_sequence_overlap'}
        conflicts = {'status': 'PASS', 'cases': {name: {'status': 'PASS'} for name in conflict_names}}
        cold = {'status': 'PASS', 'process_identity_changed': True, 'cache_entries_before': 0,
                'cache_entries_after': 0, 'cache_miss_delta': 1, 'cache_hit_delta': 0,
                'db_duplicate_delta': 1, 'database_work_observed': True, 'application_ack': True,
                'database_unchanged': True}
        complete = {'warm_replay': phase, 'conflicts': conflicts, 'cold_replay': cold,
                    'exact_cleanup_verified': True}
        self.assertFalse(p5a.cache_smoke_acceptance_ready({'warm_replay': phase, 'conflicts': conflicts}))
        self.assertTrue(p5a.cache_smoke_acceptance_ready(complete))
        self.assertFalse(p5a.cache_smoke_acceptance_ready(
            {**complete, 'warm_replay': {**phase, 'database_calls_avoided_delta': 0}}))
        self.assertFalse(p5a.cache_smoke_acceptance_ready({'warm_replay': phase}))

    def test_p5b_cache_acceptance_requires_cold_database_fallback_and_cleanup(self):
        warm = {'status': 'PASS', 'process_identity_same': True, 'payload_sha256_matches': True,
                'application_ack': True, 'database_unchanged': True, 'cache_hit_delta': 1,
                'database_calls_avoided_delta': 1, 'duplicate_committed_delta': 1}
        conflicts = {'status': 'PASS', 'cases': {name: {'status': 'PASS'} for name in (
            'same_id_changed_payload', 'same_id_moved_sensor_time_key',
            'same_ram_key_different_id', 'different_first_sequence_overlap')}}
        cold = {'status': 'PASS', 'process_identity_changed': True, 'cache_entries_before': 0,
                'cache_entries_after': 0, 'cache_miss_delta': 1, 'cache_hit_delta': 0, 'db_duplicate_delta': 1,
                'database_work_observed': True, 'application_ack': True, 'database_unchanged': True}
        self.assertFalse(p5a.cache_smoke_acceptance_ready({'warm_replay': warm, 'conflicts': conflicts}))
        self.assertFalse(p5a.cache_smoke_acceptance_ready({'warm_replay': warm, 'conflicts': conflicts,
                                                           'cold_replay': cold}))
        self.assertTrue(p5a.cache_smoke_acceptance_ready({'warm_replay': warm, 'conflicts': conflicts,
            'cold_replay': cold, 'exact_cleanup_verified': True}))

    def test_cold_replay_accepts_duplicate_only_transaction_without_new_data(self):
        before = {'process_instance': 'new', 'ram_dedup': {'entries': 0,
            'lookup_miss_total': 3, 'lookup_hit_total': 0, 'publication_stored_total': 7}, 'ingress': {'DuplicateCommitted': 0},
            'persistence': {'begin_attempts': 7, 'confirmed_commits': 7,
                'confirmed_new_rows': 20, 'copy_calls': 4}, 'ingress': {
                'DuplicateCommitted': 0, 'Committed': 3, 'SamplesNew': 20}}
        after = {'process_instance': 'new', 'ram_dedup': {'entries': 0,
            'lookup_miss_total': 4, 'lookup_hit_total': 0, 'publication_stored_total': 7}, 'ingress': {'DuplicateCommitted': 1},
            'persistence': {'begin_attempts': 8, 'confirmed_commits': 8,
                'confirmed_new_rows': 20, 'copy_calls': 4}, 'ingress': {
                'DuplicateCommitted': 1, 'Committed': 3, 'SamplesNew': 20}}
        db = {'ledger': 20, 'raw': 20, 'temporal': 20, 'version': '20', 'fingerprint': 'same'}
        ack = b'{"protocol_version":1,"message_type":"telemetry_ack","message_id":"m","status":"committed","committed_at":"2026-10-26T10:00:00Z"}'
        selected = {'message_id': 'm', 'gateway_id': 'gw', 'payload_sha256': 'a', 'original_bytes_sha256': 'a'}
        good = p5a.cold_replay_evidence(before, after, db, db, selected=selected,
            ack_topic='gateways/gw/acks/m', ack_payload=ack, old_process_identity='old')
        self.assertEqual(good['status'], 'PASS')
        self.assertEqual(good['begin_attempts_delta'], 1)
        self.assertEqual(good['confirmed_commits_delta'], 1)
        self.assertEqual(good['confirmed_new_rows_delta'], 0)
        self.assertEqual(good['copy_calls_delta'], 0)
        self.assertEqual(good['ingress_committed_delta'], 0)
        self.assertEqual(good['ingress_samples_new_delta'], 0)
        bad = p5a.cold_replay_evidence({**before, 'process_instance': 'old'}, after, db, db,
            selected=selected, ack_topic='gateways/gw/acks/m', ack_payload=ack,
            old_process_identity='old')
        self.assertEqual(bad['status'], 'FAIL')
        for field, value in (('confirmed_new_rows', 21), ('copy_calls', 5)):
            invalid = {**after, 'persistence': {**after['persistence'], field: value}}
            self.assertEqual(p5a.cold_replay_evidence(before, invalid, db, db,
                selected=selected, ack_topic='gateways/gw/acks/m', ack_payload=ack,
                old_process_identity='old')['status'], 'FAIL', field)
        for field, value in (('Committed', 4), ('SamplesNew', 21)):
            invalid = {**after, 'ingress': {**after['ingress'], field: value}}
            self.assertEqual(p5a.cold_replay_evidence(before, invalid, db, db,
                selected=selected, ack_topic='gateways/gw/acks/m', ack_payload=ack,
                old_process_identity='old')['status'], 'FAIL', field)
        no_db_work = {**after, 'persistence': {**after['persistence'], 'begin_attempts': 7}}
        self.assertEqual(p5a.cold_replay_evidence(before, no_db_work, db, db,
            selected=selected, ack_topic='gateways/gw/acks/m', ack_payload=ack,
            old_process_identity='old')['status'], 'FAIL')

    def test_cold_backend_replacement_stops_old_then_registers_owned_same_profile(self):
        fixture = SimpleNamespace(project='ncs_fixture', private=Path('/unused/private'))
        old = {'image': 'sha256:fixture', 'labels': {'test.run': 'ncs_fixture'},
            'expectation': {'network': 'ncs_fixture_iot_net'}, 'receipt': {'id': 'old-backend'}}
        item = {}
        env = [key + '=' + value for key, value in p5a.profile('on', '1', cache_smoke=True).items()]
        obj = {'Id': 'new-backend', 'Name': '/ncs_fixture-backend-cache-cold',
            'State': {'Running': True}, 'Config': {'Env': env}}
        registry = mock.Mock()
        registry.intent.return_value = item
        registry.resolve.side_effect = [None, obj]
        registry.command.return_value = SimpleNamespace(returncode=0, stdout=b'', stderr=b'')
        smoke = mock.Mock()
        smoke.BUDGETS = {'up': 30}
        smoke.compose_command.return_value = ['docker', 'compose', '-p', 'ncs_fixture', 'config', '--format', 'json']
        smoke.process_env.return_value = {'PATH': '/usr/bin'}
        smoke.startup_state.return_value = True
        containers = {'backend': old, 'nginx': {'receipt': {'id': 'nginx'}}}
        with mock.patch.object(p5a.subprocess, 'run', return_value=SimpleNamespace(
                returncode=0, stdout=b'', stderr=b'')) as compose:
            result = p5a.replace_backend_for_cold_cache(smoke, fixture, registry, containers, mock.Mock())
        names = [call[0] for call in registry.mock_calls]
        self.assertLess(names.index('command'), names.index('remove'))
        self.assertLess(names.index('remove'), names.index('allocating'))
        self.assertIs(containers['backend'], item)
        self.assertIs(result, obj)
        registry.intent.assert_called_once_with('container', 'ncs_fixture-backend-cache-cold',
            old['image'], old['labels'])
        self.assertIs(item['expectation'], old['expectation'])
        self.assertEqual(registry.command.call_args_list[0].args[0], ['stop', '--time', '20', 'old-backend'])
        self.assertEqual(registry.command.call_args_list[1].args[0], ['exec', 'nginx', 'nginx', '-s', 'reload'])
        fragment = smoke.private_write.call_args_list[0].args[1]
        self.assertIn('MQTT_RAM_DEDUP_ENABLED: "true"', fragment)
        self.assertIn('MQTT_MICROBATCH_ENABLED: "true"', fragment)
        self.assertEqual(compose.call_args.args[0][-7:],
            ['up', '--detach', '--no-deps', '--no-build', '--pull', 'never', 'backend'])

    def test_p5b_conflict_payloads_are_four_valid_targeted_mutations(self):
        original = json.dumps({
            'protocol_version': 1, 'message_id': 'a6f9a8e0-c223-4bce-9aab-1ebf62901fb3',
            'message_type': 'telemetry_batch', 'gateway_id': 'gw-a', 'sensor_id': 'sensor-a',
            'boot_id': '2c285db6-eef6-4ca0-b613-9f1663f1d95a', 'first_sequence': 42,
            'sample_count': 2, 'measured_at': '2026-10-26T10:00:00Z',
            'sample_interval_us': 10000, 'samples': [21.5, 21.6]}, separators=(',', ':')).encode()
        cases = p5a.conflict_payloads(original, gateway_id='gw-a', original_sensor_id='sensor-a',
                                      conflict_sensor_id='sensor-b', now='2026-10-26T10:05:00Z')
        self.assertEqual(set(cases), {'same_id_changed_payload', 'same_id_moved_sensor_time_key',
                                      'same_ram_key_different_id', 'different_first_sequence_overlap'})
        parsed = {name: json.loads(payload) for name, payload in cases.items()}
        self.assertEqual(parsed['same_id_changed_payload']['message_id'],
                         'a6f9a8e0-c223-4bce-9aab-1ebf62901fb3')
        self.assertNotEqual(parsed['same_id_changed_payload']['samples'], [21.5])
        moved = parsed['same_id_moved_sensor_time_key']
        self.assertEqual((moved['message_id'], moved['sensor_id']),
                         ('a6f9a8e0-c223-4bce-9aab-1ebf62901fb3', 'sensor-b'))
        self.assertNotEqual(moved['measured_at'], '2026-10-26T10:00:00Z')
        self.assertNotEqual(parsed['same_ram_key_different_id']['message_id'],
                            'a6f9a8e0-c223-4bce-9aab-1ebf62901fb3')
        overlap = parsed['different_first_sequence_overlap']
        self.assertEqual((overlap['first_sequence'], overlap['sample_count'], len(overlap['samples'])), (41, 2, 2))
        self.assertTrue(all(item['measured_at'] < '2026-10-26T10:05:00Z' for item in parsed.values()))

    def test_p5b_sequence_overlap_rejects_non_independent_zero_start_source(self):
        original = json.dumps({
            'protocol_version': 1, 'message_id': 'a6f9a8e0-c223-4bce-9aab-1ebf62901fb3',
            'message_type': 'telemetry_batch', 'gateway_id': 'gw-a', 'sensor_id': 'sensor-a',
            'boot_id': '2c285db6-eef6-4ca0-b613-9f1663f1d95a', 'first_sequence': 0,
            'sample_count': 1, 'measured_at': '2026-10-26T10:00:00Z',
            'sample_interval_us': 10000, 'samples': [21.5]}, separators=(',', ':')).encode()
        with self.assertRaisesRegex(ValueError, 'overlap_source_not_independent'):
            p5a.conflict_payloads(original, gateway_id='gw-a', original_sensor_id='sensor-a',
                                  conflict_sensor_id='sensor-b', now='2026-10-26T10:05:00Z')

    def test_p5b_conflict_guard_uses_batch_envelope_timestamp(self):
        message = {'sensor_id': 'sensor-a', 'boot_id': 'boot-a', 'measured_at': '2026-10-26T10:00:00Z',
                   'first_sequence': 0, 'sample_count': 2, 'samples': [21.49, 21.5]}
        values = p5a.conflict_guard_variables(message, gateway_id='gw-a', conflict_id='msg-a',
                                              conflict_bytes=b'payload')
        self.assertEqual(values['timestamp'], '2026-10-26T10:00:00Z')
        self.assertEqual((values['first'], values['last']), ('0', '1'))

    def test_p5b_conflict_guard_variables_pass_real_db_psql_validation(self):
        from types import SimpleNamespace
        message = {'sensor_id': 'sensor-a', 'boot_id': 'boot-a', 'measured_at': '2026-10-26T10:00:00Z',
                   'first_sequence': 0, 'sample_count': 2, 'samples': [21.49, 21.5]}
        variables = p5a.conflict_guard_variables(message, gateway_id='gw-a', conflict_id='msg-a',
                                                 conflict_bytes=b'payload')
        registry = SimpleNamespace(command=lambda args, input=None: SimpleNamespace(
            returncode=0, stdout=b'f\n', stderr=b''))
        result = smoke.db_psql(registry, 'postgres', 'SELECT 1;', variables)
        self.assertEqual(result, 'f\n')
        self.assertEqual(variables['first'], '0')
        self.assertEqual(variables['last'], '1')
        self.assertTrue(all(isinstance(value, str) for value in variables.values()))

    def test_p5b_overlap_requires_distinct_cached_range_and_nonzero_lower_start(self):
        original = json.dumps({
            'protocol_version': 1, 'message_id': 'a6f9a8e0-c223-4bce-9aab-1ebf62901fb3',
            'message_type': 'telemetry_batch', 'gateway_id': 'gw-a', 'sensor_id': 'sensor-a',
            'boot_id': '2c285db6-eef6-4ca0-b613-9f1663f1d95a', 'first_sequence': 5000,
            'sample_count': 2, 'measured_at': '2026-10-26T10:00:00Z',
            'sample_interval_us': 10000, 'samples': [21.5, 21.6]}, separators=(',', ':')).encode()
        cases = p5a.conflict_payloads(original, gateway_id='gw-a', original_sensor_id='sensor-a',
                                      conflict_sensor_id='sensor-b', now='2026-10-26T10:05:00Z')
        overlap = json.loads(cases['different_first_sequence_overlap'])
        self.assertEqual((overlap['first_sequence'], overlap['sample_count']), (4999, 2))
        self.assertNotEqual(overlap['first_sequence'], 0)
        self.assertEqual(overlap['first_sequence'] + 1, 5000)
        self.assertEqual((overlap['sensor_id'], overlap['boot_id']), ('sensor-a',
                         '2c285db6-eef6-4ca0-b613-9f1663f1d95a'))

    def test_p5b_conflict_evidence_requires_guard_rejection_without_ack_or_change(self):
        before = {'process_instance': 'p', 'ingress': {'Committed': 8, 'DuplicateCommitted': 4,
                    'SamplesNew': 8, 'PersistFailures': 0, 'Received': 12, 'ACKPublished': 12,
                    'ACKPublishFailures': 0, 'ValidationRejected': 0, 'ShaperRateLimited': 0,
                    'InFlight': 0, 'QueueDepth': 0},
                  'ram_dedup': {'lookup_hit_total': 4, 'lookup_metadata_mismatch_total': 1,
                                'lookup_miss_total': 7, 'database_calls_avoided_total': 4}}
        after = {'process_instance': 'p', 'ingress': {'Committed': 8, 'DuplicateCommitted': 4,
                    'SamplesNew': 8, 'PersistFailures': 1, 'Received': 13, 'ACKPublished': 12,
                    'ACKPublishFailures': 0, 'ValidationRejected': 0, 'ShaperRateLimited': 0,
                    'InFlight': 0, 'QueueDepth': 0},
                 'ram_dedup': {'lookup_hit_total': 4, 'lookup_metadata_mismatch_total': 2,
                               'lookup_miss_total': 7, 'database_calls_avoided_total': 4}}
        db = {'ledger': 8, 'raw': 8, 'temporal': 8, 'version': '8', 'fingerprint': 'same'}
        evidence = p5a.conflict_evidence(before, after, db, db, expected_lookup='metadata_mismatch',
         ack_payload=None, guard_reason='message_id_payload_mismatch',
                                         guard_precondition={'kind': 'message_id_payload_mismatch', 'matched': True},
                                         rejection_reason='message_conflict', expected_rejection_reason='message_conflict')
        self.assertEqual(evidence['status'], 'PASS')
        self.assertFalse(evidence['application_ack'])
        self.assertEqual(evidence['guard_reason'], 'message_id_payload_mismatch')
        self.assertEqual(p5a.conflict_evidence(before, after, db, db, expected_lookup='metadata_mismatch',
                           ack_payload=b'{}', guard_reason='message_id_payload_mismatch',
                           guard_precondition={'kind': 'message_id_payload_mismatch', 'matched': True},
                           rejection_reason='message_conflict', expected_rejection_reason='message_conflict')['status'], 'FAIL')
        self.assertEqual(p5a.conflict_evidence(before, after, db, db, expected_lookup='miss',
                           ack_payload=None, guard_reason='message_id_payload_mismatch',
                           guard_precondition={'kind': 'message_id_payload_mismatch', 'matched': False},
                            rejection_reason=None, expected_rejection_reason='message_conflict')['status'], 'FAIL')

    def test_p5b_conflict_rejection_reason_matches_repository_guard(self):
        expected = p5a.conflict_rejection_reasons()
        self.assertEqual(expected['same_id_changed_payload'], 'message_conflict')
        self.assertEqual(expected['same_id_moved_sensor_time_key'], 'message_conflict')
        self.assertEqual(expected['same_ram_key_different_id'], 'sequence_overlap')
        self.assertEqual(expected['different_first_sequence_overlap'], 'sequence_overlap')

    def test_p5b_moved_sensor_provision_uses_target_owner_and_saves_safe_status(self):
        target = SimpleNamespace(gateway_id='gateway-b', sensor_id='sensor-b', sensor_unit='C')
        tokens = {'a': 'owner-a-secret', 'b': 'owner-b-secret'}
        smoke = mock.Mock()
        smoke.http_request.return_value = (403, {}, None)
        run_data = {}
        persist = mock.Mock()
        with self.assertRaisesRegex(ValueError, '^paper_p5b_conflict_sensor_provision_failed$'):
            p5a.provision_conflict_sensor(smoke, 'https://fixture.invalid', target, tokens,
                                          run_data, persist)
        self.assertEqual(smoke.http_request.call_args.kwargs['token'], 'owner-b-secret')
        self.assertEqual(smoke.http_request.call_args.kwargs['body'], {'name': 'Cache conflict sensor', 'unit': 'C'})
        self.assertEqual(run_data['conflict_sensor_provision'], {'status': 403})
        persist.assert_called_once_with()

        smoke.http_request.return_value = (200, {}, None)
        run_data.clear()
        moved_id = p5a.provision_conflict_sensor(smoke, 'https://fixture.invalid', target, tokens,
                                                 run_data, persist)
        self.assertEqual(moved_id, 'sensor-b-conflict')
        self.assertEqual(run_data['conflict_sensor_provision'], {'status': 200})

    def test_p5b_overlap_setup_payload_uses_fresh_id_and_two_samples(self):
        original = json.dumps({'message_id': 'old-id', 'first_sequence': 12,
            'sample_count': 1, 'samples': [1.0], 'measured_at': 'old'}).encode()
        payload = p5a.overlap_setup_payload(original, first_sequence=10012,
                                            measured_at='2026-10-26T10:00:00Z')
        result = json.loads(payload)
        self.assertNotEqual(result['message_id'], 'old-id')
        self.assertEqual(result['first_sequence'], 10012)
        self.assertEqual(result['sample_count'], 2)
        self.assertEqual(len(result['samples']), 2)
        self.assertEqual(result['measured_at'], '2026-10-26T10:00:00Z')

    def test_p5b_run_does_not_shadow_conflict_backend_id_in_optional_branch(self):
        import inspect
        source = inspect.getsource(p5a.run)
        binding = "backend_id = containers['backend']['receipt']['id']"
        self.assertIn(binding, source)
        self.assertLess(source.index(binding), source.index("registry.command(['logs', '--tail', '500', backend_id])"))
        self.assertEqual(source.count(binding), 1)

    def test_shutdown_smoke_isolated_to_on_one_profile_and_uses_bounded_wait(self):
        ordinary = p5a.profile('on', '1')
        shutdown = p5a.profile('on', '1', shutdown_smoke=True)
        self.assertEqual(ordinary['MQTT_MICROBATCH_MAX_WAIT'], '1s')
        self.assertEqual(shutdown['MQTT_MICROBATCH_MAX_WAIT'], '2s')
        self.assertEqual({k: v for k, v in ordinary.items() if k != 'MQTT_MICROBATCH_MAX_WAIT'},
                         {k: v for k, v in shutdown.items() if k != 'MQTT_MICROBATCH_MAX_WAIT'})
        self.assertTrue(p5a.run_metadata('on', '1', shutdown_smoke=True)['shutdown_smoke'])
        self.assertNotIn('shutdown_smoke', p5a.run_metadata('on', '1'))
        with self.assertRaisesRegex(ValueError, '^paper_p5a_shutdown_profile_rejected$'):
            p5a.profile('off', '1', shutdown_smoke=True)
        with self.assertRaisesRegex(ValueError, '^paper_p5a_shutdown_profile_rejected$'):
            p5a.profile('on', '100', shutdown_smoke=True)

    def test_observers_fit_real_bounded_helper_and_keep_telemetry_principal(self):
        observers = p5a.observer_specs(p5a.workload_config('100'))
        self.assertEqual(len(observers), 2)
        self.assertEqual(len({item['tag'] for item in observers}), 2)
        self.assertTrue(all(item['kind'] == 'telemetry' for item in observers))
        self.assertTrue(all(1 <= item['timeout'] <= 90 for item in observers))
        self.assertTrue(all(item['count'] == 24 for item in observers))
        self.assertTrue(all(re.fullmatch(r'[a-z0-9-]{1,32}', item['tag']) for item in observers))
        with tempfile.TemporaryDirectory() as directory:
            fixture = smoke.prepare(Path(directory) / 'paper', paper_p1='baseline')
            spec = type('Spec', (), {'gateway_id': 'gw_p5a'})()
            for item in observers:
                username, password, topic = smoke.paper_p1_subscription_target(
                    fixture, spec, 'gateway-password', item['tag'], kind=item['kind'])
                self.assertEqual((username, password, topic),
                    (fixture.values['MQTT_USERNAME'], fixture.values['MQTT_PASSWORD'],
                     'gateways/+/telemetry/#'))
                command = smoke.paper_p1_subscription_command(
                    'broker', item['tag'], topic, message_count=item['count'],
                    message_timeout=item['timeout'])
                self.assertIn('-i p1-observer-' + item['tag'] + '-', command[6])
                self.assertIn('-W ' + str(item['timeout']), command[6])
            with self.assertRaisesRegex(ValueError, '^paper_p1_subscription_failed$'):
                smoke.paper_p1_subscriber(
                    fixture, None, 'broker', spec, 'pw', 'telemetry',
                    message_count=12, message_timeout=150)

    def test_history_request_uses_measured_window_and_backend_query_contract(self):
        first = '2026-10-25T10:00:00.123456Z'
        last = '2026-10-25T10:00:11.123456Z'
        path = p5a.temporal_read_path(smoke, 'urn:ngsi-ld:Sensor:gw:sensor', first, last, 1200)
        self.assertIn('/v1/digital-twins/urn:ngsi-ld:Sensor:gw:sensor/temporal?', path)
        query = parse_qs(path.split('?', 1)[1])
        self.assertEqual(query['attrs'], ['measurement'])
        self.assertEqual(query['max_points'], ['1200'])
        self.assertEqual(query['timerel'], ['between'])
        self.assertEqual(query['timeAt'], [first])
        self.assertEqual(query['endTimeAt'], [last])
        self.assertTrue(p5a.temporal_count_matches({'measurement': {'values': [[1], [2]]}}, 2))
        self.assertFalse(p5a.temporal_count_matches({'measurement': {'values': [[1]]}}, 2))

    def test_temporal_read_windows_partition_actual_bounds_under_raw_window_cap(self):
        first = '2026-10-25T10:00:00.123456+00:00'
        last = '2026-10-25T10:00:11.123456+00:00'
        windows = p5a.temporal_read_windows(first, last)
        self.assertEqual(len(windows), 2)
        self.assertEqual(windows[0][0].isoformat(), '2026-10-25T10:00:00.123456+00:00')
        self.assertEqual(windows[0][1], windows[1][0])
        self.assertTrue(all(end - start <= p5a.TEMPORAL_RAW_WINDOW
                            for start, end in windows))
        self.assertEqual(windows[-1][1].isoformat(), '2026-10-25T10:00:11.123457+00:00')
        path = p5a.temporal_read_path(smoke, 'urn:ngsi-ld:Sensor:gw:sensor',
                                      *windows[0], 1200)
        query = parse_qs(path.split('?', 1)[1])
        self.assertEqual(query['timeAt'], ['2026-10-25T10:00:00.123456Z'])
        self.assertEqual(query['endTimeAt'], ['2026-10-25T10:00:09.123456Z'])

    def test_history_point_counts_use_raw_samples_and_bounded_explicit_limits(self):
        for dataset, expected, limit in (('100', 1200, 1200), ('1', 12, 100)):
            config = p5a.workload_config(dataset)
            self.assertEqual(p5a.expected_measurement_points(config), expected)
            self.assertEqual(p5a.history_max_points(config), limit)
            self.assertGreaterEqual(limit, expected)
            self.assertLessEqual(limit, 2000)

    def test_read_diagnostics_are_safe_and_persisted_before_failure(self):
        run_data = {'history_reads': []}
        persisted = []

        def persist():
            persisted.append(json.loads(json.dumps(run_data)))

        result = p5a.history_read_diagnostic(
            'urn:ngsi-ld:Sensor:gw:sensor', 503, {'private': 'must-not-leak'},
            200, {'measurement': {'values': [[1], [2]]}}, expected_points=1200,
            max_points=1200, first='2026-10-25T10:00:00Z', last='2026-10-25T10:00:11Z',
            query_windows=[{'timeAt': '2026-10-25T10:00:00Z', 'http_status': 200}])
        self.assertFalse(result['accepted'])
        with self.assertRaisesRegex(ValueError, '^paper_p5a_read_regression_failed$'):
            p5a.persist_history_read_diagnostic(run_data, result, persist)
        self.assertEqual(len(persisted), 1)
        saved = json.dumps(persisted[0])
        self.assertIn('http_status', saved)
        self.assertIn('expected_points', saved)
        self.assertIn('actual_points', saved)
        self.assertIn('max_points', saved)
        self.assertIn('2026-10-25T10:00:00Z', saved)
        self.assertIn('"windows"', saved)
        self.assertNotIn('must-not-leak', saved)
        self.assertNotIn('private', saved)

    def test_history_diagnostics_classify_malformed_and_error_bodies_safely(self):
        malformed = p5a.history_read_diagnostic(
            'urn:ngsi-ld:Sensor:gw:sensor', 200, [], 200, {}, expected_points=12,
            max_points=100, first='2026-10-25T10:00:00Z', last='2026-10-25T10:00:01Z')
        error = p5a.history_read_diagnostic(
            'urn:ngsi-ld:Sensor:gw:sensor', 200,
            {'id': 'urn:ngsi-ld:Sensor:gw:sensor', 'reported_state': {'value': 1}, 'reported_version': '1'},
            400, {'error': {'message': 'private'}},
            expected_points=12, max_points=100,
            first='2026-10-25T10:00:00Z', last='2026-10-25T10:00:01Z')
        self.assertEqual(malformed['reason'], 'twin_body_malformed')
        self.assertEqual(malformed['actual_points'], None)
        self.assertEqual(error['reason'], 'temporal_http_error')
        self.assertEqual(error['actual_points'], None)
        self.assertNotIn('private', json.dumps([malformed, error]))

    def test_measurement_export_parses_real_ack_summary_without_conflating_windows(self):
        output = ('measurement summary: samples=12 p50_ms=4.12 p95_ms=9.87 min_ms=1.01 '
                  'max_ms=12.34 first_publishes=12 retry_attempts=0 missing_origins=0 '
                  'overflow_samples=0 gen_duration_ms=100.00 publish_drain_duration_ms=45.00 '
                  'total_duration_ms=145.00')
        measurement = p5a.gateway_measurement(output)
        self.assertEqual(measurement['status'], 'available')
        self.assertEqual(measurement['samples'], 12)
        self.assertEqual(measurement['p95_ms'], 9.87)
        self.assertEqual(measurement['publish_drain_duration_ms'], 45.0)
        self.assertNotEqual(measurement['gen_duration_ms'], measurement['publish_drain_duration_ms'])
        self.assertEqual(measurement['reported_resolution_ms'], 0.01)
        self.assertIsNone(measurement['histogram_resolution_ms'])
        self.assertEqual(p5a.gateway_measurement_args(p5a.workload_config('100')),
                         ['--measure', '--measure-capacity', '12'])
        missing = p5a.gateway_measurement('no measurement summary')
        self.assertEqual(missing, {'status': 'unavailable', 'reason': 'summary_missing'})

    def test_finite_workload_is_identical_between_modes_and_has_real_group_members(self):
        off = p5a.workload_config('100')
        on = p5a.workload_config('100')
        self.assertEqual(off, on)
        self.assertEqual(off['gateway_count'], 2)
        self.assertEqual(off['messages_per_gateway'], 12)
        self.assertEqual(off['offered_unique_messages'], 24)
        self.assertEqual(off['offered_unique_samples'], 2400)
        self.assertGreaterEqual(off['messages_per_group_target'], 2)
        self.assertLessEqual(off['run_deadline_seconds'], 180)
        one = p5a.workload_config('1')
        self.assertEqual(one['offered_unique_messages'], off['offered_unique_messages'])
        self.assertEqual(one['offered_unique_samples'], 24)

    def test_accounting_requires_exact_drain_and_reconciles_offered_data(self):
        config = p5a.workload_config('100')
        p5a.validate_accounting(config, {
            'offered_unique': 24, 'committed_unique': 24, 'raw_samples': 2400,
            'temporal': 24, 'application_acks': 24, 'outbox_rows': 0,
            'outbox_bytes': 0, 'payload_budget_bytes': 0,
        })
        with self.assertRaisesRegex(ValueError, '^paper_p5a_accounting_rejected$'):
            p5a.validate_accounting(config, {
                'offered_unique': 24, 'committed_unique': 24, 'raw_samples': 2399,
                'temporal': 24, 'application_acks': 24, 'outbox_rows': 0,
                'outbox_bytes': 0, 'payload_budget_bytes': 0,
            })

    def test_acceptance_requires_runtime_groups_replay_acl_reads_and_cleanup(self):
        required = p5a.REQUIRED_CHECKS
        checks = {name: {'status': 'PASS'} for name in required}
        checks['group_metrics_observed'] = {'status': 'PASS', 'groups': 1, 'max_members': 2}
        self.assertTrue(p5a.acceptance_ready(checks, 'on'))
        checks['group_metrics_observed'] = {'status': 'FAIL', 'groups': 1, 'max_members': 1}
        self.assertFalse(p5a.acceptance_ready(checks, 'on'))
        self.assertFalse(p5a.acceptance_ready(
            {name: {'status': 'PASS'} for name in required - {'duplicate_replay_unchanged'}}, 'off'))

    def test_metadata_and_csv_are_secret_and_payload_safe(self):
        metadata = p5a.run_metadata('off', '1')
        self.assertEqual(metadata['offered_pattern']['status'], 'configured_not_run')
        safe = json.dumps(p5a.safe_artifact({'payload': 'private-bytes', 'password': 'hidden', 'count': 1}))
        self.assertNotIn('private-bytes', safe)
        self.assertNotIn('hidden', safe)
        self.assertIn('"count": 1', safe)


if __name__ == '__main__':
    unittest.main()
