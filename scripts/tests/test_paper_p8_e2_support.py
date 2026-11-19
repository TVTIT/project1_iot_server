import copy
import hashlib
import json
import os
import platform
import shutil
import subprocess
import sys
import tempfile
import time
import threading
import unittest
from datetime import datetime, timezone, timedelta
from types import SimpleNamespace
from unittest.mock import Mock, patch
from pathlib import Path

from scripts import paper_p8_bundle

from scripts.tests.paper_p8_e2_support import (
    build_e2_execution_plan, e2_fixture_profile, e2_shaper_caps, e2_official_estimate, e2_pilot_manifests,
    e2_official_manifests,
    validate_e2_result,
    validate_e2_equal_samples_result,
    verify_e2_equal_samples_raw,
    build_e2_equal_samples_context,
    validate_e2_equal_samples_context,
    calculate_e2_equal_samples_expected_counts,
    calculate_e2_equal_samples_context_sha256,
    e2_equal_samples_logical_stream,
    e2_equal_samples_manifests,
    synthetic_value,
    E2_EQUAL_SAMPLES_CONTEXT_KEYS,
    E2_EQUAL_SAMPLES_MANIFEST_KEYS,
    MAX_CONTEXT_AGE_SECONDS_BOUND,
    prepare_remote,
)
from scripts.tests import paper_p8_e2_support as e2
from scripts.tests import paper_p8_support as p8
from scripts.tests import native_compose_smoke as smoke_runtime
from scripts import paper_p8_client


class E2SupportTests(unittest.TestCase):
    def test_resource_source_receives_original_container_mapping(self):
        containers = {'backend': {'kind': 'container', 'receipt': {'id': 'b'}},
                      'postgres': {'kind': 'container', 'receipt': {'id': 'p'}}}
        with patch.object(e2, 'read_postgres_diagnostics', return_value={'ok': True}), \
             patch.object(e2, 'read_linux_resources', return_value={'containers': []}) as linux:
            e2._read_diagnostic_sources(object(), object(), 'p', containers)
        self.assertIs(linux.call_args.args[1], containers)

    def test_official_begin_closes_over_receipt_mapping_for_linux_sampler(self):
        containers = {'backend': {'kind': 'container', 'receipt': {'id': 'b'}},
                      'postgres': {'kind': 'container', 'receipt': {'id': 'p'}}}
        prepared = {'phase_id': 'samples-1', 'workload': 'steady_burst', 'profile': 'official',
            'manifests': {'A': {'phases': {'measure_seconds': 1, 'drain_seconds': 1}},
                          'B': {'phases': {'measure_seconds': 1, 'drain_seconds': 1}}},
            'pg': 'pg', 'specs': [SimpleNamespace(), SimpleNamespace()], 'tokens': {'admin': 'x'}}
        smoke = SimpleNamespace(paper_p1_count_db=Mock(return_value={}))
        registry = SimpleNamespace(resolve=Mock(return_value={'Id': 'p'}))
        with patch.object(e2._support, '_admin_metrics', return_value={}), \
             patch.object(e2, '_target_counts', return_value={}), \
             patch.object(e2, 'read_container_resources', return_value={}), \
             patch.object(e2, 'read_postgres_diagnostics', return_value={}), \
             patch.object(e2, 'read_linux_resources', return_value={}) as linux:
            e2.begin_remote_collection(smoke, SimpleNamespace(), object(), registry, containers,
                                       prepared, phase_id='samples-1')
            prepared['collector'].side_sampler.sample_step()
            self.assertIs(linux.call_args.args[1], containers)
            prepared['collector'].stop(timeout_seconds=0.1)

    def test_equal_samples_compact_finish_does_not_use_legacy_terminal_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            binary = root / 'paperload'; binary.write_bytes(b'bin')
            collector = e2.EqualSamplesCompactSampler(scrape=lambda: {}, interval=0.1,
                max_snapshots=8, byte_cap=4096, deadline_seconds=10)
            prepared = {'phase_id': 'equal-samples-1', 'workload': 'equal_samples',
                'profile': 'pilot', 'collector': collector, 'samples_per_message': 1,
                'session_id': 's', 'run_id': 'r'}
            smoke = SimpleNamespace(durable_json=Mock(),
                paper_p0_stats=Mock(side_effect=AssertionError('legacy path')))
            with patch.object(e2, '_load_e2_result_bundle', return_value={} ), \
                 patch.object(e2._support, '_admin_metrics', return_value={}), \
                 patch.object(e2, 'read_container_resources', return_value={}):
                with self.assertRaises((KeyError, ValueError)):
                    e2.finish_remote_collection(smoke, SimpleNamespace(), object(), object(), {},
                        prepared, root, phase_id='equal-samples-1', paperload_binary=str(binary))
            smoke.paper_p0_stats.assert_not_called()
            self.assertEqual(collector.metrics, [])

    def test_scrape_finished_clock_is_captured_before_slow_resource_read(self):
        clock = {'value': 10.0}
        def slow_resource():
            clock['value'] = 99.0
            return {}
        sampler = e2.EqualSamplesCompactSampler(scrape=lambda: {'ingress': {}},
            resource_reader=slow_resource, interval=0.1, max_snapshots=8,
            byte_cap=4096, deadline_seconds=10, time_fn=lambda: clock['value'],
            wall_fn=lambda: 'fixed')
        sampler._started_monotonic = 10.0
        sampler.sample_step()
        self.assertEqual(sampler.retained_snapshots[0]['scrape_finished_monotonic'], 10.0)

    def test_failure_http_error_still_reads_terminal_sources_and_resources(self):
        sampler = e2.EqualSamplesCompactSampler(scrape=lambda: {}, interval=0.1,
            max_snapshots=8, byte_cap=4096, deadline_seconds=10)
        sampler.side_sampler = e2.IndependentSideSampler(reader={
            'postgres': lambda: {}, 'linux': lambda: {}, 'docker': lambda: {}})
        containers = {'postgres': {'kind': 'container', 'receipt': {'id': 'pg'}}}
        registry = SimpleNamespace(resolve=Mock(return_value={'Id': 'pg'}))
        prepared = {'collector': sampler, 'phase_id': 'p', 'session_id': 's', 'run_id': 'r',
                    'tokens': {'admin': 'x'}, 'pg': 'pg', 'specs': [], 'before_targets': []}
        with patch.object(e2._support, '_admin_metrics', side_effect=RuntimeError()), \
             patch.object(e2, 'read_postgres_diagnostics', return_value={'pg': 1}), \
             patch.object(e2, 'read_linux_resources', return_value={'linux': 1}), \
             patch.object(e2, 'read_container_resources', return_value={'services': {'backend': {'available': True}}}) as docker:
            result = e2.failure_diagnostics(prepared, 'ABORTED', smoke=object(), endpoint='endpoint',
                registry=registry, containers=containers)
        self.assertEqual(result['postgres_linux']['terminal']['postgres'], {'pg': 1})
        self.assertIn('workload_terminal', [r['phase'] for r in result['resource_samples']])
        docker.assert_called_once()

    def test_diagnostics_envelope_reuses_full_failure_contract_for_normal_finish(self):
        hist = {'upper_seconds': [1.0], 'buckets': [2], 'count': 2, 'overflow': 0,
                'sum_seconds': 0.5}
        collector = e2.EqualSamplesCompactSampler(scrape=lambda: {'ingress': {
            'Durations': {'ack_publish': hist}}, 'persistence': {'confirmed_commits': 2}},
            interval=0.1, max_snapshots=8, byte_cap=4096, deadline_seconds=10,
            baseline_metrics={'ingress': {'Received': 0}})
        collector.sample_step()
        # Attach a triggered side sampler: baseline 0, then observe increase to 25
        side = e2.IndependentSideSampler(reader={
            'postgres': lambda: {'active_marker': 'triggered'}, 'linux': lambda: {'cpu': 50},
            'docker': lambda: {'containers': []}},
            max_samples=64, byte_cap=2 * 1024 * 1024, capture_seconds=60,
            baseline_metrics={'ingress': {'Received': 0}, 'process_instance': 'proc-123'},
            deadline_seconds=120)
        # Simulate valid HTTP increase on same process triggers side
        side.observe_ingress({'process_instance': 'proc-123', 'ingress': {'Received': 25},
                              'scrape_finished_monotonic': 10.0,
                              'scrape_finished_timestamp': '2026-11-07T00:00:10Z'})
        self.assertTrue(side._triggered)
        collector.side_sampler = side
        collector.side_sampler.sample_step()
        prepared = {'collector': collector, 'phase_id': 'samples-1', 'session_id': 's',
            'run_id': 'r', 'source_provenance': {'revision': 'abc'}, 'bundle_sha256': 'b',
            'manifest_sha256': ['m'], 'diagnostic_baseline': {'postgres': {'base': 1}},
            '_started_wall': 'started'}
        envelope = e2.failure_diagnostics(prepared, 'VALIDATED', terminal_data={
            'metrics': {'ingress': {'Received': 2}}, 'resources': {'services': {'backend': {'available': True}}},
            'postgres_linux': {'postgres': {'terminal': 1}},
            'database_counts': {'available': True}, 'collector_stop_joined': True})
        self.assertEqual(envelope['source'], {'revision': 'abc'})
        # Baseline and terminal separate
        self.assertEqual(envelope['postgres_linux']['baseline'], {'postgres': {'base': 1}})
        self.assertEqual(envelope['postgres_linux']['terminal'], {'postgres': {'terminal': 1}})
        sample = next(row['snapshot'] for row in envelope['snapshots'] if row['phase'] == 'sample')
        self.assertEqual(sample['ingress']['durations']['ack_publish']['count'], 2)
        self.assertEqual(envelope['full_p8_accepted'], False)
        self.assertIn('coverage', envelope)
        # Active side data survives: triggered side samples present
        pl = envelope['postgres_linux']
        self.assertIsInstance(pl.get('samples'), list)
        self.assertIn('coverage', pl)
        # Active markers present in side sample (if any samples captured)
        if len(pl['samples']) > 0:
            active_sample = pl['samples'][0]
            self.assertEqual(active_sample['postgres']['value']['active_marker'], 'triggered')
            self.assertEqual(active_sample['linux']['value']['cpu'], 50)

    def test_fake_clock_sampler_to_writer_end_to_end_regression(self):
        """Task 4: fake-clock sampler -> failure_diagnostics -> actual writer regression.

        Long delay before valid HTTP trigger; side captures active PG/Linux markers;
        natural end; many idle ticks; export via failure_diagnostics with real persist
        helper; active data + coverage survive; baseline/terminal separate; no active
        eviction; no extra read after natural end; idempotent repeated export.
        """
        import tempfile, json
        from pathlib import Path
        from scripts.tests import native_compose_smoke as smoke_runtime

        # Fake clock: long delay (minutes) before valid HTTP trigger
        clock = {'mono': 0.0, 'wall': '2026-11-07T00:00:00Z'}
        def time_fn(): return clock['mono']
        def wall_fn(): return clock['wall']

        # Build side sampler with capture mode, baseline 0, process_instance matched
        side = e2.IndependentSideSampler(reader={
            'postgres': lambda: {'pg_active': True, 'rows': 1000},
            'linux': lambda: {'linux_active': True, 'cpu': 42},
            'docker': lambda: {'containers': []}},
            max_samples=128, byte_cap=2 * 1024 * 1024, capture_seconds=300,
            baseline_metrics={'ingress': {'Received': 0}, 'process_instance': 'proc-a'},
            deadline_seconds=3600, time_fn=time_fn, wall_fn=wall_fn)

        # Simulate many idle ticks (operator wait) with Received == baseline (no trigger)
        for _ in range(20):
            clock['mono'] += 30.0  # 30s idle ticks = 10 minutes of idle
            clock['wall'] = '2026-11-07T00:10:00Z'
            side.observe_ingress({'process_instance': 'proc-a', 'ingress': {'Received': 0},
                                  'scrape_finished_monotonic': clock['mono'],
                                  'scrape_finished_timestamp': clock['wall']})
        self.assertFalse(side._triggered)  # No-trigger: idle does not trigger
        self.assertEqual(side._state, 'waiting')

        # Valid HTTP increase on same process triggers side (after long delay)
        clock['mono'] += 60.0  # another minute passed
        clock['wall'] = '2026-11-07T00:11:00Z'
        side.observe_ingress({'process_instance': 'proc-a', 'ingress': {'Received': 150},
                              'scrape_finished_monotonic': clock['mono'],
                              'scrape_finished_timestamp': clock['wall']})
        self.assertTrue(side._triggered)
        self.assertEqual(side._state, 'active_capture')

        # Sample active PG/Linux markers during capture window
        for _ in range(5):
            clock['mono'] += 10.0
            clock['wall'] = '2026-11-07T00:11:' + str(10 + _) + 'Z'
            side.sample_step()  # captures active PG/Linux markers

        # Natural end: freeze capture
        clock['mono'] += 300.0  # capture_seconds elapsed
        clock['wall'] = '2026-11-07T00:16:10Z'
        side._freeze_if_needed(clock['mono'], clock['wall'], reason='natural_end')
        self.assertTrue(side._frozen)
        self.assertEqual(side._freeze_reason, 'natural_end')

        # Many idle ticks after natural end - no new reads, no active eviction
        samples_before = len(side.samples)
        for _ in range(10):
            clock['mono'] += 60.0  # 10 more minutes idle
            clock['wall'] = '2026-11-07T00:17:' + str(_) + 'Z'
            side.observe_ingress({'process_instance': 'proc-a', 'ingress': {'Received': 150},
                                  'scrape_finished_monotonic': clock['mono'],
                                  'scrape_finished_timestamp': clock['wall']})
        self.assertEqual(len(side.samples), samples_before)  # No extra reads after freeze
        self.assertFalse(side._active_truncated)  # No active eviction

        # Build collector with real data and fake clock
        hist = {'upper_seconds': [1.0], 'buckets': [2], 'count': 2, 'overflow': 0,
                'sum_seconds': 0.5}
        collector = e2.EqualSamplesCompactSampler(scrape=lambda: {'ingress': {
            'Durations': {'ack_publish': hist}}, 'persistence': {'confirmed_commits': 2}},
            interval=0.1, max_snapshots=8, byte_cap=4096, deadline_seconds=10,
            baseline_metrics={'ingress': {'Received': 0}},
            time_fn=time_fn, wall_fn=wall_fn)
        collector.sample_step()
        collector.side_sampler = side

        prepared = {'collector': collector, 'phase_id': 'samples-1', 'session_id': 's',
            'run_id': 'r', 'source_provenance': {'revision': 'abc'}, 'bundle_sha256': 'b',
            'manifest_sha256': ['m'], 'diagnostic_baseline': {'postgres': {'base': 1}},
            '_started_wall': 'started'}

        # Export via failure_diagnostics with real persist helper (actual writer)
        with tempfile.TemporaryDirectory(dir='/var/tmp/opencode') as tmp:
            diag_path = Path(tmp) / 'p8-e2-diagnostics.json'

            def persist_callback(payload):
                smoke_runtime.paper_p8_e2_persist_diagnostics(diag_path, payload)

            envelope = e2.failure_diagnostics(prepared, 'PARTIAL', terminal_data={
                'metrics': {'ingress': {'Received': 2}},
                'resources': {'services': {'backend': {'available': True}}},
                'postgres_linux': {'postgres': {'terminal': 1}},
                'database_counts': {'available': True}, 'collector_stop_joined': True},
                persist_callback=persist_callback, time_fn=time_fn, wall_fn=wall_fn)

            # Persist via real helper
            persist_callback(envelope)

            # Verify artifact written and readable
            self.assertTrue(diag_path.is_file())
            file_size = diag_path.stat().st_size
            self.assertGreater(file_size, 0)
            self.assertLessEqual(file_size, 8 * 1024 * 1024)

            # Read back and verify
            written = json.loads(diag_path.read_text(encoding='utf-8'))
            self.assertEqual(written['full_p8_accepted'], False)
            self.assertEqual(written.get('workload_outcome'), 'PARTIAL')

            # Active data and coverage survive
            pl = written['postgres_linux']
            self.assertEqual(pl['baseline'], {'postgres': {'base': 1}})
            self.assertEqual(pl['terminal'], {'postgres': {'terminal': 1}})
            self.assertIn('coverage', pl)
            self.assertGreater(pl['coverage']['samples_retained'], 0)
            self.assertGreater(len(pl['samples']), 0)
            active_sample = pl['samples'][0]
            self.assertEqual(active_sample['postgres']['value']['pg_active'], True)
            self.assertEqual(active_sample['linux']['value']['linux_active'], True)

            # Idempotent: repeated failure_diagnostics returns same result
            envelope2 = e2.failure_diagnostics(prepared, 'PARTIAL', terminal_data={
                'metrics': {'ingress': {'Received': 2}},
                'resources': {'services': {'backend': {'available': True}}},
                'postgres_linux': {'postgres': {'terminal': 1}},
                'database_counts': {'available': True}, 'collector_stop_joined': True},
                persist_callback=persist_callback, time_fn=time_fn, wall_fn=wall_fn)
            self.assertEqual(envelope2, envelope)

    def test_side_sampler_qualification_table_no_trigger_early_stop_cap(self):
        """Task 4 qualification table: no-trigger, early-stop, cap outcomes."""
        cases = [
            # (label, baseline_received, observe_received, expected_triggered, expected_state)
            ('no-trigger-idle', 10, 10, False, 'waiting'),
            ('no-trigger-decrease', 100, 50, False, 'waiting'),
            ('trigger-increase', 10, 25, True, 'active_capture'),
            ('trigger-from-zero-baseline', 0, 1, True, 'active_capture'),
        ]
        for label, baseline_recv, observe_recv, expected_trig, expected_state in cases:
            with self.subTest(label=label):
                side = e2.IndependentSideSampler(reader={
                    'postgres': lambda: {}, 'linux': lambda: {}, 'docker': lambda: {}},
                    max_samples=64, byte_cap=2 * 1024 * 1024, capture_seconds=60,
                    baseline_metrics={'ingress': {'Received': baseline_recv},
                                      'process_instance': 'proc-x'},
                    deadline_seconds=120)
                side.observe_ingress({'process_instance': 'proc-x', 'ingress': {'Received': observe_recv},
                                      'scrape_finished_monotonic': 10.0,
                                      'scrape_finished_timestamp': '2026-11-07T00:00:10Z'})
                self.assertEqual(side._triggered, expected_trig, label)
                if expected_trig:
                    self.assertEqual(side._state, expected_state, label)
                side.stop(timeout_seconds=0)

    def test_official_side_sampler_does_not_block_metrics_scrapes(self):
        entered, second_scrape = threading.Event(), threading.Event()
        calls = []
        release = threading.Event()
        def slow():
            entered.set()
            release.wait()
            return {'safe': True}
        def scrape():
            calls.append(1)
            if len(calls) >= 2:
                second_scrape.set()
            return {'ingress': {'Received': len(calls)}}
        collector = e2.EqualSamplesCompactSampler(scrape=scrape, interval=0.1,
            baseline_metrics={}, deadline_seconds=5)
        collector.start_manual()
        collector.start()
        side = e2.IndependentSideSampler(reader={'postgres': slow, 'linux': lambda: {'ok': True}}, interval=0.01)
        collector.side_sampler = side
        side.start()
        try:
            self.assertTrue(entered.wait(1))
            self.assertTrue(second_scrape.wait(1))
            self.assertGreaterEqual(len(collector.retained_snapshots), 1)
        finally:
            release.set()
            self.assertTrue(collector.stop(timeout_seconds=2))

    def test_side_sampler_has_independent_count_and_byte_eviction_caps(self):
        sampler = e2.IndependentSideSampler(reader={
            'postgres': lambda: {'data': 'x' * 80}, 'linux': lambda: {'ok': True}},
            max_samples=2, byte_cap=1200)
        for _ in range(5):
            sampler.sample_step()
        coverage = sampler.coverage()
        self.assertEqual(coverage['samples_retained'], 2)
        self.assertEqual(coverage['samples_evicted'], 3)
        self.assertLessEqual(coverage['ring_bytes_used'], 1200)
        self.assertEqual(coverage['ring_bytes_used'], sum(len(json.dumps(row, separators=(',', ':')).encode()) for row in sampler.samples))
        with self.assertRaises(ValueError): e2.IndependentSideSampler(reader={}, max_samples=0)
        with self.assertRaises(ValueError): e2.IndependentSideSampler(reader={}, byte_cap=0)
        self.assertEqual(sampler.stop(timeout_seconds=0), True)
        self.assertEqual(sampler.stop(timeout_seconds=0), True)

    # === STEP 2: Official E2 active capture wiring tests ===

    def test_official_steady_burst_creates_side_sampler_with_capture_seconds_from_phases(self):
        """Official steady_burst profile creates side sampler with capture_seconds = max(phase durations) + 10."""
        prepared = {'phase_id': 'samples-100', 'workload': 'steady_burst', 'profile': 'official',
            'manifests': {'A': {'phases': {'warmup_seconds': 5, 'measure_seconds': 10, 'drain_seconds': 30, 'cooldown_seconds': 0}},
                          'B': {'phases': {'warmup_seconds': 0, 'measure_seconds': 8, 'drain_seconds': 20, 'cooldown_seconds': 2}}},
            'session_deadline_monotonic': time.monotonic() + 3600,
            'pg': object(), 'specs': [SimpleNamespace(), SimpleNamespace()], 'tokens': {'admin': 'x'}}
        smoke = SimpleNamespace(paper_p1_count_db=Mock(return_value={}),
            _admin_metrics=Mock(return_value={'ingress': {'Received': 0, 'process_instance': 'proc-123'}}),
            paper_p0_stats=Mock(return_value={}))
        registry = object()
        containers = {'postgres': {'kind': 'container', 'receipt': {'id': 'pg'}}}
        with patch.object(e2._support, '_admin_metrics', return_value={'ingress': {'Received': 0, 'process_instance': 'proc-123'}}), \
             patch.object(e2, '_target_counts', return_value={}), \
             patch.object(e2, 'read_container_resources', return_value={}), \
             patch.object(e2, 'read_postgres_diagnostics', return_value={}), \
             patch.object(e2, 'read_linux_resources', return_value={}):
            e2.begin_remote_collection(smoke, SimpleNamespace(), object(), registry, containers,
                                       prepared, phase_id='samples-100')
        collector = prepared['collector']
        side = collector.side_sampler
        self.assertIsNotNone(side)
        # Phase A: 5+10+30+0 = 45, Phase B: 0+8+20+2 = 30, max = 45, +10 = 55
        self.assertEqual(side.capture_seconds, 55)
        self.assertEqual(side.max_samples, 128)
        self.assertEqual(side.byte_cap, 2 * 1024 * 1024)
        self.assertFalse(side._legacy_mode)
        collector.stop(timeout_seconds=0.1)
        side.stop(timeout_seconds=0.1)

    def test_official_steady_burst_uses_remaining_session_deadline_for_side_sampler(self):
        """Side sampler deadline uses remaining session budget, not stale pre-baseline budget."""
        # session_deadline_monotonic = now + 100s, but baseline reads take ~5s
        # Remaining should be ~95s, not the original 100s or the workload-only duration
        session_deadline = time.monotonic() + 100
        prepared = {'phase_id': 'samples-100', 'workload': 'steady_burst', 'profile': 'official',
            'manifests': {'A': {'phases': {'measure_seconds': 10, 'drain_seconds': 30}},
                          'B': {'phases': {'measure_seconds': 10, 'drain_seconds': 30}}},
            'session_deadline_monotonic': session_deadline,
            'pg': object(), 'specs': [SimpleNamespace(), SimpleNamespace()], 'tokens': {'admin': 'x'}}
        smoke = SimpleNamespace(paper_p1_count_db=Mock(return_value={}),
            _admin_metrics=Mock(return_value={'ingress': {'Received': 0, 'process_instance': 'proc-123'}}),
            paper_p0_stats=Mock(return_value={}))
        registry = object()
        containers = {'postgres': {'kind': 'container', 'receipt': {'id': 'pg'}}}
        with patch.object(e2._support, '_admin_metrics', return_value={'ingress': {'Received': 0, 'process_instance': 'proc-123'}}), \
             patch.object(e2, '_target_counts', return_value={}), \
             patch.object(e2, 'read_container_resources', return_value={}), \
             patch.object(e2, 'read_postgres_diagnostics', return_value={}), \
             patch.object(e2, 'read_linux_resources', return_value={}):
            before_begin = time.monotonic()
            e2.begin_remote_collection(smoke, SimpleNamespace(), object(), registry, containers,
                                       prepared, phase_id='samples-100')
        collector = prepared['collector']
        side = collector.side_sampler
        # Deadline should be based on remaining session time, capped at 86400
        # capture_seconds = 10+30+10 = 50, deadline should be >= capture_seconds
        self.assertGreaterEqual(side.deadline - side._start_time, 50)
        self.assertLessEqual(side.deadline - side._start_time, 100)
        collector.stop(timeout_seconds=0.1)
        side.stop(timeout_seconds=0.1)

    def test_pilot_profile_no_capture_seconds_legacy_mode(self):
        """Pilot profile preserves legacy rolling semantics (no capture_seconds)."""
        prepared = {'phase_id': 'samples-8', 'workload': 'steady_burst', 'profile': 'pilot',
            'manifests': {'A': {'phases': {'measure_seconds': 8, 'drain_seconds': 12}},
                          'B': {'phases': {'measure_seconds': 4, 'drain_seconds': 12}}},
            'session_deadline_monotonic': time.monotonic() + 3600,
            'pg': object(), 'specs': [SimpleNamespace(), SimpleNamespace()], 'tokens': {'admin': 'x'}}
        smoke = SimpleNamespace(paper_p1_count_db=Mock(return_value={}),
            _admin_metrics=Mock(return_value={'ingress': {'Received': 0}}),
            paper_p0_stats=Mock(return_value={}))
        registry = object()
        containers = {}
        with patch.object(e2._support, '_admin_metrics', return_value={'ingress': {'Received': 0}}), \
             patch.object(e2, '_target_counts', return_value={}), \
             patch.object(e2, 'read_container_resources', return_value={}):
            e2.begin_remote_collection(smoke, SimpleNamespace(), object(), registry, containers,
                                       prepared, phase_id='samples-8')
        collector = prepared['collector']
        self.assertIsNone(collector.side_sampler)  # Pilot doesn't get side sampler
        collector.stop(timeout_seconds=0.1)

    def test_equal_samples_workload_no_capture_seconds(self):
        """Equal samples workload preserves legacy rolling semantics."""
        prepared = {'phase_id': 'equal-samples-1', 'workload': 'equal_samples', 'profile': 'pilot',
            'manifests': {'A': {'phases': {'measure_seconds': 60, 'drain_seconds': 20}},
                          'B': {'phases': {'measure_seconds': 60, 'drain_seconds': 20}}},
            'session_deadline_monotonic': time.monotonic() + 3600,
            'pg': object(), 'specs': [SimpleNamespace(), SimpleNamespace()], 'tokens': {'admin': 'x'}}
        smoke = SimpleNamespace(paper_p1_count_db=Mock(return_value={}),
            _admin_metrics=Mock(return_value={'ingress': {'Received': 0}}),
            paper_p0_stats=Mock(return_value={}))
        registry = object()
        containers = {}
        with patch.object(e2._support, '_admin_metrics', return_value={'ingress': {'Received': 0}}), \
             patch.object(e2, '_target_counts', return_value={}), \
             patch.object(e2, 'read_container_resources', return_value={}):
            e2.begin_remote_collection(smoke, SimpleNamespace(), object(), registry, containers,
                                       prepared, phase_id='equal-samples-1')
        collector = prepared['collector']
        self.assertIsNone(collector.side_sampler)  # Equal samples doesn't get side sampler
        collector.stop(timeout_seconds=0.1)

    def test_standalone_side_sampler_without_capture_seconds_preserves_rolling(self):
        """Standalone IndependentSideSampler without capture_seconds preserves rolling behavior."""
        sampler = e2.IndependentSideSampler(reader={
            'postgres': lambda: {}, 'linux': lambda: {}, 'docker': lambda: {}},
            max_samples=64, byte_cap=512 * 1024)
        self.assertTrue(sampler._legacy_mode)
        self.assertIsNone(sampler.capture_seconds)
        self.assertEqual(sampler.max_samples, 64)
        self.assertEqual(sampler.byte_cap, 512 * 1024)
        self.assertEqual(sampler.stop(timeout_seconds=0), True)

    def test_sampler_notifies_side_observe_ingress_before_resource_reader(self):
        """EqualSamplesCompactSampler.sample_step calls side.observe_ingress for every valid HTTP scrape BEFORE resource reader."""
        observed_snapshots = []
        resource_called = []
        clock = {'value': 10.0}
        def scrape():
            return {'process_instance': 'proc-123', 'ingress': {'Received': 1},
                    'persistence': {}, 'db': {}}
        def slow_resource():
            resource_called.append(clock['value'])
            clock['value'] = 99.0
            time.sleep(0.01)
            return {}
        def mock_observe(snapshot):
            observed_snapshots.append((snapshot.get('process_instance'), snapshot.get('ingress', {}).get('Received'),
                                       snapshot.get('scrape_finished_monotonic')))
        collector = e2.EqualSamplesCompactSampler(scrape=scrape, resource_reader=slow_resource,
            interval=0.1, max_snapshots=8, byte_cap=4096, deadline_seconds=100,
            time_fn=lambda: clock['value'],
            wall_fn=lambda: '2026-11-07T00:00:00Z',
            baseline_metrics={'ingress': {'Received': 0}, 'process_instance': 'proc-123'})
        collector.side_sampler = Mock()
        collector.side_sampler.observe_ingress = mock_observe
        collector._started_monotonic = 10.0
        collector.sample_step()
        # observe_ingress called once with correct snapshot
        self.assertEqual(len(observed_snapshots), 1)
        proc_inst, received, scrape_mono = observed_snapshots[0]
        self.assertEqual(proc_inst, 'proc-123')
        self.assertEqual(received, 1)
        self.assertEqual(scrape_mono, 10.0)  # Captured BEFORE slow resource reader
        # Resource reader was called AFTER observe_ingress
        self.assertEqual(len(resource_called), 1)

    def test_sampler_notifies_side_even_when_ring_rejects_oversized_entry(self):
        """HTTP observation notification occurs even when metrics ring rejects oversized entry."""
        observed = []
        def scrape():
            # Large payload that exceeds byte_cap
            return {'process_instance': 'proc-123', 'ingress': {'Received': 1, 'data': 'x' * 5000},
                    'persistence': {}, 'db': {}}
        def mock_observe(snapshot):
            observed.append(snapshot.get('ingress', {}).get('Received'))
        collector = e2.EqualSamplesCompactSampler(scrape=scrape, resource_reader=lambda: {},
            interval=0.1, max_snapshots=8, byte_cap=1024, deadline_seconds=100,
            time_fn=lambda: 10.0,
            wall_fn=lambda: '2026-11-07T00:00:00Z',
            baseline_metrics={'ingress': {'Received': 0}, 'process_instance': 'proc-123'})
        collector.side_sampler = Mock()
        collector.side_sampler.observe_ingress = mock_observe
        collector._started_monotonic = 10.0
        collector.sample_step()
        # Ring rejected oversized entry, but side was still notified
        self.assertEqual(len(observed), 1)
        self.assertEqual(observed[0], 1)
        self.assertEqual(len(collector.retained_snapshots), 0)  # Ring empty due to rejection
        self.assertIn('collector_snapshot_exceeds_byte_cap', collector.errors)

    def test_sampler_notifies_side_on_every_valid_scrape_not_just_first(self):
        """Side sampler notified on EVERY valid projected HTTP scrape, not just trigger."""
        observed = []
        call_count = {'n': 0}
        clock = {'value': 10.0}
        def scrape():
            call_count['n'] += 1
            return {'process_instance': 'proc-123', 'ingress': {'Received': call_count['n']},
                    'persistence': {}, 'db': {}}
        def mock_observe(snapshot):
            observed.append(snapshot.get('ingress', {}).get('Received'))
        collector = e2.EqualSamplesCompactSampler(scrape=scrape, resource_reader=lambda: {},
            interval=0.1, max_snapshots=100, byte_cap=64 * 1024, deadline_seconds=100,
            time_fn=lambda: clock['value'],
            wall_fn=lambda: '2026-11-07T00:00:00Z',
            baseline_metrics={'ingress': {'Received': 0}, 'process_instance': 'proc-123'})
        collector.side_sampler = Mock()
        collector.side_sampler.observe_ingress = mock_observe
        collector._started_monotonic = 10.0
        for i in range(5):
            collector.sample_step()
        # All 5 scrapes notified
        self.assertEqual(observed, [1, 2, 3, 4, 5])
        self.assertEqual(len(collector.retained_snapshots), 5)

    def test_sampler_does_not_notify_on_failed_http_scrape(self):
        """Missing/failed HTTP scrape does not fabricate counter or trigger side notification."""
        observed = []
        def scrape():
            raise RuntimeError('HTTP failed')
        def mock_observe(snapshot):
            observed.append(snapshot)
        collector = e2.EqualSamplesCompactSampler(scrape=scrape, resource_reader=lambda: {},
            interval=0.1, max_snapshots=8, byte_cap=4096, deadline_seconds=100,
            time_fn=lambda: 10.0,
            wall_fn=lambda: '2026-11-07T00:00:00Z',
            baseline_metrics={'ingress': {'Received': 0}, 'process_instance': 'proc-123'})
        collector.side_sampler = Mock()
        collector.side_sampler.observe_ingress = mock_observe
        collector._started_monotonic = 10.0
        collector.sample_step()
        # No notification on failed scrape
        self.assertEqual(len(observed), 0)
        self.assertIn('scrape_RuntimeError', collector.errors)

    def test_sampler_does_not_notify_on_missing_ingress_in_scrape(self):
        """Scrape without ingress dict does not trigger side notification."""
        observed = []
        def scrape():
            return {'persistence': {}, 'db': {}}  # No ingress
        def mock_observe(snapshot):
            observed.append(snapshot)
        collector = e2.EqualSamplesCompactSampler(scrape=scrape, resource_reader=lambda: {},
            interval=0.1, max_snapshots=8, byte_cap=4096, deadline_seconds=100,
            time_fn=lambda: 10.0,
            wall_fn=lambda: '2026-11-07T00:00:00Z',
            baseline_metrics={'ingress': {'Received': 0}, 'process_instance': 'proc-123'})
        collector.side_sampler = Mock()
        collector.side_sampler.observe_ingress = mock_observe
        collector._started_monotonic = 10.0
        collector.sample_step()
        # No notification - ingress missing from projection
        self.assertEqual(len(observed), 0)

    def test_side_observe_ingress_equal_received_is_idle_no_trigger(self):
        """Equal Received = idle (no trigger, no error)."""
        side = e2.IndependentSideSampler(reader={
            'postgres': lambda: {}, 'linux': lambda: {}, 'docker': lambda: {}},
            max_samples=128, byte_cap=2 * 1024 * 1024,
            capture_seconds=50,
            baseline_metrics={'ingress': {'Received': 10}, 'process_instance': 'proc-123'},
            deadline_seconds=100)
        # Observe with same Received = baseline (idle)
        side.observe_ingress({'process_instance': 'proc-123', 'ingress': {'Received': 10},
                              'scrape_finished_monotonic': 20.0, 'scrape_finished_timestamp': '2026-11-07T00:00:00Z'})
        self.assertFalse(side._triggered)
        self.assertEqual(side._state, 'waiting')
        self.assertNotIn('counter_reset', side._qualification_reasons)
        self.assertEqual(side.stop(timeout_seconds=0), True)

    def test_side_observe_ingress_missing_baseline_qualified_not_error(self):
        """Missing baseline -> active mode remains bounded waiting with qualification 'baseline_unavailable'.

        capture_seconds alone selects active mode; baseline_metrics=None is a
        diagnostics qualification, not legacy. observe_ingress cannot trigger
        without a valid baseline even if Received>0. Coverage always reports
        mode='active_capture', trigger_observed=False, complete=False.
        """
        # Side sampler with capture_seconds but NO baseline_metrics
        side = e2.IndependentSideSampler(reader={
            'postgres': lambda: {}, 'linux': lambda: {}, 'docker': lambda: {}},
            max_samples=128, byte_cap=2 * 1024 * 1024,
            capture_seconds=50,
            baseline_metrics=None,  # No baseline!
            deadline_seconds=100)
        # Active mode selected by capture_seconds alone (baseline absence is qualification)
        self.assertFalse(side._legacy_mode)
        self.assertIsNotNone(side.capture_seconds)
        self.assertIsNone(side.baseline_metrics)
        # Observe with valid data - should not trigger because no baseline
        side.observe_ingress({'process_instance': 'proc-123', 'ingress': {'Received': 10},
                              'scrape_finished_monotonic': 20.0, 'scrape_finished_timestamp': '2026-11-07T00:00:00Z'})
        self.assertFalse(side._triggered)
        self.assertEqual(side._state, 'waiting')
        # Sticky qualification recorded
        self.assertIn('baseline_unavailable', side._qualification_reasons)
        self.assertIn('baseline_unavailable', side.errors)
        # stop before trigger -> no_trigger
        self.assertEqual(side.stop(timeout_seconds=0), True)
        self.assertEqual(side._state, 'no_trigger')
        # Coverage: active mode, no trigger, incomplete
        cov = side.coverage()
        self.assertEqual(cov['mode'], 'active_capture')
        self.assertEqual(cov['state'], 'no_trigger')
        self.assertFalse(cov.get('trigger_observed', True))
        self.assertFalse(cov.get('complete', True))
        self.assertIn('baseline_unavailable', cov.get('qualifications', []))
        # Old count/bytes fields preserved
        self.assertIn('samples_captured', cov)
        self.assertIn('samples_retained', cov)
        self.assertIn('samples_evicted', cov)

    def test_side_observe_ingress_invalid_baseline_qualified(self):
        """Invalid baseline (missing process_instance) -> qualification, no trigger."""
        side = e2.IndependentSideSampler(reader={
            'postgres': lambda: {}, 'linux': lambda: {}, 'docker': lambda: {}},
            max_samples=128, byte_cap=2 * 1024 * 1024,
            capture_seconds=50,
            baseline_metrics={'ingress': {'Received': 10}},  # Missing process_instance
            deadline_seconds=100)
        self.assertFalse(side._legacy_mode)  # Has capture_seconds and baseline_metrics dict
        side.observe_ingress({'process_instance': 'proc-123', 'ingress': {'Received': 15},
                              'scrape_finished_monotonic': 20.0, 'scrape_finished_timestamp': '2026-11-07T00:00:00Z'})
        self.assertFalse(side._triggered)
        self.assertIn('baseline_missing_process_instance', side._qualification_reasons)
        self.assertEqual(side._state, 'waiting')
        self.assertEqual(side.stop(timeout_seconds=0), True)

    def test_side_observe_ingress_process_instance_changed_qualification(self):
        """Process instance change -> qualification, no trigger."""
        side = e2.IndependentSideSampler(reader={
            'postgres': lambda: {}, 'linux': lambda: {}, 'docker': lambda: {}},
            max_samples=128, byte_cap=2 * 1024 * 1024,
            capture_seconds=50,
            baseline_metrics={'ingress': {'Received': 10}, 'process_instance': 'proc-baseline'},
            deadline_seconds=100)
        side.observe_ingress({'process_instance': 'proc-different', 'ingress': {'Received': 15},
                              'scrape_finished_monotonic': 20.0, 'scrape_finished_timestamp': '2026-11-07T00:00:00Z'})
        self.assertFalse(side._triggered)
        self.assertIn('process_instance_changed', side._qualification_reasons)
        self.assertEqual(side._state, 'waiting')
        self.assertEqual(side.stop(timeout_seconds=0), True)

    def test_side_observe_ingress_counter_reset_qualification(self):
        """Decrease from last observation or baseline -> counter_reset qualification."""
        side = e2.IndependentSideSampler(reader={
            'postgres': lambda: {}, 'linux': lambda: {}, 'docker': lambda: {}},
            max_samples=128, byte_cap=2 * 1024 * 1024,
            capture_seconds=50,
            baseline_metrics={'ingress': {'Received': 100}, 'process_instance': 'proc-123'},
            deadline_seconds=100)
        # First observation: increase (trigger)
        side.observe_ingress({'process_instance': 'proc-123', 'ingress': {'Received': 150},
                              'scrape_finished_monotonic': 20.0, 'scrape_finished_timestamp': '2026-11-07T00:00:00Z'})
        self.assertTrue(side._triggered)
        # Second observation: decrease (counter reset)
        side.observe_ingress({'process_instance': 'proc-123', 'ingress': {'Received': 100},
                              'scrape_finished_monotonic': 21.0, 'scrape_finished_timestamp': '2026-11-07T00:00:01Z'})
        self.assertIn('counter_reset', side._qualification_reasons)
        self.assertEqual(side.stop(timeout_seconds=0), True)

    def test_blocked_pg_reader_does_not_block_multiple_http_scrapes(self):
        """Slow/blocked PG reader does not block multiple HTTP scrapes from notifying side."""
        observed = []
        scrape_times = []
        pg_started = threading.Event()
        pg_release = threading.Event()
        clock = {'value': 10.0}
        def slow_pg():
            pg_started.set()
            pg_release.wait(2)
            return {'data': 'pg'}
        def scrape():
            scrape_times.append(clock['value'])
            return {'process_instance': 'proc-123', 'ingress': {'Received': len(scrape_times)},
                    'persistence': {}, 'db': {}}
        def mock_observe(snapshot):
            observed.append(snapshot.get('ingress', {}).get('Received'))
        collector = e2.EqualSamplesCompactSampler(scrape=scrape, resource_reader=slow_pg,
            interval=0.1, max_snapshots=100, byte_cap=64 * 1024, deadline_seconds=100,
            time_fn=lambda: clock['value'],
            wall_fn=lambda: '2026-11-07T00:00:00Z',
            baseline_metrics={'ingress': {'Received': 0}, 'process_instance': 'proc-123'})
        collector.side_sampler = Mock()
        collector.side_sampler.observe_ingress = mock_observe
        collector._started_monotonic = 10.0
        # Don't start collector thread - call sample_step manually
        for _ in range(3):
            collector.sample_step()
            clock['value'] += 0.1
        # All 3 scrapes notified side even though PG is blocked
        self.assertEqual(len(observed), 3)
        self.assertEqual(observed, [1, 2, 3])

    def test_side_capture_seconds_calculation_unequal_arms_chooses_longer(self):
        """Unequal phase arms choose the longer declared duration."""
        prepared = {'phase_id': 'samples-100', 'workload': 'steady_burst', 'profile': 'official',
            'manifests': {'A': {'phases': {'warmup_seconds': 0, 'measure_seconds': 10, 'drain_seconds': 30, 'cooldown_seconds': 0}},
                          'B': {'phases': {'warmup_seconds': 0, 'measure_seconds': 20, 'drain_seconds': 40, 'cooldown_seconds': 0}}},
            'session_deadline_monotonic': time.monotonic() + 3600,
            'pg': object(), 'specs': [SimpleNamespace(), SimpleNamespace()], 'tokens': {'admin': 'x'}}
        smoke = SimpleNamespace(paper_p1_count_db=Mock(return_value={}),
            _admin_metrics=Mock(return_value={'ingress': {'Received': 0, 'process_instance': 'proc-123'}}),
            paper_p0_stats=Mock(return_value={}))
        registry = object()
        containers = {'postgres': {'kind': 'container', 'receipt': {'id': 'pg'}}}
        with patch.object(e2._support, '_admin_metrics', return_value={'ingress': {'Received': 0, 'process_instance': 'proc-123'}}), \
             patch.object(e2, '_target_counts', return_value={}), \
             patch.object(e2, 'read_container_resources', return_value={}), \
             patch.object(e2, 'read_postgres_diagnostics', return_value={}), \
             patch.object(e2, 'read_linux_resources', return_value={}):
            e2.begin_remote_collection(smoke, SimpleNamespace(), object(), registry, containers,
                                       prepared, phase_id='samples-100')
        side = prepared['collector'].side_sampler
        # Arm A: 10+30=40, Arm B: 20+40=60, max=60, +10=70
        self.assertEqual(side.capture_seconds, 70)
        prepared['collector'].stop(timeout_seconds=0.1)
        side.stop(timeout_seconds=0.1)

    def test_side_capture_seconds_includes_warmup_and_cooldown(self):
        """Capture seconds includes warmup and cooldown if non-zero."""
        prepared = {'phase_id': 'samples-100', 'workload': 'steady_burst', 'profile': 'official',
            'manifests': {'A': {'phases': {'warmup_seconds': 5, 'measure_seconds': 10, 'drain_seconds': 30, 'cooldown_seconds': 5}},
                          'B': {'phases': {'warmup_seconds': 0, 'measure_seconds': 10, 'drain_seconds': 30, 'cooldown_seconds': 0}}},
            'session_deadline_monotonic': time.monotonic() + 3600,
            'pg': object(), 'specs': [SimpleNamespace(), SimpleNamespace()], 'tokens': {'admin': 'x'}}
        smoke = SimpleNamespace(paper_p1_count_db=Mock(return_value={}),
            _admin_metrics=Mock(return_value={'ingress': {'Received': 0, 'process_instance': 'proc-123'}}),
            paper_p0_stats=Mock(return_value={}))
        registry = object()
        containers = {'postgres': {'kind': 'container', 'receipt': {'id': 'pg'}}}
        with patch.object(e2._support, '_admin_metrics', return_value={'ingress': {'Received': 0, 'process_instance': 'proc-123'}}), \
             patch.object(e2, '_target_counts', return_value={}), \
             patch.object(e2, 'read_container_resources', return_value={}), \
             patch.object(e2, 'read_postgres_diagnostics', return_value={}), \
             patch.object(e2, 'read_linux_resources', return_value={}):
            e2.begin_remote_collection(smoke, SimpleNamespace(), object(), registry, containers,
                                       prepared, phase_id='samples-100')
        side = prepared['collector'].side_sampler
        # Arm A: 5+10+30+5=50, Arm B: 0+10+30+0=40, max=50, +10=60
        self.assertEqual(side.capture_seconds, 60)
        prepared['collector'].stop(timeout_seconds=0.1)
        side.stop(timeout_seconds=0.1)

    def test_sampler_actual_http_observation_triggers_at_scrape_finish_even_if_resource_reader_advances_time(self):
        """HTTP observation triggers at scrape finish time, even if resource reader later advances clock."""
        clock = {'value': 10.0}
        observed_scrape_times = []
        def scrape():
            return {'process_instance': 'proc-123', 'ingress': {'Received': 1},
                    'persistence': {}, 'db': {}}
        def slow_resource():
            # Advance clock during resource read
            clock['value'] = 99.0
            return {}
        def mock_observe(snapshot):
            observed_scrape_times.append(snapshot.get('scrape_finished_monotonic'))
        collector = e2.EqualSamplesCompactSampler(scrape=scrape, resource_reader=slow_resource,
            interval=0.1, max_snapshots=8, byte_cap=4096, deadline_seconds=100,
            time_fn=lambda: clock['value'],
            wall_fn=lambda: '2026-11-07T00:00:00Z',
            baseline_metrics={'ingress': {'Received': 0}, 'process_instance': 'proc-123'})
        collector.side_sampler = Mock()
        collector.side_sampler.observe_ingress = mock_observe
        collector._started_monotonic = 10.0
        collector.sample_step()
        # Scrape finished time captured BEFORE resource reader advanced clock
        self.assertEqual(observed_scrape_times, [10.0])
        self.assertEqual(clock['value'], 99.0)  # Clock advanced by resource reader

    def test_official_side_sampler_has_128_samples_and_2mib_cap(self):
        """Official side sampler has 128 total samples and 2MiB cap (pretrigger 4/64KiB included)."""
        prepared = {'phase_id': 'samples-100', 'workload': 'steady_burst', 'profile': 'official',
            'manifests': {'A': {'phases': {'measure_seconds': 10, 'drain_seconds': 30}},
                          'B': {'phases': {'measure_seconds': 10, 'drain_seconds': 30}}},
            'session_deadline_monotonic': time.monotonic() + 3600,
            'pg': object(), 'specs': [SimpleNamespace(), SimpleNamespace()], 'tokens': {'admin': 'x'}}
        smoke = SimpleNamespace(paper_p1_count_db=Mock(return_value={}),
            _admin_metrics=Mock(return_value={'ingress': {'Received': 0, 'process_instance': 'proc-123'}}),
            paper_p0_stats=Mock(return_value={}))
        registry = object()
        containers = {'postgres': {'kind': 'container', 'receipt': {'id': 'pg'}}}
        with patch.object(e2._support, '_admin_metrics', return_value={'ingress': {'Received': 0, 'process_instance': 'proc-123'}}), \
             patch.object(e2, '_target_counts', return_value={}), \
             patch.object(e2, 'read_container_resources', return_value={}), \
             patch.object(e2, 'read_postgres_diagnostics', return_value={}), \
             patch.object(e2, 'read_linux_resources', return_value={}):
            e2.begin_remote_collection(smoke, SimpleNamespace(), object(), registry, containers,
                                       prepared, phase_id='samples-100')
        side = prepared['collector'].side_sampler
        self.assertEqual(side.max_samples, 128)
        self.assertEqual(side.byte_cap, 2 * 1024 * 1024)
        # Pre-trigger limit should be min(4, 128) = 4 samples and min(64KiB, 2MiB) = 64KiB
        prepared['collector'].stop(timeout_seconds=0.1)
        side.stop(timeout_seconds=0.1)

    def test_http_ring_max3600_2mib_unchanged(self):
        """HTTP ring max 3600/2MiB and intervals unchanged."""
        collector = e2.EqualSamplesCompactSampler(scrape=lambda: {'ingress': {}},
            interval=1.0, max_snapshots=3600, byte_cap=2 * 1024 * 1024, deadline_seconds=1800)
        self.assertEqual(collector.max_snapshots, 3600)
        self.assertEqual(collector.byte_cap, 2 * 1024 * 1024)
        self.assertEqual(collector.interval, 1.0)

    def test_side_sampler_deadline_independently_bounds_actual_capture_end(self):
        """Session deadline independently bounds actual capture end; delayed begin doesn't eat capture window."""
        # session_deadline = now + 60s, but begin happens at now + 5s
        # capture_seconds = 50s, so capture_end = trigger + 50s
        # If trigger at now + 5s, capture_end = now + 55s < session_deadline (now + 60s) -> OK
        session_deadline = time.monotonic() + 60
        prepared = {'phase_id': 'samples-100', 'workload': 'steady_burst', 'profile': 'official',
            'manifests': {'A': {'phases': {'measure_seconds': 10, 'drain_seconds': 30}},
                          'B': {'phases': {'measure_seconds': 10, 'drain_seconds': 30}}},
            'session_deadline_monotonic': session_deadline,
            'pg': object(), 'specs': [SimpleNamespace(), SimpleNamespace()], 'tokens': {'admin': 'x'}}
        smoke = SimpleNamespace(paper_p1_count_db=Mock(return_value={}),
            _admin_metrics=Mock(return_value={'ingress': {'Received': 0, 'process_instance': 'proc-123'}}),
            paper_p0_stats=Mock(return_value={}))
        registry = object()
        containers = {'postgres': {'kind': 'container', 'receipt': {'id': 'pg'}}}
        with patch.object(e2._support, '_admin_metrics', return_value={'ingress': {'Received': 0, 'process_instance': 'proc-123'}}), \
             patch.object(e2, '_target_counts', return_value={}), \
             patch.object(e2, 'read_container_resources', return_value={}), \
             patch.object(e2, 'read_postgres_diagnostics', return_value={}), \
             patch.object(e2, 'read_linux_resources', return_value={}):
            # Simulate 5s delay before begin
            time.sleep(0.01)
            e2.begin_remote_collection(smoke, SimpleNamespace(), object(), registry, containers,
                                       prepared, phase_id='samples-100')
        side = prepared['collector'].side_sampler
        # capture_seconds = 10+30+10 = 50
        # deadline should allow full capture window
        self.assertGreaterEqual(side.deadline - side._start_time, 50)
        self.assertLessEqual(side.deadline - side._start_time, 60)
        prepared['collector'].stop(timeout_seconds=0.1)
        side.stop(timeout_seconds=0.1)

    def test_begin_delay_and_slow_diagnostic_baseline_reduce_remaining_deadline_not_capture_duration(self):
        """Begin delay and slow diagnostic baseline reads reduce remaining deadline but not requested capture duration."""
        session_deadline = time.monotonic() + 100
        prepared = {'phase_id': 'samples-100', 'workload': 'steady_burst', 'profile': 'official',
            'manifests': {'A': {'phases': {'measure_seconds': 10, 'drain_seconds': 30}},
                          'B': {'phases': {'measure_seconds': 10, 'drain_seconds': 30}}},
            'session_deadline_monotonic': session_deadline,
            'pg': object(), 'specs': [SimpleNamespace(), SimpleNamespace()], 'tokens': {'admin': 'x'}}
        # Slow baseline read
        slow_baseline_done = threading.Event()
        def slow_admin_metrics(*args, **kwargs):
            time.sleep(0.02)  # Simulate slow baseline
            slow_baseline_done.set()
            return {'ingress': {'Received': 0, 'process_instance': 'proc-123'}}
        smoke = SimpleNamespace(paper_p1_count_db=Mock(return_value={}),
            _admin_metrics=slow_admin_metrics,
            paper_p0_stats=Mock(return_value={}))
        registry = object()
        containers = {'postgres': {'kind': 'container', 'receipt': {'id': 'pg'}}}
        with patch.object(e2._support, '_admin_metrics', side_effect=slow_admin_metrics), \
             patch.object(e2, '_target_counts', return_value={}), \
             patch.object(e2, 'read_container_resources', return_value={}), \
             patch.object(e2, 'read_postgres_diagnostics', return_value={}), \
             patch.object(e2, 'read_linux_resources', return_value={}):
            before = time.monotonic()
            e2.begin_remote_collection(smoke, SimpleNamespace(), object(), registry, containers,
                                       prepared, phase_id='samples-100')
        side = prepared['collector'].side_sampler
        # capture_seconds unchanged at 50
        self.assertEqual(side.capture_seconds, 50)
        # But deadline reduced by baseline time
        elapsed = time.monotonic() - before
        self.assertLess(side.deadline - side._start_time, 100)
        self.assertGreaterEqual(side.deadline - side._start_time, 50)
        prepared['collector'].stop(timeout_seconds=0.1)
        side.stop(timeout_seconds=0.1)

    # === End STEP 2 tests ===

    def test_side_sampler_stop_interrupts_waiting_reader_sequence(self):
        entered, release = threading.Event(), threading.Event()
        calls = []
        def blocked():
            calls.append('postgres'); entered.set(); release.wait(2); return {'ok': True}
        sampler = e2.IndependentSideSampler(reader={'postgres': blocked,
            'linux': lambda: calls.append('linux'), 'docker': lambda: calls.append('docker')},
            interval=0.001, deadline_seconds=5)
        sampler.start()
        self.assertTrue(entered.wait(1))
        stopped = threading.Thread(target=lambda: sampler.stop(timeout_seconds=0.01))
        stopped.start(); stopped.join(0.2)
        release.set(); sampler._thread.join(1)
        self.assertEqual(calls, ['postgres'])

    def test_docker_socket_deadline_expiry_is_not_extended_during_recv(self):
        class FakeSocket:
            closed = False
            def settimeout(self, value): self.timeouts = getattr(self, 'timeouts', []) + [value]
            def connect(self, path): pass
            def sendall(self, request): pass
            def recv(self, size): return b''
            def close(self): self.closed = True
        sock = FakeSocket()
        with patch.object(e2.os.path, 'exists', return_value=True), \
             patch.object(e2.socket, 'socket', return_value=sock), \
             patch.object(e2.time, 'monotonic', side_effect=[0.0, 0.0, 1.0] + [1.0] * 20):
            result = e2.read_container_resources(None, {'backend': {'name': 'owned-id'}},
                socket_path='/fake/docker.sock', timeout=0.5)
        self.assertEqual(result['services']['backend']['reason'], 'docker_stats_read_failed_TimeoutError')
        self.assertTrue(sock.closed)
        self.assertEqual(len(sock.timeouts), 1)
    def test_backend_histogram_dto_is_projected_and_invalid_arrays_are_unavailable(self):
        upper_seconds = [0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05,
                         0.1, 0.25, 0.5, 1, 2.5, 5, 10]
        bucket_counts = [0] * 13 + [3]
        dto = {'ingress': {'Received': 4, 'Durations': {
            name: {'upper_seconds': upper_seconds, 'buckets': bucket_counts, 'count': 4,
                   'overflow': 1, 'sum_seconds': 0.25}
            for name in ('queue_wait', 'validation', 'persistence', 'ack_publish',
                         'batch_wait', 'group_persistence')}}}
        projected = e2.EqualSamplesCompactSampler._project_metrics(dto)['ingress']['durations']
        self.assertEqual(set(projected), {'queue_wait', 'validation', 'persistence',
            'ack_publish', 'batch_wait', 'group_persistence'})
        self.assertEqual(projected['ack_publish'], {'available': True,
            'upper_seconds': upper_seconds, 'buckets': bucket_counts, 'count': 4,
            'overflow': 1, 'sum_seconds': 0.25})
        for field, bad in (('buckets', [2, '1']), ('buckets', [2, 1] * 20),
                           ('upper_seconds', [0.1, float('nan')]),
                           ('sum_seconds', float('inf')), ('count', '4')):
            invalid = copy.deepcopy(dto)
            invalid['ingress']['Durations']['ack_publish'][field] = bad
            hist = e2.EqualSamplesCompactSampler._project_metrics(invalid)['ingress']['durations']['ack_publish']
            self.assertEqual(hist, {'available': False, 'reason': 'invalid'})
        missing = e2.EqualSamplesCompactSampler._project_metrics({'ingress': {}})
        self.assertEqual(missing['ingress']['durations']['queue_wait'],
                         {'available': False, 'reason': 'missing'})
        self.assertNotIn('secret', json.dumps(e2.EqualSamplesCompactSampler._project_metrics(
            {'ingress': {'Durations': {'queue_wait': {'secret': 'TOKEN'}}}})))

    def test_sampler_projection_and_failure_export_preserve_compact_persistence_metrics(self):
        bounds = [0.1, 1.0]
        hist = {'upper_seconds': bounds, 'buckets': [2, 1], 'count': 3,
                'overflow': 0, 'sum_seconds': 0.5}
        raw = {'ingress': {'Durations': {'queue_wait': hist}},
               'persistence': {'durations': {name: hist for name in
                   ('begin', 'gateway_lock', 'sensor_lock', 'graph_lookup', 'entity_lock',
                    'state_lock', 'ledger', 'copy', 'reported_state', 'temporal', 'commit')},
                   'secret': 'discard'},
               'db': {'acquired': 1, 'idle': 2, 'total': 3, 'max': 4,
                      'acquire_count': 5, 'empty_acquire_count': 6,
                      'canceled_acquire_count': 7, 'acquire_seconds': 0.25,
                      'password': 'discard'}}
        sampler = e2.EqualSamplesCompactSampler(scrape=lambda: raw, interval=0.1,
            max_snapshots=8, byte_cap=4096, deadline_seconds=10,
            time_fn=lambda: 5.0, wall_fn=lambda: '2026-11-07T00:00:00Z')
        sampler.sample_step()
        sample = sampler.retained_snapshots[0]
        self.assertEqual(sample['persistence']['durations']['commit']['count'], 3)
        self.assertEqual(sample['db']['acquired'], 1)
        self.assertIn('scrape_started_monotonic', sample)
        self.assertIn('scrape_finished_timestamp', sample)
        prepared = {'collector': sampler, 'phase_id': 'p', 'session_id': 's', 'run_id': 'r'}
        exported = e2.failure_diagnostics(prepared, 'ABORTED')['snapshots'][0]['snapshot']
        self.assertEqual(exported['ingress']['durations']['queue_wait']['count'], 3)
        self.assertEqual(exported['persistence']['durations']['commit']['count'], 3)
        self.assertEqual(exported['db']['acquired'], 1)
        self.assertNotIn('discard', json.dumps(exported))

    def test_sampler_deadline_uses_supervisor_session_lifetime_not_workload_duration(self):
        prepared = {'phase_id': 'samples-1', 'workload': 'steady_burst', 'profile': 'official',
            'manifests': {'A': {'phases': {'measure_seconds': 40, 'drain_seconds': 20}},
                          'B': {'phases': {'measure_seconds': 40, 'drain_seconds': 20}}},
            'session_deadline_monotonic': time.monotonic() + 3600,
            'pg': object(), 'specs': [SimpleNamespace(), SimpleNamespace()], 'tokens': {'admin': 'x'}}
        smoke = SimpleNamespace(paper_p1_count_db=Mock(return_value={}), paper_p0_stats=Mock(return_value={}))
        with patch.object(e2._support, '_admin_metrics', return_value={}), \
             patch.object(e2, '_target_counts', return_value={}), \
             patch.object(e2, 'read_container_resources', return_value={}):
            e2.begin_remote_collection(smoke, SimpleNamespace(), object(), object(), [], prepared, phase_id='samples-1')
        sampler = prepared['collector']
        self.assertGreater(sampler.deadline_seconds, 3500)
        time.sleep(0.01)  # Producer may start after begin; this delay must not consume workload-only budget.
        self.assertGreater(sampler.deadline_seconds, 3500)
        sampler.stop(timeout_seconds=0.1)

    def test_sampler_deadline_respects_short_remaining_supervisor_session(self):
        prepared = {'phase_id': 'samples-1', 'workload': 'steady_burst', 'profile': 'official',
            'manifests': {'A': {'phases': {'measure_seconds': 40, 'drain_seconds': 20}},
                          'B': {'phases': {'measure_seconds': 40, 'drain_seconds': 20}}},
            'session_deadline_monotonic': time.monotonic() + 5,
            'pg': object(), 'specs': [SimpleNamespace(), SimpleNamespace()], 'tokens': {'admin': 'x'}}
        smoke = SimpleNamespace(paper_p1_count_db=Mock(return_value={}), paper_p0_stats=Mock(return_value={}))
        with patch.object(e2._support, '_admin_metrics', return_value={}), \
             patch.object(e2, '_target_counts', return_value={}), \
             patch.object(e2, 'read_container_resources', return_value={}):
            e2.begin_remote_collection(smoke, SimpleNamespace(), object(), object(), [], prepared, phase_id='samples-1')
        self.assertLess(prepared['collector'].deadline_seconds, 5.1)
        prepared['collector'].stop(timeout_seconds=0.1)

    def test_source_fingerprint_includes_untracked_validator_bytes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            subprocess.run(['git', 'init', '-q', str(root)], check=True)
            subprocess.run(['git', '-C', str(root), 'config', 'user.email', 'test@example.invalid'], check=True)
            subprocess.run(['git', '-C', str(root), 'config', 'user.name', 'Test'], check=True)
            (root / 'tracked.py').write_text('base = 1\n')
            subprocess.run(['git', '-C', str(root), 'add', 'tracked.py'], check=True)
            subprocess.run(['git', '-C', str(root), 'commit', '-qm', 'fixture'], check=True)
            validator = root / 'untracked_validator.py'
            validator.write_text('value = 1\n')
            first = p8._source_fingerprint(root)
            validator.write_text('value = 2\n')
            second = p8._source_fingerprint(root)
            self.assertEqual(first['revision'], second['revision'])
            self.assertTrue(first['dirty'])
            self.assertNotEqual(first['dirty_source_sha256'], second['dirty_source_sha256'])

    def test_official_retry_policy_validation_is_strict_and_optional(self):
        policy = {'mode': 'bounded_backoff_v1', 'initial_interval_ms': 500,
                  'max_interval_ms': 8000, 'jitter_percent': 10,
                  'rate_per_second': 100, 'burst': 20}
        self.assertEqual(e2.validate_retry_policy(policy, pending_messages=20), policy)
        for bad in (None, {**policy, 'rate_per_second': True},
                    {**policy, 'extra': 1}, {**policy, 'max_interval_ms': 499},
                    {**policy, 'burst': 21}, {**policy, 'mode': 'fixed'}):
            with self.subTest(bad=bad), self.assertRaisesRegex(ValueError, 'e2_retry_policy_rejected'):
                e2.validate_retry_policy(bad, pending_messages=20)

    def test_retry_policy_output_proof_is_strict_but_legacy_is_optional(self):
        expected = {'mode':'bounded_backoff_v1','initial_interval_ms':500,
                    'max_interval_ms':8000,'jitter_percent':10,'rate_per_second':100,'burst':20}
        prepared = {'manifests': {'A': {'retry_policy': expected}, 'B': {'retry_policy': expected}}}
        good = {'A': {'retry_policy': expected}, 'B': {'retry_policy': expected}}
        self.assertEqual(e2.validate_applied_retry_policy(prepared, good), expected)
        for bad in ({'A': {}, 'B': good['B']},
                    {'A': {'retry_policy': dict(expected, burst=19)}, 'B': good['B']}):
            with self.subTest(bad=bad), self.assertRaisesRegex(ValueError, 'e2_retry_policy_applied_proof_mismatch'):
                e2.validate_applied_retry_policy(prepared, bad)
        self.assertIsNone(e2.validate_applied_retry_policy({'manifests': {'A': {}, 'B': {}}},
            {'A': {}, 'B': {}}))

    def test_estimator_includes_policy_queue_once_and_reports_shaper_headroom(self):
        policy = {'mode':'bounded_backoff_v1','initial_interval_ms':500,'max_interval_ms':8000,
                  'jitter_percent':10,'rate_per_second':500,'burst':100}
        for rate, duration, burst, expected_messages in ((1000,10,0,10000),
                (5000,2,0,10000),(20000,1,0,20000),(20000,1,10000,30000)):
            result = e2.e2_official_estimate(rate_per_second=rate, duration_seconds=duration,
                samples_per_message=100, payload_bytes=4096, official_opt_in=True,
                burst_messages=burst, pending_messages=22000, pending_bytes=32 << 20,
                retry_policy=policy)
            self.assertEqual(result['unique_messages_estimate'], expected_messages)
            self.assertEqual(result['retry_queue_logical_bytes_estimate'], 2*22000*16)
            self.assertEqual(result['pending_retry_memory_bytes_estimate'],
                             2*((32 << 20)+22000*4096)+2*22000*16)
            self.assertEqual(result['unique_offered_rate_per_producer'], rate/2)
            self.assertEqual(result['retry_attempt_rate_per_producer'], 500)
            self.assertEqual(result['retry_attempt_burst_per_producer'], 100)
            self.assertEqual(result['fixture_shaper_attempt_rate_per_gateway'], 500)
            self.assertEqual(result['fixture_shaper_attempt_burst_per_gateway'], 500)
            self.assertEqual(result['retry_headroom_not_reserved'], rate/2+500 > 500)
        with self.assertRaisesRegex(ValueError, 'e2_predicted_bound_exceeded'):
            e2.e2_official_estimate(rate_per_second=20000, duration_seconds=20,
                samples_per_message=100, payload_bytes=4096, official_opt_in=True,
                burst_messages=10000, pending_messages=22000, pending_bytes=32 << 20,
                retry_policy=policy)

    def setUp(self):
        self.targets = [{'gateway_id': 'gw-a', 'sensor_id': 's-a'},
                        {'gateway_id': 'gw-b', 'sensor_id': 's-b'}]
        self.profiles = e2_pilot_manifests(common={'tls': {'https_endpoint': 'https://localhost',
            'https_ca_file': 'ca.crt', 'mqtt_tls_endpoint': 'mqtts://localhost',
            'mqtt_tls_ca_file': 'mqtt-ca.crt'}}, targets=self.targets,
            gateway_identity_files={'gw-a': 'gw-a.json', 'gw-b': 'gw-b.json'},
            telemetry_identity_files={'A': 'identity-a.json', 'B': 'identity-b.json'})

    def test_failure_diagnostics_retains_sanitized_metrics(self):
        collector = SimpleNamespace(
            stop=Mock(return_value=True),
            metrics=[{'phase': 'workload_sample_1', 'snapshot': {
                'ingress': {'ShaperRateLimited': 7}, 'secret': 'must-not-leak'}}],
            errors=[])
        prepared = {'collector': collector, 'phase_id': 'samples-100',
                    'session_id': 'session', 'run_id': 'run'}
        result = e2.failure_diagnostics(prepared, 'ABORTED')
        self.assertFalse(result['full_p8_accepted'])
        self.assertEqual(result['snapshots'][0]['snapshot']['ingress']['ShaperRateLimited'], 7)
        self.assertNotIn('must-not-leak', json.dumps(result))
        self.assertEqual(collector.stop.call_count, 1)
        self.assertAlmostEqual(collector.stop.call_args.kwargs['timeout_seconds'], 15.0, places=2)

    def test_failure_diagnostics_does_not_read_live_collector(self):
        collector = SimpleNamespace(stop=Mock(return_value=False), errors=['collector_join_timeout'])
        result = e2.failure_diagnostics({'collector': collector, 'phase_id': 'p',
                                         'session_id': 's', 'run_id': 'r'}, 'ABORTED')
        self.assertEqual(result['snapshots'], [])
        self.assertEqual(result['snapshot_status'], 'collector_join_timeout')

    def test_flush_failure_diagnostics_collects_terminal_counts_and_is_idempotent(self):
        sampler = e2.EqualSamplesCompactSampler(scrape=lambda: {'ingress': {'Received': 3}},
            interval=0.1, max_snapshots=8, byte_cap=1024, deadline_seconds=10,
            baseline_metrics={'ingress': {'Received': 1}})
        sampler.start_manual()
        spec_a = SimpleNamespace(gateway_id='ga', sensor_id='sa', sensor_urn='urn:a')
        spec_b = SimpleNamespace(gateway_id='gb', sensor_id='sb', sensor_urn='urn:b')
        prepared = {'collector': sampler, 'phase_id': 'phase-safe', 'session_id': 'session-safe',
            'run_id': 'run-safe', 'specs': [spec_a, spec_b], 'before_targets': [
                {'ledger': 1, 'raw': 2, 'temporal': 3, 'configuration': 0},
                {'ledger': 4, 'raw': 5, 'temporal': 6, 'configuration': 0}],
            'before_global': {}, 'baseline': {'ingress': {'Received': 1}},
            'tokens': {'admin': 'SECRET'}, 'pg': object(), 'bundle_sha256': 'b'*64,
            'manifest_sha256': ['a'*64, 'c'*64], 'outcome': 'ABORTED'}
        smoke = SimpleNamespace(http_request=Mock(return_value=(200, {}, {'ingress': {'Received': 9,
            'api_key': 'SECRET'}, 'secret': 'SECRET'})), paper_p1_count_db=Mock(return_value={}))
        counts = [
            {'ledger': 2, 'raw': 4, 'temporal': 5, 'configuration': 0},
            {'ledger': 8, 'raw': 9, 'temporal': 10, 'configuration': 0}]
        persisted = []
        smoke.http_request.side_effect = lambda *args, **kwargs: (
            self.assertTrue(persisted), (200, {}, {'ingress': {'Received': 9,
                'api_key': 'SECRET'}, 'secret': 'SECRET'}))[1]
        with patch.object(e2, '_target_counts', side_effect=counts), \
             patch.object(e2, 'read_container_resources', return_value={'backend': {'memory_bytes': 7}}):
            first = e2.failure_diagnostics(prepared, 'ABORTED', smoke=smoke,
                endpoint='https://fixture', registry=object(), containers={},
                persist_callback=lambda result: persisted.append(copy.deepcopy(result)),
                time_fn=lambda: 10.0, wall_fn=lambda: '2026-11-06T00:00:00Z')
            second = e2.failure_diagnostics(prepared, 'ABORTED', smoke=smoke,
                endpoint='https://fixture', registry=object(), containers={})
        self.assertEqual(first, second)
        self.assertTrue(first['database_counts']['available'])
        self.assertEqual(first['database_counts']['targets']['ga/sa']['delta']['raw'], 2)
        self.assertEqual(first['terminal_metrics']['ingress']['Received'], 9)
        self.assertNotIn('SECRET', json.dumps(first))
        self.assertEqual(smoke.http_request.call_count, 1)

    def test_failure_diagnostics_caps_deadline_and_records_unavailable(self):
        now = {'value': 0.0}
        def stop(*args, **kwargs):
            now['value'] = 16.0
            return True
        collector = SimpleNamespace(stop=Mock(side_effect=stop), metrics=[], errors=[],
            get_coverage=lambda: {'snapshots_captured': 0})
        prepared = {'collector': collector, 'phase_id': 'p', 'session_id': 's', 'run_id': 'r',
                    'specs': [], 'before_targets': [], 'tokens': {'admin': 'SECRET'}}
        result = e2.failure_diagnostics(prepared, 'IMPORT_REJECTED', smoke=Mock(),
            endpoint='https://fixture', registry=object(), containers={}, time_fn=lambda: now['value'])
        self.assertEqual(result['workload_outcome'], 'IMPORT_REJECTED')
        self.assertEqual(result['terminal_metrics_status']['reason'], 'diagnostics_deadline_exceeded')
        self.assertFalse(result['database_counts']['available'])

    def test_failure_diagnostics_keeps_snapshots_when_terminal_http_and_db_fail(self):
        sampler = e2.EqualSamplesCompactSampler(scrape=lambda: None, interval=0.1,
            max_snapshots=8, byte_cap=1024, deadline_seconds=10,
            baseline_metrics={'ingress': {'Received': 1}})
        sampler.start_manual()
        spec = SimpleNamespace(gateway_id='ga', sensor_id='sa', sensor_urn='urn:a')
        prepared = {'collector': sampler, 'phase_id': 'p', 'session_id': 's', 'run_id': 'r',
            'specs': [spec], 'before_targets': [{'ledger': 0, 'raw': 0, 'temporal': 0, 'configuration': 0}],
            'pg': object(), 'tokens': {'admin': 'DO-NOT-LEAK'}}
        smoke = SimpleNamespace(http_request=Mock(side_effect=RuntimeError('DO-NOT-LEAK')),
                                db_psql=Mock(side_effect=RuntimeError('DO-NOT-LEAK')))
        with patch.object(e2, '_target_counts', side_effect=RuntimeError('DO-NOT-LEAK')):
            result = e2.failure_diagnostics(prepared, 'DEADLINE', smoke=smoke,
                endpoint='https://fixture', registry=object(), containers={})
        self.assertTrue(result['snapshots'])
        self.assertEqual(result['terminal_metrics_status']['reason'], 'terminal_scrape_failed')
        self.assertEqual(result['database_counts']['targets']['ga/sa']['reason'], 'database_query_failed')
        self.assertNotIn('DO-NOT-LEAK', json.dumps(result))

    def test_failure_diagnostics_uses_real_target_count_helper_with_timeout_and_retains_resources(self):
        spec = SimpleNamespace(gateway_id='ga', sensor_id='sa', sensor_urn='urn:a')
        collector = SimpleNamespace(stop=Mock(return_value=True), metrics=[], resources=[
            {'phase':'sample','services':{'backend':{'memory_bytes':11}}}], errors=[],
            get_coverage=lambda: {})
        prepared = {'collector':collector,'session_id':'s','run_id':'r','phase_id':'p',
            'specs':[spec],'before_targets':[{'ledger':1,'raw':2,'temporal':3,'configuration':0}],
            'pg':object(),'tokens':{'admin':'secret'}}
        smoke = SimpleNamespace(http_request=Mock(return_value=(200,{}, {'ingress':{'Received':9}})),
            db_psql=Mock(return_value=json.dumps({'ledger':2,'raw':4,'temporal':5,'configuration':0})))
        saved=[]
        with patch.object(e2, 'read_container_resources', side_effect=[RuntimeError('resource unavailable')]):
            result=e2.failure_diagnostics(prepared,'ABORTED',smoke=smoke,endpoint='https://fixture',
                registry=object(),containers={},persist_callback=lambda value:saved.append(copy.deepcopy(value)))
        self.assertTrue(result['database_counts']['available'])
        self.assertTrue(result['terminal_metrics_status']['available'])
        self.assertEqual(result['database_counts']['targets']['ga/sa']['delta']['raw'],2)
        self.assertEqual(result['resource_samples'][0]['services']['backend']['memory_bytes'],11)
        self.assertEqual(smoke.db_psql.call_args.kwargs['timeout'], unittest.mock.ANY)
        self.assertTrue(any('resource_samples' in row for row in saved))

    def test_failure_diagnostics_envelope_size_between_2mib_and_8mib_preserved(self):
        """Diagnostics envelope between 2MiB and 8MiB written via actual helper."""
        import tempfile
        from pathlib import Path
        from scripts.tests import native_compose_smoke as smoke_runtime

        # Build a representative combined HTTP+side envelope where EACH component <= 2MiB
        # but total > 2MiB and <= 8MiB. Use real sampler machinery for structure,
        # then pad deterministically so the file size exceeds 2MiB (each logical part
        # stays <= 2MiB; only the combined total crosses 2MiB).
        http_ingress = {'Received': 100, 'durations': {'ack_publish': {
            'upper_seconds': [0.1, 1.0], 'buckets': [50, 50], 'count': 100, 'overflow': 0, 'sum_seconds': 25.0}}}
        side_postgres = {'data': 'y' * (1024 * 1024)}  # ~1MiB
        side_linux = {'cpu': 100, 'mem': 'z' * (1024 * 1024)}  # ~1MiB

        collector = e2.EqualSamplesCompactSampler(scrape=lambda: {'ingress': http_ingress},
            interval=0.1, max_snapshots=10, byte_cap=4 * 1024 * 1024, deadline_seconds=100,
            baseline_metrics={'ingress': {'Received': 0}})
        collector.start_manual()
        for _ in range(5):
            collector.sample_step()
        collector.record_terminal({'ingress': {'Received': 99}}, None)

        side = e2.IndependentSideSampler(reader={
            'postgres': lambda: side_postgres, 'linux': lambda: side_linux, 'docker': lambda: {'containers': []}},
            max_samples=10, byte_cap=4 * 1024 * 1024, capture_seconds=10,
            baseline_metrics={'ingress': {'Received': 0}, 'process_instance': 'proc-123'},
            deadline_seconds=100)
        collector.side_sampler = side
        side.start()
        for _ in range(5):
            side.sample_step()
        side.stop(timeout_seconds=1.0)

        prepared = {'collector': collector, 'phase_id': 'test', 'session_id': 's', 'run_id': 'r',
                    'tokens': {'admin': 'x'}, 'specs': [], 'before_targets': [],
                    'source_provenance': {'revision': 'abc'}, 'bundle_sha256': 'b',
                    'manifest_sha256': ['m'], '_started_wall': 'started'}

        envelope = e2.failure_diagnostics(prepared, 'PARTIAL', terminal_data={
            'metrics': {'ingress': {'Received': 2}}, 'resources': {'services': {'backend': {'available': True}}},
            'postgres_linux': {'postgres': {'terminal': 1}},
            'database_counts': {'available': True}, 'collector_stop_joined': True})

        # Verify envelope structure survives failure_diagnostics
        self.assertIn('snapshots', envelope)
        self.assertIn('postgres_linux', envelope)
        self.assertEqual(envelope['full_p8_accepted'], False)
        self.assertEqual(envelope.get('workload_outcome'), 'PARTIAL')
        self.assertIn('coverage', envelope)

        # Pad deterministically so combined total > 2MiB but <= 8MiB.
        # HTTP component ~100 bytes (<= 2MiB), side samples ~2MiB total (<= 2MiB),
        # padding pushes combined over 2MiB while staying <= 8MiB.
        pad_needed = max(0, (2 * 1024 * 1024 + 4096) - len(json.dumps(envelope).encode()))
        envelope['writer_padding'] = 'p' * pad_needed
        total_size = len(json.dumps(envelope).encode())
        self.assertGreater(total_size, 2 * 1024 * 1024)
        self.assertLessEqual(total_size, 8 * 1024 * 1024)

        # Now test the ACTUAL helper writes this envelope to a protected owned temp dir
        with tempfile.TemporaryDirectory(dir='/var/tmp/opencode') as tmp:
            diag_path = Path(tmp) / 'p8-e2-diagnostics.json'
            smoke_runtime.paper_p8_e2_persist_diagnostics(diag_path, envelope)
            self.assertTrue(diag_path.is_file())
            # Read back and assert size and exact content
            written = json.loads(diag_path.read_text(encoding='utf-8'))
            self.assertEqual(written['full_p8_accepted'], False)
            self.assertEqual(written.get('workload_outcome'), 'PARTIAL')
            self.assertIn('snapshots', written)
            self.assertIn('postgres_linux', written)
            self.assertEqual(written['coverage'], envelope['coverage'])
            # Assert actual file size > 2MiB and <= 8MiB
            actual_file_size = diag_path.stat().st_size
            self.assertGreater(actual_file_size, 2 * 1024 * 1024)
            self.assertLessEqual(actual_file_size, 8 * 1024 * 1024)
            # Verify padding survives roundtrip
            self.assertEqual(written.get('writer_padding'), envelope['writer_padding'])

    def test_failure_diagnostics_envelope_over_8mib_rejected_deterministically(self):
        """Actual helper over 8MiB MUST raise fixed error and leave path absent or previous artifact unchanged."""
        import tempfile
        from pathlib import Path
        from scripts.tests import native_compose_smoke as smoke_runtime

        # Create an envelope that's over 8MiB
        large_data = 'x' * (9 * 1024 * 1024)  # 9 MiB
        oversized_envelope = {
            'schema_version': 1, 'experiment': 'E2', 'full_p8_accepted': False,
            'snapshot_status': 'available', 'large_field': large_data}

        with tempfile.TemporaryDirectory(dir='/var/tmp/opencode') as tmp:
            diag_path = Path(tmp) / 'p8-e2-diagnostics.json'

            # First write a small valid artifact
            small_envelope = {'schema_version': 1, 'experiment': 'E2', 'full_p8_accepted': False}
            smoke_runtime.paper_p8_e2_persist_diagnostics(diag_path, small_envelope)
            self.assertTrue(diag_path.is_file())
            small_content = diag_path.read_text(encoding='utf-8')

            # Now attempt to write oversized envelope - should raise fixed error
            with self.assertRaises(ValueError) as ctx:
                smoke_runtime.paper_p8_e2_persist_diagnostics(diag_path, oversized_envelope)
            self.assertEqual(str(ctx.exception), 'diagnostics_file_size_limit')

            # Verify the previous small artifact is UNCHANGED (atomic write failed, original preserved)
            unchanged_content = diag_path.read_text(encoding='utf-8')
            self.assertEqual(unchanged_content, small_content)

            # Verify path still exists (previous artifact preserved)
            self.assertTrue(diag_path.is_file())

        # Also test that writing oversized to a NEW path leaves it absent
        with tempfile.TemporaryDirectory(dir='/var/tmp/opencode') as tmp:
            new_diag_path = Path(tmp) / 'p8-e2-diagnostics-new.json'
            with self.assertRaises(ValueError) as ctx:
                smoke_runtime.paper_p8_e2_persist_diagnostics(new_diag_path, oversized_envelope)
            self.assertEqual(str(ctx.exception), 'diagnostics_file_size_limit')
            # Path should not exist (atomic write never completed)
            self.assertFalse(new_diag_path.exists())

    def test_bounded_collector_stop_does_not_scrape_if_join_times_out(self):
        scrape = Mock()
        collector = p8.BoundedCollector(scrape=scrape)
        collector._thread = Mock()
        collector._thread.is_alive.return_value = True
        self.assertFalse(collector.stop(None))
        scrape.assert_not_called()

    def test_steady_burst_collector_retains_rolling_window_past_64_with_baseline_terminal(self):
        counter = {'n': 0}
        def scrape():
            counter['n'] += 1
            return {'ingress': {'Received': counter['n'], 'secret': 'discard'}}
        sampler = e2.EqualSamplesCompactSampler(scrape=scrape, interval=0.1,
            max_snapshots=80, byte_cap=4096, deadline_seconds=30,
            baseline_metrics={'ingress': {'Received': 0, 'secret': 'discard'}})
        sampler.start_manual()
        for _ in range(70):
            sampler.sample_step()
        sampler.record_terminal({'ingress': {'Received': 99, 'secret': 'discard'}}, None)
        self.assertEqual(sampler.get_coverage()['snapshots_captured'], 71)
        self.assertGreater(sampler.get_coverage()['snapshots_evicted'], 0)
        self.assertEqual(sampler.metrics[0]['phase'], 'workload_start')
        self.assertEqual(sampler.metrics[-1]['phase'], 'workload_terminal')
        self.assertLessEqual(sampler.get_coverage()['ring_bytes_used'], 4096)
        self.assertNotIn('discard', json.dumps(sampler.metrics))

    def test_collector_snapshot_and_errors_are_bounded_and_missing_counters_stay_missing(self):
        sampler = e2.EqualSamplesCompactSampler(scrape=lambda: {
            'ingress': {'Received': 5, 'ACKPublished': None, 'secret': 'x' * 10000,
                        'ack_latency': {'count': 2, 'sum': 3, 'buckets': {'le_1ms': 1}}}},
            interval=0.1, max_snapshots=8, byte_cap=1024, deadline_seconds=10)
        sampler.sample_step()
        self.assertLessEqual(sampler.current_ring_bytes, 1024)
        snapshot = sampler.retained_snapshots[0]
        self.assertEqual(snapshot['ingress']['Received'], 5)
        self.assertNotIn('ACKPublished', snapshot['ingress'])
        self.assertNotIn('secret', json.dumps(snapshot))
        for index in range(40):
            sampler._error(f'bounded_error_{index}')
        self.assertLessEqual(len(sampler.errors), sampler.max_errors)
        self.assertLessEqual(len(json.dumps(snapshot, separators=(',', ':')).encode()), 1024)
        sampler.record_terminal({'ingress': {'Received': 1, 'secret': 'bad'}}, None)
        self.assertNotIn('secret', json.dumps(sampler.terminal_metrics))

    def test_collector_stop_timeout_is_bounded_and_idempotent_without_second_scrape(self):
        scrape = Mock(return_value={'ingress': {'Received': 1}})
        sampler = e2.EqualSamplesCompactSampler(scrape=scrape, interval=0.1,
            max_snapshots=8, byte_cap=1024, deadline_seconds=10)
        sampler.start_manual()
        sampler._thread = Mock()
        sampler._thread.is_alive.return_value = True
        self.assertFalse(sampler.stop(timeout_seconds=0.01))
        self.assertFalse(sampler.stop(timeout_seconds=0.01))
        sampler._thread.is_alive.return_value = False
        self.assertTrue(sampler.stop(timeout_seconds=0.01))
        self.assertTrue(sampler.stop(timeout_seconds=0.01))
        self.assertEqual(scrape.call_count, 1)

    def test_begin_remote_collector_covers_declared_workload_and_drain(self):
        prepared = {'phase_id': 'samples-100', 'workload': 'steady_burst',
            'profile': 'official', 'manifests': {'A': {'phases': {'measure_seconds': 7200,
                'drain_seconds': 300}}, 'B': {'phases': {'measure_seconds': 7200,
                'drain_seconds': 300}}}, 'session_remaining_seconds': 7200,
            'pg': object(), 'specs': [SimpleNamespace(), SimpleNamespace()],
            'tokens': {'admin': 'not-a-real-token'}}
        smoke = SimpleNamespace(paper_p1_count_db=Mock(return_value={}),
            _admin_metrics=Mock(return_value={'ingress': {'Received': 0}}),
            paper_p0_stats=Mock(return_value={}))
        registry = object()
        with patch.object(e2._support, '_admin_metrics', return_value={'ingress': {'Received': 0}}), \
             patch.object(e2, '_target_counts', return_value={} ), \
             patch.object(e2, 'read_container_resources', return_value={}):
            e2.begin_remote_collection(smoke, SimpleNamespace(), object(), registry, [],
                                       prepared, phase_id='samples-100')
        self.assertIsInstance(prepared['collector'], e2.EqualSamplesCompactSampler)
        # Deadline is now computed AFTER baseline read, so it's slightly less than 7200
        self.assertGreaterEqual(prepared['collector'].deadline_seconds, 7199.9)
        self.assertLessEqual(prepared['collector'].deadline_seconds, 7200.1)
        prepared['collector'].stop(timeout_seconds=0.1)

    def test_pilot_profiles_match_sample_counts_and_concurrent_work(self):
        self.assertNotEqual(self.profiles['profiles']['100']['A']['run_id'],
                            self.profiles['profiles']['1']['A']['run_id'])
        for samples, pair in self.profiles['profiles'].items():
            self.assertEqual(int(samples), pair['A']['samples_per_message'])
            self.assertEqual(pair['A']['run_id'], f'e2-{samples}')
            self.assertEqual(pair['A']['samples_per_message'], pair['B']['samples_per_message'])
            self.assertEqual(pair['A']['seed'], pair['B']['seed'])
            self.assertEqual(pair['A']['targets'], [self.targets[0]])
            self.assertEqual(pair['B']['targets'], [self.targets[1]])
            self.assertEqual(pair['A']['gateway_identity_files'], {'gw-a':'gw-a.json'})
            self.assertEqual(pair['B']['gateway_identity_files'], {'gw-b':'gw-b.json'})
            self.assertEqual(pair['A']['topology']['gateways'], 1)
            self.assertEqual(pair['B']['topology']['gateways'], 1)
            self.assertEqual(pair['A']['phases']['measure_seconds'], 8)
            self.assertEqual(pair['B']['phases']['measure_seconds'], 4)
            self.assertEqual(pair['B']['traffic']['burst_messages'], 12)
            self.assertEqual(sum((8, 16)), 24)
            self.assertEqual(pair['A']['limits'], pair['B']['limits'])

    def test_next_fixture_profile_is_separate_and_bounded(self):
        proposed = e2_fixture_profile(microbatch_wait_ms=50)
        self.assertEqual(proposed['shaper'], {'rate_per_second':2,'burst':4})
        self.assertEqual(proposed['microbatch'], {'max_messages':32,'max_rows':500,
                         'max_bytes':4 << 20,'wait_ms':50})
        self.assertFalse(proposed['effective_fixture_flags']['STAGE5_ENABLED'])
        self.assertFalse(proposed['effective_fixture_flags']['WEBSOCKET_ENABLED'])
        with self.assertRaisesRegex(ValueError, 'microbatch_wait_rejected'):
            e2_fixture_profile(microbatch_wait_ms=75)

    def test_e2_shaper_caps_formula_and_bounds(self):
        # 1. Pilot profile remains fixed 2 rate, 4 burst regardless of other inputs
        pilot = e2_shaper_caps(profile='pilot', offered_rate=2000, retry_rate=750)
        self.assertEqual(pilot, {'rate_per_second': 2, 'burst': 4})

        # 2. Official without retry: ceil(nominal * 1.10) rate, ceil(rate * tolerance) + burst
        off1 = e2_shaper_caps(profile='official', offered_rate=1000, burst_messages=0)
        self.assertEqual(off1, {'rate_per_second': 550, 'burst': 550})

        # 3. Official with retry and burst: ceil(1000 * 1.10 + 750) = 1850; 1850 + 100 + 50 = 2000
        off2 = e2_shaper_caps(profile='official', offered_rate=2000, burst_messages=50,
                              retry_rate=750, retry_burst=100)
        self.assertEqual(off2, {'rate_per_second': 1850, 'burst': 2000})

        # 4. Custom tolerance window: tolerance=0.5 -> ceil(1850 * 0.5) + 100 + 50 = 925 + 150 = 1075
        off3 = e2_shaper_caps(profile='official', offered_rate=2000, burst_messages=50,
                              retry_rate=750, retry_burst=100, tolerance_seconds=0.5)
        self.assertEqual(off3, {'rate_per_second': 1850, 'burst': 1075})

        # 5. Upper bound limits matching Go backend ShaperOptions
        with self.assertRaisesRegex(ValueError, 'e2_shaper_rate_exceeds_backend_limit'):
            e2_shaper_caps(profile='official', offered_rate=20000, retry_rate=2000)
        with self.assertRaisesRegex(ValueError, 'e2_shaper_burst_exceeds_backend_limit'):
            e2_shaper_caps(profile='official', offered_rate=2000, burst_messages=99000, retry_burst=2000)

        # 6. Verify e2_fixture_profile integrates with shaper caps and L3 microbatch caps
        official_fixture = e2_fixture_profile(microbatch_wait_ms=50, profile='official',
                                              offered_rate=2000, burst_messages=4,
                                              retry_rate=750, retry_burst=100)
        self.assertEqual(official_fixture['shaper'], {'rate_per_second': 1850, 'burst': 1954})
        self.assertEqual(official_fixture['microbatch'], {'max_messages': 64, 'max_rows': 6400,
                                                          'max_bytes': 4 << 20, 'wait_ms': 50})

    def test_plan_requires_private_matching_manifests_and_official_optin(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            a, b = [root / f'{x}.json' for x in ('a', 'b')]
            for arm, path in zip(('A', 'B'), (a, b)):
                path.write_text(json.dumps(self.profiles['profiles']['100'][arm]))
                path.chmod(0o600)
            plan = build_e2_execution_plan(output_dir=root / 'plan', manifest_a=a, manifest_b=b)
            spec = json.loads(Path(plan['run_spec']).read_text())
            self.assertEqual(len(spec['paperload']), 2)
            self.assertEqual([Path(x['manifest']).name for x in spec['paperload']],
                             ['A.manifest.json', 'B.manifest.json'])
            with self.assertRaisesRegex(ValueError, 'official_profile_requires_opt_in'):
                build_e2_execution_plan(output_dir=root / 'other', manifest_a=a, manifest_b=b,
                                        profile='official')

    def test_official_estimate_enforces_bounds_and_is_not_wire_bytes(self):
        estimate = e2_official_estimate(rate_per_second=100, duration_seconds=10,
            samples_per_message=100, payload_bytes=5000, official_opt_in=True)
        self.assertEqual(estimate['unique_messages_estimate'], 1000)
        self.assertIsNone(estimate['payload_bytes_wire'])
        self.assertTrue(estimate['not_measurements'])
        self.assertGreater(estimate['total_storage_growth_bytes_estimate'],
                           estimate['application_payload_bytes_estimate'])
        with self.assertRaisesRegex(ValueError, 'predicted_bound_exceeded'):
            e2_official_estimate(rate_per_second=20000, duration_seconds=7200,
                samples_per_message=100, payload_bytes=100000, official_opt_in=True)
        with self.assertRaisesRegex(ValueError, 'official_profile_requires_opt_in'):
            e2_official_estimate(rate_per_second=100, duration_seconds=10,
                samples_per_message=1, payload_bytes=100)

    def test_official_pending_budget_and_burst_sizing(self):
        args = dict(common={}, targets=self.targets,
            gateway_identity_files={'gw-a': 'a', 'gw-b': 'b'},
            telemetry_identity_files={'A': 'ia', 'B': 'ib'}, samples_per_message=100,
            offered_rate=1000, duration_seconds=20, burst_messages=100,
            pending_messages=1024, pending_bytes=8 << 20)
        pair = e2_official_manifests(**args)['profiles']['100']
        for arm in ('A', 'B'):
            self.assertEqual(pair[arm]['limits']['pending_messages'], 1024)
            self.assertEqual(pair[arm]['limits']['pending_bytes'], 8 << 20)
        estimate = e2_official_estimate(rate_per_second=1000, duration_seconds=20,
            samples_per_message=100, payload_bytes=4096, official_opt_in=True,
            burst_messages=100, pending_messages=1024, pending_bytes=8 << 20)
        self.assertEqual(estimate['unique_messages_estimate'], 20100)
        self.assertEqual(estimate['raw_rows_estimate'], 2010000)
        for name, value in (('pending_messages', 0), ('pending_bytes', (1 << 30) + 1)):
            with self.assertRaises(ValueError):
                e2_official_manifests(**{**args, name: value})

    def test_official_schedule_splits_total_rate_and_accounts_both_sample_sizes(self):
        for samples, expected_raw in ((100, 100_700), (1, 1_007)):
            manifests = e2_official_manifests(common={'tls': {'https_endpoint':'https://localhost',
                'https_ca_file':'ca','mqtt_tls_endpoint':'mqtts://localhost', 'mqtt_tls_ca_file':'ca'}},
                targets=self.targets, gateway_identity_files={'gw-a':'a','gw-b':'b'},
                telemetry_identity_files={'A':'ia','B':'ib'}, samples_per_message=samples,
                offered_rate=100, duration_seconds=10, burst_messages=7)
            pair = manifests['profiles'][str(samples)]
            self.assertEqual(pair['A']['traffic']['rate_per_second'] +
                             pair['B']['traffic']['rate_per_second'], 100)
            self.assertEqual(manifests['raw_rows_per_profile'], expected_raw)
            self.assertEqual(pair['B']['traffic']['burst_messages'], 7)
            self.assertEqual(pair['A']['traffic']['burst_messages'], 0)

    def test_official_rates_support_and_rejections(self):
        from scripts.tests.paper_p8_e2_support import OFFICIAL_RATES
        expected_rates = (100, 200, 500, 1000, 2000, 5000, 8000, 10000, 15000, 20000)
        self.assertEqual(OFFICIAL_RATES, expected_rates)
        for rate in expected_rates:
            duration = 5 if rate == 20000 else 10
            manifests = e2_official_manifests(common={'tls': {'https_endpoint':'https://localhost',
                'https_ca_file':'ca','mqtt_tls_endpoint':'mqtts://localhost', 'mqtt_tls_ca_file':'ca'}},
                targets=self.targets, gateway_identity_files={'gw-a':'a','gw-b':'b'},
                telemetry_identity_files={'A':'ia','B':'ib'}, samples_per_message=100,
                offered_rate=rate, duration_seconds=duration)
            pair = manifests['profiles']['100']
            rate_a = pair['A']['traffic']['rate_per_second']
            rate_b = pair['B']['traffic']['rate_per_second']
            self.assertEqual(rate_a, rate // 2)
            self.assertEqual(rate_b, rate - rate // 2)
            self.assertEqual(rate_a + rate_b, rate)
            estimate = e2_official_estimate(rate_per_second=rate, duration_seconds=duration,
                samples_per_message=100, payload_bytes=4096, official_opt_in=True)
            self.assertEqual(estimate['rate_per_second'], rate)
            self.assertEqual(estimate['unique_messages_estimate'], rate * duration)

        for invalid_rate in (-100, 0, 50, 150, 250, 300, 999, 12000, 25000):
            with self.assertRaisesRegex(ValueError, 'e2_official_workload_rejected'):
                e2_official_manifests(common={'tls': {'https_endpoint':'https://localhost',
                    'https_ca_file':'ca','mqtt_tls_endpoint':'mqtts://localhost', 'mqtt_tls_ca_file':'ca'}},
                    targets=self.targets, gateway_identity_files={'gw-a':'a','gw-b':'b'},
                    telemetry_identity_files={'A':'ia','B':'ib'}, samples_per_message=100,
                    offered_rate=invalid_rate, duration_seconds=10)
            with self.assertRaisesRegex(ValueError, 'e2_estimate_input_rejected'):
                e2_official_estimate(rate_per_second=invalid_rate, duration_seconds=10,
                    samples_per_message=100, payload_bytes=4096, official_opt_in=True)

    def test_official_plan_requires_optin_and_rejects_oversize_schedule(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            pair = e2_official_manifests(common={'tls': {'https_endpoint':'https://localhost',
                'https_ca_file':'ca','mqtt_tls_endpoint':'mqtts://localhost', 'mqtt_tls_ca_file':'ca'}},
                targets=self.targets, gateway_identity_files={'gw-a':'a','gw-b':'b'},
                telemetry_identity_files={'A':'ia','B':'ib'}, samples_per_message=1,
                offered_rate=100, duration_seconds=10)
            paths=[]
            for arm in ('A','B'):
                path=root/f'{arm}.json'; path.write_text(json.dumps(pair['profiles']['1'][arm])); path.chmod(0o600); paths.append(path)
            with self.assertRaisesRegex(ValueError, 'official_profile_requires_opt_in'):
                build_e2_execution_plan(output_dir=root/'no-opt', manifest_a=paths[0], manifest_b=paths[1], profile='official')
            plan=build_e2_execution_plan(output_dir=root/'yes', manifest_a=paths[0], manifest_b=paths[1], profile='official', official_opt_in=True)
            self.assertEqual(plan['expected_counts'], {'unique':1000,'raw':1000,'temporal':1000,'ack':1000})
            bad=json.loads(paths[1].read_text()); bad['traffic']['total_rate_per_second']=20000
            paths[1].write_text(json.dumps(bad))
            with self.assertRaisesRegex(ValueError, 'distribution_mismatch'):
                build_e2_execution_plan(output_dir=root/'bad', manifest_a=paths[0], manifest_b=paths[1], profile='official', official_opt_in=True)

    def test_phase_overlap_and_accounting_fail_closed(self):
        a = {'status': 'COMPLETE', 'telemetry': {
            'generated_unique': 8, 'offered_unique': 8, 'publish_attempts': 8, 'broker_pubacks': 8,
            'application_acks': 8, 'pending_messages': 0, 'pending_bytes': 0, 'retries':0,'invalid_acks':0}}
        b = {'status': 'COMPLETE', 'telemetry': {
            'generated_unique': 16, 'offered_unique': 16, 'publish_attempts': 18, 'broker_pubacks': 18,
            'application_acks': 16, 'pending_messages': 0, 'pending_bytes': 0, 'retries':2,'invalid_acks':0}}
        phases = [{'child': f'paperload-telemetry:{arm}.manifest.json', 'event': event,
                   'observed_at_runner_monotonic_ns': stamp}
                  for arm, event, stamp in [('A','measure_begin',10),('A','measure_end',100),
                                            ('B','burst_begin',50),('B','burst_end',70)]]
        ingress = {'available': True, 'delta': {'Received':24,'Accepted':24,'Committed':24,
            'DuplicateCommitted':0,'SamplesNew':24,'ACKPublished':24,'ShaperRateLimited':2,
            **dict.fromkeys(('RejectedNotRunning','RejectedStopping','RejectedRetained',
                'RejectedOversized','RejectedInvalidTopic','QueueFull','ByteBudgetFull'),0)}}
        result = validate_e2_result(workload_a=a, workload_b=b, ingress_delta=ingress,
            database_delta={'ledger':24,'raw':24,'temporal':24,'configuration':0},
            database_by_gateway={'gw-a':{'ledger':8,'raw':8,'temporal':8,'configuration':0},
                                 'gw-b':{'ledger':16,'raw':16,'temporal':16,'configuration':0}},
            phase_events=phases, raw_verification=[{'verified_exact':True,'gateway_id':'gw-a','sensor_id':'s-a','ledger_count':8},
                {'verified_exact':True,'gateway_id':'gw-b','sensor_id':'s-b','ledger_count':16}],
            execution_context={'samples_per_message':1,'role_to_target':{'A':self.targets[0],'B':self.targets[1]}})
        self.assertTrue(result['valid'], result['failures'])
        self.assertEqual(result['phase_overlap']['observed_overlap_ns'], 20)
        self.assertEqual(result['actual_shaper_drops']['available'], 2)
        result = validate_e2_result(workload_a=a, workload_b=b, ingress_delta={},
            database_delta={'ledger':23}, database_by_gateway={}, phase_events=[],
            execution_context={'samples_per_message':1,'role_to_target':{'A':self.targets[0],'B':self.targets[1]}})
        self.assertFalse(result['valid'])
        self.assertEqual(result['phase_overlap']['available'], None)
        self.assertEqual(result['received']['available'], None)

    def test_official_zero_burst_does_not_require_empty_burst_overlap(self):
        a = {'status': 'COMPLETE', 'telemetry': {
            'generated_unique': 50, 'offered_unique': 50, 'publish_attempts': 50,
            'broker_pubacks': 50, 'application_acks': 50, 'pending_messages': 0,
            'pending_bytes': 0, 'retries': 0, 'invalid_acks': 0}}
        b = {'status': 'COMPLETE', 'telemetry': {
            'generated_unique': 50, 'offered_unique': 50, 'publish_attempts': 50,
            'broker_pubacks': 50, 'application_acks': 50, 'pending_messages': 0,
            'pending_bytes': 0, 'retries': 0, 'invalid_acks': 0}}
        phases = [{'child': f'paperload-telemetry:{arm}.manifest.json', 'event': event,
                   'observed_at_runner_monotonic_ns': stamp}
                  for arm, event, stamp in [('A', 'measure_begin', 10), ('A', 'measure_end', 100),
                                            ('B', 'burst_begin', 101), ('B', 'burst_end', 101)]]
        result = validate_e2_result(workload_a=a, workload_b=b, ingress_delta={},
            database_delta={'ledger': 100, 'raw': 100, 'temporal': 100, 'configuration': 0},
            database_by_gateway={'gw-a': {'ledger': 50, 'raw': 50, 'temporal': 50, 'configuration': 0},
                                 'gw-b': {'ledger': 50, 'raw': 50, 'temporal': 50, 'configuration': 0}},
            phase_events=phases,
            raw_verification=[{'verified_exact': True, 'gateway_id': 'gw-a', 'sensor_id': 's-a', 'ledger_count': 50},
                              {'verified_exact': True, 'gateway_id': 'gw-b', 'sensor_id': 's-b', 'ledger_count': 50}],
            execution_context={'profile': 'official', 'samples_per_message': 1,
                'role_to_target': {'A': self.targets[0], 'B': self.targets[1]},
                'role_manifests': {arm: {'traffic': {'rate_per_second': 50, 'burst_messages': 0},
                    'phases': {'measure_seconds': 1}} for arm in ('A', 'B')}})
        self.assertNotIn('a_measure_does_not_overlap_b_burst', result['failures'])

    def test_official_accounting_uses_declared_nonpilot_counts_and_keeps_retries(self):
        pair = e2_official_manifests(common={'tls': {'https_endpoint':'https://localhost',
            'https_ca_file':'ca','mqtt_tls_endpoint':'mqtts://localhost','mqtt_tls_ca_file':'ca'}},
            targets=self.targets, gateway_identity_files={'gw-a':'a','gw-b':'b'},
            telemetry_identity_files={'A':'ia','B':'ib'}, samples_per_message=100,
            offered_rate=100, duration_seconds=1, burst_messages=3)['profiles']['100']
        role_counts = {'A':50,'B':53}
        def child(arm, retry=0):
            n=role_counts[arm]
            return {'status':'COMPLETE','telemetry':{'generated_unique':n,'offered_unique':n,
                'publish_attempts':n+retry,'broker_pubacks':n+retry,'application_acks':n,
                'pending_messages':0,'pending_bytes':0,'retries':retry,'invalid_acks':0}}
        ingress = {'available':True,'delta':{'Received':105,'Accepted':104,'Committed':103,
            'DuplicateCommitted':1,'SamplesNew':10300,'ACKPublished':104,
            'ShaperRateLimited':2,**dict.fromkeys(('RejectedNotRunning','RejectedStopping',
            'RejectedRetained','RejectedOversized','RejectedInvalidTopic','QueueFull','ByteBudgetFull'),0)}}
        phases=[{'child':f'paperload-telemetry:{arm}.manifest.json','event':event,
            'observed_at_runner_monotonic_ns':stamp} for arm,event,stamp in
            [('A','measure_begin',10),('A','measure_end',100),('B','burst_begin',50),('B','burst_end',70)]]
        result=validate_e2_result(workload_a=child('A',2),workload_b=child('B',1),
            ingress_delta=ingress,database_delta={'ledger':103,'raw':10300,'temporal':103,'configuration':0},
            database_by_gateway={'gw-a':{'ledger':50,'raw':5000,'temporal':50,'configuration':0},
                'gw-b':{'ledger':53,'raw':5300,'temporal':53,'configuration':0}},
            phase_events=phases,raw_verification=[{'count_verified':True,'gateway_id':'gw-a','sensor_id':'s-a','ledger_count':50,'raw_count':5000},
                {'count_verified':True,'gateway_id':'gw-b','sensor_id':'s-b','ledger_count':53,'raw_count':5300}],
            execution_context={'profile':'official','samples_per_message':100,
                'role_to_target':{'A':self.targets[0],'B':self.targets[1]},'role_manifests':pair})
        self.assertTrue(result['valid'],result['failures'])
        self.assertEqual(result['database_expected']['ledger'],103)
        self.assertEqual(result['actual_accounting']['publish_attempts'],106)
        self.assertEqual(result['actual_accounting']['duplicate_committed'],1)
        missing_ack=child('A',2)
        missing_ack['telemetry']['application_acks'] -= 1
        rejected=validate_e2_result(workload_a=missing_ack,workload_b=child('B',1),
            ingress_delta=ingress,database_delta={'ledger':103,'raw':10300,'temporal':103,'configuration':0},
            database_by_gateway={'gw-a':{'ledger':50,'raw':5000,'temporal':50,'configuration':0},
                'gw-b':{'ledger':53,'raw':5300,'temporal':53,'configuration':0}},
            phase_events=phases,raw_verification=[{'count_verified':True,'gateway_id':'gw-a','sensor_id':'s-a','ledger_count':50,'raw_count':5000},
                {'count_verified':True,'gateway_id':'gw-b','sensor_id':'s-b','ledger_count':53,'raw_count':5300}],
            execution_context={'profile':'official','samples_per_message':100,
                'role_to_target':{'A':self.targets[0],'B':self.targets[1]},'role_manifests':pair})
        self.assertFalse(rejected['valid'])
        self.assertIn('A_application_acks',rejected['failures'])

    def test_official_preflight_bounds_database_storage_not_only_payload(self):
        with self.assertRaisesRegex(ValueError, 'predicted_bound_exceeded'):
            e2_official_estimate(rate_per_second=100, duration_seconds=10,
                samples_per_message=100, payload_bytes=1, official_opt_in=True,
                max_disk_bytes=100000)

    def test_plan_rejects_same_gateway_and_missing_tls(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifests = []
            for arm in ('A', 'B'):
                m = dict(self.profiles['profiles']['100'][arm])
                if arm == 'B':
                    m['targets'] = [{'gateway_id':'gw-a','sensor_id':'s-other'}]
                    m['gateway_identity_files'] = {'gw-a':'other.json'}
                p = root / f'{arm}.json'; p.write_text(json.dumps(m)); p.chmod(0o600); manifests.append(p)
            with self.assertRaisesRegex(ValueError, 'gateway_targets_not_distinct'):
                build_e2_execution_plan(output_dir=root / 'bad', manifest_a=manifests[0], manifest_b=manifests[1])

    def test_missing_or_invalid_execution_evidence_is_rejected(self):
        a = {'status':'COMPLETE','telemetry':{'generated_unique':8,'offered_unique':8,
            'publish_attempts':8,'broker_pubacks':8,'application_acks':8,'pending_messages':0,'pending_bytes':0,
            'retries':0,'invalid_acks':0}}
        b = {'status':'COMPLETE','telemetry':{'generated_unique':16,'offered_unique':16,
            'publish_attempts':16,'broker_pubacks':16,'application_acks':16,'pending_messages':0,'pending_bytes':0,
            'retries':0,'invalid_acks':0}}
        result = validate_e2_result(workload_a=a, workload_b=b, ingress_delta={}, database_delta={},
            database_by_gateway={}, phase_events=[], execution_context={})
        self.assertFalse(result['valid'])

    def test_wrong_samples_scope_counts_and_reset_intervals_fail_closed(self):
        workloads = [
            {'status':'COMPLETE','telemetry':{'generated_unique':8,'offered_unique':8,'publish_attempts':8,
             'broker_pubacks':8,'application_acks':8,'pending_messages':0,'pending_bytes':0,'retries':0,'invalid_acks':0}},
            {'status':'COMPLETE','telemetry':{'generated_unique':16,'offered_unique':16,'publish_attempts':16,
             'broker_pubacks':16,'application_acks':16,'pending_messages':0,'pending_bytes':0,'retries':0,'invalid_acks':0}},
        ]
        phases = [{'child':f'paperload-telemetry:{arm}.manifest.json','event':event,
                   'observed_at_runner_monotonic_ns':stamp} for arm,event,stamp in
                  [('A','measure_begin',20),('A','measure_end',10),('B','burst_begin',5),('B','burst_end',15)]]
        result = validate_e2_result(workload_a=workloads[0], workload_b=workloads[1], ingress_delta={},
            database_delta={}, database_by_gateway={}, phase_events=phases,
            execution_context={'samples_per_message':0,'role_to_target':{'A':self.targets[0],'B':self.targets[1]}})
        self.assertFalse(result['valid'])
        self.assertIn('samples_per_message_missing_or_invalid', result['failures'])
        self.assertIn('phase_overlap_inconclusive', result['failures'])

    def test_result_requires_exact_gateway_sensor_pair(self):
        result = validate_e2_result(workload_a={}, workload_b={}, ingress_delta={},
            database_delta={}, database_by_gateway={}, phase_events=[],
            raw_verification=[{'verified_exact': True, 'gateway_id': 'gw-a', 'sensor_id': 's-b', 'ledger_count': 8},
                              {'verified_exact': True, 'gateway_id': 'gw-b', 'sensor_id': 's-a', 'ledger_count': 16}],
            execution_context={'samples_per_message': 1, 'role_to_target': {'A':self.targets[0], 'B':self.targets[1]}})
        self.assertIn('pilot_raw_retention_verification', result['failures'])

    def test_raw_verification_real_db_rejects_list_variables_and_uses_named_uuid_vars(self):
        class Registry:
            def __init__(self): self.calls = []
            def command(self, args, input=None):
                self.calls.append((args, input))
                return type('Result', (), {'returncode': 0, 'stdout': b'[]' if len(self.calls) == 1 else b'0', 'stderr': b''})()
        registry = Registry()
        with self.assertRaisesRegex(ValueError, 'db_variable_invalid'):
            smoke_runtime.db_psql(registry, 'pg', 'select 1', {'message_a': ['bad']})
        class Smoke:
            db_psql = staticmethod(smoke_runtime.db_psql)
        mid = '123e4567-e89b-12d3-a456-426614174000'
        row = p8.verify_pilot_raw(Smoke(), registry, 'pg',
            type('Spec', (), {'gateway_id':'gw-a', 'sensor_id':'s-a'})(), message_ids=[mid])
        self.assertEqual(row['ledger_count'], 0)
        self.assertTrue(any("message_a" in str(args) for args, _ in registry.calls))
        self.assertFalse(any(isinstance(value, list) for args, _ in registry.calls for value in args))

    def test_failed_child_is_boundedly_stopped(self):
        proc = Mock()
        proc.poll.return_value = None
        e2._stop(proc, timeout=2)
        proc.terminate.assert_called_once()
        proc.wait.assert_called_once_with(timeout=2)

    def test_private_artifact_serialization_omits_compose_secrets(self):
        flags = e2._safe_flags({'services': {'backend': {'environment': {
            'MQTT_SHAPER_ENABLED': 'true', 'DATABASE_URL': 'secret-value', 'JWT_SECRET': 'secret-value'}}}})
        text = json.dumps(flags)
        self.assertIn('MQTT_SHAPER_ENABLED', text)
        self.assertNotIn('secret-value', text)
        self.assertNotIn('DATABASE_URL', text)

    def test_effective_flags_use_actual_config_keys_and_reject_profile_mismatch(self):
        env = {'WEBSOCKET_ENABLED':'false', 'STAGE5_ENABLED':'false',
            'STAGE5_WAKEUP_ENABLED':'false', 'MQTT_SHAPER_ENABLED':'true',
            'MQTT_MICROBATCH_ENABLED':'true', 'MQTT_RAM_DEDUP_ENABLED':'false',
            'MQTT_SHAPER_RATE_PER_SECOND':'2', 'MQTT_SHAPER_BURST':'4',
            'MQTT_MICROBATCH_MAX_MESSAGES':'32', 'MQTT_MICROBATCH_MAX_ROWS':'500',
            'MQTT_MICROBATCH_MAX_PAYLOAD_BYTES':str(4 << 20), 'MQTT_MICROBATCH_MAX_WAIT':'50ms'}
        self.assertEqual(e2._validate_fixture_flags({'services':{'backend':{'environment':env}}}), env)
        class _DummyOfficialFixture:
            paper_p8_profile = 'official'
            paper_p8_e2_offered_rate = 2000
            paper_p8_e2_burst_messages = 4
            paper_p8_e2_retry_rate_per_second = 0
            paper_p8_e2_retry_burst = 0
            paper_p8_e2_samples_per_message = 100
        env_official = dict(env, MQTT_SHAPER_RATE_PER_SECOND='1100', MQTT_SHAPER_BURST='1104',
                            MQTT_MICROBATCH_MAX_MESSAGES='64', MQTT_MICROBATCH_MAX_ROWS='6400')
        self.assertEqual(e2._validate_fixture_flags({'services':{'backend':{'environment':env_official}}}, _DummyOfficialFixture()), env_official)
        env['MQTT_MICROBATCH_MAX_WAIT'] = '100'
        with self.assertRaisesRegex(ValueError, 'effective_fixture_flags_mismatch'):
            e2._validate_fixture_flags({'services':{'backend':{'environment':env}}})

    def test_valid_first_profile_does_not_make_invalid_second_profile_pass(self):
        completed = {'status':'WORKLOAD_COMPLETE_PENDING_CLEANUP', 'profiles':[
            {'status':'PASS'}, {'status':'FAIL', 'validation':{'valid':False}}], 'full_p8_accepted':False}
        self.assertFalse(completed['full_p8_accepted'])
        self.assertNotEqual([p['status'] for p in completed['profiles']], ['PASS', 'PASS'])

    def test_remote_phase_event_projection_preserves_only_bounded_arm_source(self):
        events = [{'child':'paperload-telemetry:A.manifest.json','event':'measure_begin',
                   'observed_at_runner_monotonic_ns':10},
                  {'child':'paperload-telemetry:B.manifest.json','event':'burst_begin',
                   'observed_at_runner_monotonic_ns':20}]
        projected = paper_p8_client._safe_phase_events(events)
        self.assertEqual([row['child'] for row in projected], [row['child'] for row in events])
        with self.assertRaisesRegex(ValueError, 'phase event schema rejected'):
            paper_p8_client._safe_phase_events([{'child':'/private/path', 'event':'measure_begin'}])

    def test_e2_runner_phase_events_use_runtime_child_names_and_elapsed_field(self):
        events = [{'child':'paperload-telemetry:A.json','event':'measure_begin',
                   'child_elapsed_ns':123,'observed_at_runner_monotonic_ns':456,
                   'clock_qualification':'runner observed pipe receipt; observation lag unknown; child elapsed is process-local',
                   'configured_phases':{'measure_seconds':8},
                   'counters':{'generated_unique':4}}]
        projected = paper_p8_client._safe_phase_events(events)
        self.assertEqual(projected[0]['child'], 'paperload-telemetry:A.json')
        self.assertEqual(projected[0]['elapsed_ns'], 123)
        self.assertNotIn('child_elapsed_ns', projected[0])

    def test_e2_phase_overlap_accepts_actual_runner_child_names(self):
        events = []
        for arm, names, start in (
                ('A', ('measure_begin', 'measure_end'), 100),
                ('B', ('burst_begin', 'burst_end'), 150)):
            for offset, name in enumerate(names):
                events.append({'child':f'paperload-telemetry:{arm}.json', 'event':name,
                               'observed_at_runner_monotonic_ns':start + offset * 100})
        a = e2._phase_intervals(events, 'A')
        b = e2._phase_intervals(events, 'B')
        self.assertGreater(min(a['measure_end'], b['burst_end']) -
                           max(a['measure_begin'], b['burst_begin']), 0)

    def test_e2_remote_prepare_rejects_unselected_sample_profile(self):
        with self.assertRaisesRegex(ValueError, 'samples_profile_rejected'):
            e2.prepare_remote(None, None, None, None, None, None, None,
                session_id='a'*32, samples_per_message=10, output_root='unused', bundle_root='unused')

    def test_equal_samples_context_defaults(self):
        ctx = build_e2_equal_samples_context()
        self.assertEqual(ctx['schema_version'], 1)
        self.assertEqual(ctx['workload_type'], 'equal_samples')
        self.assertEqual(ctx['seed'], 84)
        self.assertEqual(ctx['total_samples'], 6000)
        self.assertEqual(ctx['sample_rate'], 100)
        self.assertEqual(ctx['duration_seconds'], 60)
        self.assertEqual(ctx['logical_producers'], ['A', 'B'])
        self.assertEqual(ctx['samples_per_producer'], 3000)
        self.assertEqual(ctx['producer_sample_rate'], 50)
        self.assertEqual(ctx['sample_interval_us'], 20000)
        self.assertEqual(ctx['stream_sequence_start'], 0)
        self.assertEqual(ctx['max_context_age_seconds'], 86400)
        self.assertIn('timestamp_anchor', ctx)
        self.assertTrue(ctx['timestamp_anchor'].endswith('Z'))
        self.assertIn('context_sha256', ctx)
        self.assertEqual(len(ctx['context_sha256']), 64)
        self.assertEqual(ctx['context_sha256'], calculate_e2_equal_samples_context_sha256(ctx))
        self.assertEqual(set(ctx.keys()), E2_EQUAL_SAMPLES_CONTEXT_KEYS)
        validated = validate_e2_equal_samples_context(ctx)
        self.assertEqual(validated, ctx)

    def test_equal_samples_context_boundaries(self):
        ctx_min_dur = build_e2_equal_samples_context(total_samples=200, sample_rate=20)
        self.assertEqual(ctx_min_dur['duration_seconds'], 10)
        self.assertEqual(ctx_min_dur['samples_per_producer'], 100)
        self.assertEqual(ctx_min_dur['sample_interval_us'], 100000)
        validate_e2_equal_samples_context(ctx_min_dur)

        ctx_max_dur = build_e2_equal_samples_context(total_samples=24000, sample_rate=200)
        self.assertEqual(ctx_max_dur['duration_seconds'], 120)
        self.assertEqual(ctx_max_dur['samples_per_producer'], 12000)
        self.assertEqual(ctx_max_dur['sample_interval_us'], 10000)
        validate_e2_equal_samples_context(ctx_max_dur)

        ctx_min_rate = build_e2_equal_samples_context(total_samples=200, sample_rate=2)
        self.assertEqual(ctx_min_rate['duration_seconds'], 100)
        self.assertEqual(ctx_min_rate['sample_interval_us'], 1000000)
        validate_e2_equal_samples_context(ctx_min_rate)

        with self.assertRaisesRegex(ValueError, 'duration_seconds_out_of_bounds'):
            build_e2_equal_samples_context(total_samples=60000, sample_rate=200)

    def test_equal_samples_context_invalid_inputs(self):
        for bad in (6000.0, 100.0, 84.0):
            with self.assertRaises(ValueError):
                build_e2_equal_samples_context(total_samples=bad)
        with self.assertRaises(ValueError):
            build_e2_equal_samples_context(sample_rate=100.0)
        with self.assertRaises(ValueError):
            build_e2_equal_samples_context(seed=84.0)

        with self.assertRaises(ValueError):
            build_e2_equal_samples_context(total_samples=True)
        with self.assertRaises(ValueError):
            build_e2_equal_samples_context(sample_rate=False)
        with self.assertRaises(ValueError):
            build_e2_equal_samples_context(seed=True)

        with self.assertRaises(ValueError):
            build_e2_equal_samples_context(total_samples=0)
        with self.assertRaises(ValueError):
            build_e2_equal_samples_context(total_samples=-200)
        with self.assertRaises(ValueError):
            build_e2_equal_samples_context(sample_rate=0)
        with self.assertRaises(ValueError):
            build_e2_equal_samples_context(sample_rate=-2)
        with self.assertRaises(ValueError):
            build_e2_equal_samples_context(seed=-1)

        with self.assertRaisesRegex(ValueError, 'total_samples_divisible_by_200'):
            build_e2_equal_samples_context(total_samples=6100)

        with self.assertRaisesRegex(ValueError, 'total_samples_max_exceeded'):
            build_e2_equal_samples_context(total_samples=60200)

        with self.assertRaisesRegex(ValueError, 'sample_rate_must_be_even'):
            build_e2_equal_samples_context(sample_rate=99)

        with self.assertRaisesRegex(ValueError, 'sample_rate_out_of_bounds'):
            build_e2_equal_samples_context(sample_rate=1)
        with self.assertRaisesRegex(ValueError, 'sample_rate_out_of_bounds'):
            build_e2_equal_samples_context(sample_rate=202)

        with self.assertRaisesRegex(ValueError, 'duration_must_be_integer'):
            build_e2_equal_samples_context(total_samples=6000, sample_rate=70)

        with self.assertRaisesRegex(ValueError, 'duration_seconds_out_of_bounds'):
            build_e2_equal_samples_context(total_samples=400, sample_rate=100)
        with self.assertRaisesRegex(ValueError, 'duration_seconds_out_of_bounds'):
            build_e2_equal_samples_context(total_samples=16000, sample_rate=100)

        with self.assertRaisesRegex(ValueError, 'sample_interval_us_must_be_integer'):
            build_e2_equal_samples_context(total_samples=1400, sample_rate=14)

        with self.assertRaisesRegex(ValueError, 'unknown_context_parameters'):
            build_e2_equal_samples_context(extra_parameter=123)

        # Validation rejects unknown or missing keys in context dict
        valid_ctx = build_e2_equal_samples_context()
        tampered_unknown = dict(valid_ctx)
        tampered_unknown['bogus_key'] = 'bad'
        with self.assertRaisesRegex(ValueError, 'unknown_context_keys'):
            validate_e2_equal_samples_context(tampered_unknown)

        tampered_missing = dict(valid_ctx)
        del tampered_missing['seed']
        with self.assertRaisesRegex(ValueError, 'missing_context_keys'):
            validate_e2_equal_samples_context(tampered_missing)

    def test_equal_samples_context_anchor_and_freshness(self):
        fixed_now = datetime(2026, 10, 31, 12, 0, 0, tzinfo=timezone.utc)
        anchor_valid = '2026-10-31T11:58:00Z'
        ctx = build_e2_equal_samples_context(timestamp_anchor=anchor_valid, now_utc=fixed_now)
        validate_e2_equal_samples_context(ctx, now_utc=fixed_now)

        with self.assertRaisesRegex(ValueError, 'anchor_must_be_utc_z'):
            build_e2_equal_samples_context(timestamp_anchor='2026-10-31T11:58:00+07:00', now_utc=fixed_now)
        with self.assertRaisesRegex(ValueError, 'anchor_must_be_utc_z'):
            build_e2_equal_samples_context(timestamp_anchor='2026-10-31T11:58:00', now_utc=fixed_now)

        with self.assertRaisesRegex(ValueError, 'anchor_year_out_of_bounds'):
            build_e2_equal_samples_context(timestamp_anchor='0000-10-31T11:58:00Z', now_utc=fixed_now)
        with self.assertRaisesRegex(ValueError, 'anchor_year_out_of_bounds'):
            build_e2_equal_samples_context(timestamp_anchor='10000-01-01T00:00:00Z', now_utc=fixed_now)

        anchor_future_stream = '2026-10-31T11:59:30Z'
        with self.assertRaisesRegex(ValueError, 'equal_samples_stream_not_in_past'):
            build_e2_equal_samples_context(timestamp_anchor=anchor_future_stream, now_utc=fixed_now)

        with self.assertRaisesRegex(ValueError, 'equal_samples_stream_not_in_past'):
            build_e2_equal_samples_context(timestamp_anchor='2026-10-31T12:05:00Z', now_utc=fixed_now)

        old_anchor = '2026-10-29T10:00:00Z'
        ctx_old = build_e2_equal_samples_context(
            timestamp_anchor=old_anchor,
            max_context_age_seconds=86400,
            now_utc=datetime(2026, 10, 29, 12, 0, 0, tzinfo=timezone.utc),
        )
        ctx_old_copy = json.loads(json.dumps(ctx_old))
        with self.assertRaisesRegex(ValueError, 'equal_samples_context_expired'):
            validate_e2_equal_samples_context(ctx_old, now_utc=fixed_now)
        self.assertEqual(ctx_old, ctx_old_copy)

    def test_equal_samples_context_hash_tampering(self):
        ctx = build_e2_equal_samples_context()
        validate_e2_equal_samples_context(ctx)

        tampered = dict(ctx)
        tampered['seed'] = 85
        with self.assertRaisesRegex(ValueError, 'equal_samples_context_hash_mismatch'):
            validate_e2_equal_samples_context(tampered)

        tampered = dict(ctx)
        tampered['total_samples'] = 4000
        with self.assertRaisesRegex(ValueError, 'equal_samples_context_hash_mismatch'):
            validate_e2_equal_samples_context(tampered)

        tampered = dict(ctx)
        tampered['context_sha256'] = '0' * 64
        with self.assertRaisesRegex(ValueError, 'equal_samples_context_hash_mismatch'):
            validate_e2_equal_samples_context(tampered)

    def test_equal_samples_expected_counts(self):
        ctx = build_e2_equal_samples_context()

        c1 = calculate_e2_equal_samples_expected_counts(ctx, samples_per_message=1)
        self.assertEqual(c1['samples_per_message'], 1)
        self.assertEqual(c1['total_samples'], 6000)
        self.assertEqual(c1['total_messages'], 6000)
        self.assertEqual(c1['total_raw_rows'], 6000)
        self.assertEqual(c1['total_ledger_rows'], 6000)
        self.assertEqual(c1['total_temporal_rows'], 6000)
        self.assertEqual(c1['total_configuration_delta'], 0)
        for role in ('A', 'B'):
            self.assertEqual(c1['per_role'][role]['samples'], 3000)
            self.assertEqual(c1['per_role'][role]['messages'], 3000)
            self.assertEqual(c1['per_role'][role]['raw_rows'], 3000)
            self.assertEqual(c1['per_role'][role]['ledger_rows'], 3000)
            self.assertEqual(c1['per_role'][role]['temporal_rows'], 3000)
            self.assertEqual(c1['per_role'][role]['configuration_delta'], 0)

        c100 = calculate_e2_equal_samples_expected_counts(ctx, samples_per_message=100)
        self.assertEqual(c100['samples_per_message'], 100)
        self.assertEqual(c100['total_samples'], 6000)
        self.assertEqual(c100['total_messages'], 60)
        self.assertEqual(c100['total_raw_rows'], 6000)
        self.assertEqual(c100['total_ledger_rows'], 60)
        self.assertEqual(c100['total_temporal_rows'], 60)
        self.assertEqual(c100['total_configuration_delta'], 0)
        for role in ('A', 'B'):
            self.assertEqual(c100['per_role'][role]['samples'], 3000)
            self.assertEqual(c100['per_role'][role]['messages'], 30)
            self.assertEqual(c100['per_role'][role]['raw_rows'], 3000)
            self.assertEqual(c100['per_role'][role]['ledger_rows'], 30)
            self.assertEqual(c100['per_role'][role]['temporal_rows'], 30)
            self.assertEqual(c100['per_role'][role]['configuration_delta'], 0)

        for bad_s in (10, 50, True, False, 1.0, 100.0, '100', None):
            with self.assertRaises(ValueError):
                calculate_e2_equal_samples_expected_counts(ctx, samples_per_message=bad_s)

    def test_equal_samples_logical_stream_independent_of_batching(self):
        fixed_now = datetime(2026, 10, 31, 12, 0, 0, tzinfo=timezone.utc)
        ctx = build_e2_equal_samples_context(timestamp_anchor='2026-10-31T11:58:00Z', now_utc=fixed_now)

        for role in ('A', 'B'):
            stream_s1 = e2_equal_samples_logical_stream(ctx, role=role, samples_per_message=1)
            stream_s100 = e2_equal_samples_logical_stream(ctx, role=role, samples_per_message=100)

            flat_s1 = [sample for msg in stream_s1 for sample in msg['samples']]
            flat_s100 = [sample for msg in stream_s100 for sample in msg['samples']]

            self.assertEqual(len(flat_s1), 3000)
            self.assertEqual(len(flat_s100), 3000)
            self.assertEqual(flat_s1, flat_s100)

            self.assertEqual(flat_s1[0]['sequence'], 0)
            self.assertAlmostEqual(flat_s1[0]['value'], synthetic_value(0))
            self.assertEqual(flat_s1[-1]['sequence'], 2999)
            self.assertAlmostEqual(flat_s1[-1]['value'], synthetic_value(2999))

    def test_legacy_e2_pilot_manifests_regression(self):
        legacy = e2_pilot_manifests(common={'tls': {'endpoint': 'test'}},
                                    targets=self.targets,
                                    gateway_identity_files={'gw-a': 'a.json', 'gw-b': 'b.json'},
                                    telemetry_identity_files={'A': 'a.json', 'B': 'b.json'})
        self.assertEqual(legacy['unique_messages_per_profile'], 24)
        self.assertEqual(legacy['shared_seed'], 84)
        self.assertEqual(set(legacy['profiles'].keys()), {'1', '100'})
        self.assertEqual(legacy['profiles']['100']['A']['samples_per_message'], 100)
        self.assertEqual(legacy['profiles']['100']['A']['phases']['measure_seconds'], 8)
        self.assertEqual(legacy['profiles']['100']['B']['phases']['measure_seconds'], 4)
        self.assertEqual(legacy['profiles']['100']['B']['traffic']['burst_messages'], 12)

    def test_equal_samples_context_tightened_seed_bound(self):
        max_i64 = (1 << 63) - 1
        ctx_max_seed = build_e2_equal_samples_context(seed=max_i64)
        self.assertEqual(ctx_max_seed['seed'], max_i64)
        validate_e2_equal_samples_context(ctx_max_seed)

        ctx_zero_seed = build_e2_equal_samples_context(seed=0)
        self.assertEqual(ctx_zero_seed['seed'], 0)
        validate_e2_equal_samples_context(ctx_zero_seed)

        with self.assertRaises(ValueError):
            build_e2_equal_samples_context(seed=-1)
        with self.assertRaises(ValueError):
            build_e2_equal_samples_context(seed=1 << 63)
        with self.assertRaises(ValueError):
            build_e2_equal_samples_context(seed=1 << 64)

        bad_seed_ctx = dict(ctx_max_seed)
        bad_seed_ctx['seed'] = 1 << 63
        bad_seed_ctx['context_sha256'] = calculate_e2_equal_samples_context_sha256(bad_seed_ctx)
        with self.assertRaises(ValueError):
            validate_e2_equal_samples_context(bad_seed_ctx)

    def test_equal_samples_context_tightened_age_and_ttl(self):
        ctx_max_ttl = build_e2_equal_samples_context(max_context_age_seconds=MAX_CONTEXT_AGE_SECONDS_BOUND)
        self.assertEqual(ctx_max_ttl['max_context_age_seconds'], MAX_CONTEXT_AGE_SECONDS_BOUND)
        validate_e2_equal_samples_context(ctx_max_ttl)

        ctx_small_ttl = build_e2_equal_samples_context(max_context_age_seconds=70)
        self.assertEqual(ctx_small_ttl['max_context_age_seconds'], 70)
        validate_e2_equal_samples_context(ctx_small_ttl)

        with self.assertRaises(ValueError):
            build_e2_equal_samples_context(max_context_age_seconds=0)
        with self.assertRaises(ValueError):
            build_e2_equal_samples_context(max_context_age_seconds=-10)
        with self.assertRaises(ValueError):
            build_e2_equal_samples_context(max_context_age_seconds=MAX_CONTEXT_AGE_SECONDS_BOUND + 1)

        bad_ttl_ctx = dict(ctx_max_ttl)
        bad_ttl_ctx['max_context_age_seconds'] = 0
        bad_ttl_ctx['context_sha256'] = calculate_e2_equal_samples_context_sha256(bad_ttl_ctx)
        with self.assertRaises(ValueError):
            validate_e2_equal_samples_context(bad_ttl_ctx)

        bad_ttl_ctx['max_context_age_seconds'] = -100
        bad_ttl_ctx['context_sha256'] = calculate_e2_equal_samples_context_sha256(bad_ttl_ctx)
        with self.assertRaises(ValueError):
            validate_e2_equal_samples_context(bad_ttl_ctx)

        bad_ttl_ctx['max_context_age_seconds'] = MAX_CONTEXT_AGE_SECONDS_BOUND + 1
        bad_ttl_ctx['context_sha256'] = calculate_e2_equal_samples_context_sha256(bad_ttl_ctx)
        with self.assertRaises(ValueError):
            validate_e2_equal_samples_context(bad_ttl_ctx)

    def test_equal_samples_context_tightened_anchor_syntax_matches_go(self):
        fixed_now = datetime(2026, 10, 31, 12, 0, 0, tzinfo=timezone.utc)
        valid_anchor_frac = '2026-10-31T11:58:00.123456Z'
        ctx_frac = build_e2_equal_samples_context(timestamp_anchor=valid_anchor_frac, now_utc=fixed_now)
        self.assertEqual(ctx_frac['timestamp_anchor'], valid_anchor_frac)
        validate_e2_equal_samples_context(ctx_frac, now_utc=fixed_now)

        # Fraction with 7 digits (exceeds Go's 6 digits microsecond precision)
        with self.assertRaisesRegex(ValueError, 'anchor_must_be_utc_z'):
            build_e2_equal_samples_context(timestamp_anchor='2026-10-31T11:58:00.1234567Z', now_utc=fixed_now)

        # Empty fraction
        with self.assertRaisesRegex(ValueError, 'anchor_must_be_utc_z'):
            build_e2_equal_samples_context(timestamp_anchor='2026-10-31T11:58:00.Z', now_utc=fixed_now)

        # Invalid hour 24
        with self.assertRaisesRegex(ValueError, 'anchor_must_be_utc_z'):
            build_e2_equal_samples_context(timestamp_anchor='2026-10-31T24:00:00Z', now_utc=fixed_now)

        # Invalid minute 60, second 60
        with self.assertRaisesRegex(ValueError, 'anchor_must_be_utc_z'):
            build_e2_equal_samples_context(timestamp_anchor='2026-10-31T11:60:00Z', now_utc=fixed_now)
        with self.assertRaisesRegex(ValueError, 'anchor_must_be_utc_z'):
            build_e2_equal_samples_context(timestamp_anchor='2026-10-31T11:58:60Z', now_utc=fixed_now)

        # Lowercase z and missing z
        with self.assertRaisesRegex(ValueError, 'anchor_must_be_utc_z'):
            build_e2_equal_samples_context(timestamp_anchor='2026-10-31T11:58:00z', now_utc=fixed_now)
        with self.assertRaisesRegex(ValueError, 'anchor_must_be_utc_z'):
            build_e2_equal_samples_context(timestamp_anchor='2026-10-31T11:58:00', now_utc=fixed_now)

    def test_equal_samples_expected_counts_rejects_malformed_without_wall_clock(self):
        # 1. Historical context with past anchor (e.g. from year 2024, age > 86400s)
        historical_anchor = '2024-01-01T00:00:00Z'
        historical_ctx = build_e2_equal_samples_context(
            timestamp_anchor=historical_anchor,
            max_context_age_seconds=86400,
            now_utc=datetime(2024, 1, 1, 0, 5, 0, tzinfo=timezone.utc),
        )
        # Expected counts should SUCCEED on historical context today without wall-clock failure
        counts = calculate_e2_equal_samples_expected_counts(historical_ctx, samples_per_message=1)
        self.assertEqual(counts['total_samples'], 6000)
        self.assertEqual(counts['total_messages'], 6000)
        self.assertEqual(counts['per_role']['A']['messages'], 3000)

        # 2. Malformed contexts must be REJECTED by expected_counts
        # Non-dict context
        with self.assertRaises(ValueError):
            calculate_e2_equal_samples_expected_counts('not-a-dict', samples_per_message=1)

        # Tampered context hash
        tampered_ctx = dict(historical_ctx)
        tampered_ctx['seed'] = 999
        with self.assertRaisesRegex(ValueError, 'equal_samples_context_hash_mismatch'):
            calculate_e2_equal_samples_expected_counts(tampered_ctx, samples_per_message=1)

        # Missing key
        missing_key_ctx = dict(historical_ctx)
        del missing_key_ctx['logical_producers']
        with self.assertRaisesRegex(ValueError, 'missing_context_keys'):
            calculate_e2_equal_samples_expected_counts(missing_key_ctx, samples_per_message=1)

        # Nonpositive TTL
        bad_ttl_ctx = dict(historical_ctx)
        bad_ttl_ctx['max_context_age_seconds'] = -5
        bad_ttl_ctx['context_sha256'] = calculate_e2_equal_samples_context_sha256(bad_ttl_ctx)
        with self.assertRaises(ValueError):
            calculate_e2_equal_samples_expected_counts(bad_ttl_ctx, samples_per_message=1)

    def test_e2_equal_samples_manifests_defaults_and_exact_schema(self):
        fixed_now = datetime(2026, 10, 31, 12, 0, 0, tzinfo=timezone.utc)
        ctx = build_e2_equal_samples_context(
            timestamp_anchor='2026-10-31T11:58:00Z',
            now_utc=fixed_now,
            seed=42,
        )
        common = {
            'mode': 'remote',
            'run_id': 'test-e2-equal',
            'bundle_path': '/var/tmp/bundle',
            'publish_timeout_ms': 3000,
            'tls': {
                'https_endpoint': 'https://localhost:8443',
                'https_ca_file': 'ca.crt',
                'mqtt_tls_endpoint': 'mqtts://localhost:8883',
                'mqtt_tls_ca_file': 'mqtt-ca.crt',
                'mqtt_tls_server_name': 'localhost',
            },
        }
        gw_files = {'gw-a': 'gw-a.json', 'gw-b': 'gw-b.json'}
        tele_files = {'A': 'tele-a.json', 'B': 'tele-b.json'}

        manifest_bundle = e2_equal_samples_manifests(
            context=ctx,
            common=common,
            targets=self.targets,
            gateway_identity_files=gw_files,
            telemetry_identity_files=tele_files,
            now_utc=fixed_now,
        )

        self.assertEqual(manifest_bundle['workload_type'], 'equal_samples')
        self.assertEqual(manifest_bundle['shared_seed'], 42)
        self.assertEqual(manifest_bundle['context'], ctx)
        self.assertTrue(manifest_bundle['same_topology_security_quota_worker_policy'])
        self.assertEqual(manifest_bundle['execution'], 'NOT RUN')
        self.assertEqual(set(manifest_bundle['profiles'].keys()), {'1', '100'})
        self.assertEqual(set(manifest_bundle['expected_counts'].keys()), {'1', '100'})

        # Duration = 60, Drain = 20, default Grace = 10 -> Wall = 90
        expected_wall = 60 + 20 + 10

        for sample_str, expected_msg_count, expected_rate in (('1', 3000, 50), ('100', 30, 0.5)):
            pair = manifest_bundle['profiles'][sample_str]
            self.assertEqual(set(pair.keys()), {'A', 'B'})

            for arm in ('A', 'B'):
                m = pair[arm]
                # Legacy shape check
                self.assertEqual(m['schema_version'], 1)
                self.assertEqual(m['experiment'], 'E2')
                self.assertEqual(m['arm'], arm)
                self.assertEqual(m['profile'], 'pilot')
                self.assertEqual(m['roles'], ['telemetry'])
                self.assertEqual(m['seed'], 42)
                self.assertEqual(m['samples_per_message'], int(sample_str))
                target_idx = 0 if arm == 'A' else 1
                self.assertEqual(m['targets'], [self.targets[target_idx]])
                self.assertEqual(m['subscriptions'], [])
                self.assertEqual(m['topology'], {'gateways': 1, 'sensors_per_gateway': 1, 'users': 0, 'viewers': 0})
                self.assertEqual(m['durability_mode'], 'synthetic-load')
                self.assertEqual(m['retry_interval_ms'], 500)
                self.assertEqual(m['run_id'], f'test-e2-equal-{sample_str}')

                # Steady producers, zero warmup, zero cooldown, zero burst
                self.assertEqual(m['traffic']['burst_messages'], 0)
                self.assertEqual(m['traffic']['ramp_seconds'], 0)
                self.assertEqual(m['traffic']['rate_per_second'], expected_rate)

                # Phases: measure=context duration, drain=20, warmup=cooldown=0
                self.assertEqual(m['phases']['warmup_seconds'], 0)
                self.assertEqual(m['phases']['cooldown_seconds'], 0)
                self.assertEqual(m['phases']['measure_seconds'], 60)
                self.assertEqual(m['phases']['drain_seconds'], 20)

                # Limits: fixed safe workers/connections, wall=duration+drain+grace
                self.assertEqual(m['limits']['workers'], 2)
                self.assertEqual(m['limits']['connections'], 1)
                self.assertEqual(m['limits']['retries'], 16)
                self.assertEqual(m['limits']['pending_messages'], 64)
                self.assertEqual(m['limits']['pending_bytes'], 1 << 20)
                self.assertEqual(m['limits']['disk_growth_bytes'], 1 << 24)
                self.assertEqual(m['limits']['wall_seconds'], expected_wall)

                # Target and credential binding
                target_gw = self.targets[target_idx]['gateway_id']
                self.assertEqual(m['gateway_identity_files'], {target_gw: gw_files[target_gw]})
                self.assertEqual(m['identity_files'], {'telemetry': tele_files[arm]})

                # Opt-in equal_samples object
                self.assertIn('equal_samples', m)
                eq = m['equal_samples']
                self.assertEqual(set(eq.keys()), E2_EQUAL_SAMPLES_MANIFEST_KEYS)
                for k in E2_EQUAL_SAMPLES_CONTEXT_KEYS:
                    self.assertEqual(eq[k], ctx[k])
                self.assertEqual(eq['expected_samples'], 3000)
                self.assertEqual(eq['expected_messages'], expected_msg_count)
                self.assertEqual(eq['sample_stream_version'], 1)

    def test_e2_equal_samples_manifests_two_profiles_same_context_and_stream(self):
        fixed_now = datetime(2026, 10, 31, 12, 0, 0, tzinfo=timezone.utc)
        ctx = build_e2_equal_samples_context(
            timestamp_anchor='2026-10-31T11:58:00Z',
            now_utc=fixed_now,
            seed=84,
        )
        common = {'run_id': 'stream-test'}
        gw_files = {'gw-a': 'gw-a.json', 'gw-b': 'gw-b.json'}
        tele_files = {'A': 'tele-a.json', 'B': 'tele-b.json'}

        manifest_bundle = e2_equal_samples_manifests(
            context=ctx,
            common=common,
            targets=self.targets,
            gateway_identity_files=gw_files,
            telemetry_identity_files=tele_files,
            now_utc=fixed_now,
        )

        p1 = manifest_bundle['profiles']['1']
        p100 = manifest_bundle['profiles']['100']

        for arm in ('A', 'B'):
            # Same shared seed
            self.assertEqual(p1[arm]['seed'], p100[arm]['seed'])
            self.assertEqual(p1[arm]['seed'], 84)

            # Same canonical equal_samples context
            for k in E2_EQUAL_SAMPLES_CONTEXT_KEYS:
                self.assertEqual(p1[arm]['equal_samples'][k], p100[arm]['equal_samples'][k])

            # Identical logical samples stream
            stream_1 = e2_equal_samples_logical_stream(p1[arm]['equal_samples'], role=arm, samples_per_message=1)
            stream_100 = e2_equal_samples_logical_stream(p100[arm]['equal_samples'], role=arm, samples_per_message=100)

            flat_1 = [s for msg in stream_1 for s in msg['samples']]
            flat_100 = [s for msg in stream_100 for s in msg['samples']]

            self.assertEqual(len(flat_1), 3000)
            self.assertEqual(len(flat_100), 3000)
            self.assertEqual(flat_1, flat_100)

    def test_e2_equal_samples_manifests_validation_and_boundaries(self):
        fixed_now = datetime(2026, 10, 31, 12, 0, 0, tzinfo=timezone.utc)
        ctx = build_e2_equal_samples_context(
            timestamp_anchor='2026-10-31T11:58:00Z',
            now_utc=fixed_now,
        )
        common = {'run_id': 'val-test'}
        gw_files = {'gw-a': 'gw-a.json', 'gw-b': 'gw-b.json'}
        tele_files = {'A': 'tele-a.json', 'B': 'tele-b.json'}

        # 1. Missing or invalid context
        with self.assertRaises(ValueError):
            e2_equal_samples_manifests(
                context=None, common=common, targets=self.targets,
                gateway_identity_files=gw_files, telemetry_identity_files=tele_files)
        with self.assertRaises(ValueError):
            e2_equal_samples_manifests(
                context={}, common=common, targets=self.targets,
                gateway_identity_files=gw_files, telemetry_identity_files=tele_files)

        # 2. Targets validation: not 2 targets
        with self.assertRaisesRegex(ValueError, 'e2_requires_two_gateway_targets'):
            e2_equal_samples_manifests(
                context=ctx, common=common, targets=[self.targets[0]],
                gateway_identity_files=gw_files, telemetry_identity_files=tele_files, now_utc=fixed_now)

        # Targets validation: non-distinct gateway_id
        same_gw_targets = [{'gateway_id': 'gw-a', 'sensor_id': 's-1'}, {'gateway_id': 'gw-a', 'sensor_id': 's-2'}]
        with self.assertRaisesRegex(ValueError, 'e2_requires_two_gateway_targets'):
            e2_equal_samples_manifests(
                context=ctx, common=common, targets=same_gw_targets,
                gateway_identity_files={'gw-a': 'gw-a.json'}, telemetry_identity_files=tele_files, now_utc=fixed_now)

        # Targets validation: missing sensor_id
        bad_targets = [{'gateway_id': 'gw-a'}, {'gateway_id': 'gw-b', 'sensor_id': 's-b'}]
        with self.assertRaisesRegex(ValueError, 'e2_requires_two_gateway_targets'):
            e2_equal_samples_manifests(
                context=ctx, common=common, targets=bad_targets,
                gateway_identity_files=gw_files, telemetry_identity_files=tele_files, now_utc=fixed_now)

        # 3. Gateway credentials mismatch
        bad_gw_files = {'gw-a': 'gw-a.json', 'gw-other': 'gw-other.json'}
        with self.assertRaisesRegex(ValueError, 'e2_gateway_credentials_mismatch'):
            e2_equal_samples_manifests(
                context=ctx, common=common, targets=self.targets,
                gateway_identity_files=bad_gw_files, telemetry_identity_files=tele_files, now_utc=fixed_now)

        # 4. Telemetry credentials mismatch
        bad_tele_files = {'A': 'tele-a.json', 'C': 'tele-c.json'}
        with self.assertRaisesRegex(ValueError, 'e2_telemetry_credentials_mismatch'):
            e2_equal_samples_manifests(
                context=ctx, common=common, targets=self.targets,
                gateway_identity_files=gw_files, telemetry_identity_files=bad_tele_files, now_utc=fixed_now)

        # 5. Grace seconds out of bounds
        with self.assertRaisesRegex(ValueError, 'grace_seconds'):
            e2_equal_samples_manifests(
                context=ctx, common=common, targets=self.targets,
                gateway_identity_files=gw_files, telemetry_identity_files=tele_files,
                grace_seconds=-1, now_utc=fixed_now)
        with self.assertRaisesRegex(ValueError, 'grace_seconds'):
            e2_equal_samples_manifests(
                context=ctx, common=common, targets=self.targets,
                gateway_identity_files=gw_files, telemetry_identity_files=tele_files,
                grace_seconds=100, now_utc=fixed_now)

        # 6. Common not a dict
        with self.assertRaises(ValueError):
            e2_equal_samples_manifests(
                context=ctx, common='not-a-dict', targets=self.targets,
                gateway_identity_files=gw_files, telemetry_identity_files=tele_files, now_utc=fixed_now)

    def test_build_e2_execution_plan_equal_samples_valid(self):
        fixed_now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        ctx = build_e2_equal_samples_context(total_samples=6000, sample_rate=100, seed=84, now_utc=fixed_now)
        common = {
            'mode': 'remote', 'publish_timeout_ms': 3000,
            'tls': {'https_endpoint': 'https://127.0.0.1:443', 'https_ca_file': 'ca.pem',
                    'mqtt_tls_endpoint': 'mqtts://127.0.0.1:8883', 'mqtt_tls_ca_file': 'ca.pem',
                    'mqtt_tls_server_name': 'localhost'},
        }
        gw_files = {'gw-a': 'gw-a.json', 'gw-b': 'gw-b.json'}
        tele_files = {'A': 'tele-a.json', 'B': 'tele-b.json'}
        manifests = e2_equal_samples_manifests(
            context=ctx, common=common, targets=self.targets,
            gateway_identity_files=gw_files, telemetry_identity_files=tele_files, now_utc=fixed_now)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for S in ('100', '1'):
                pair = manifests['profiles'][S]
                paths = {}
                for arm in ('A', 'B'):
                    p = root / f'{arm}-{S}.json'
                    p.write_text(json.dumps(pair[arm]))
                    p.chmod(0o600)
                    paths[arm] = p

                plan = build_e2_execution_plan(
                    output_dir=root / f'plan-{S}', manifest_a=paths['A'], manifest_b=paths['B'])
                spec = json.loads(Path(plan['run_spec']).read_text())
                self.assertEqual(len(spec['paperload']), 2)
                self.assertEqual([x['role'] for x in spec['paperload']], ['telemetry', 'telemetry'])
                self.assertEqual(spec['stop_timeout_seconds'], 30)
                # Verify written manifests in plan output
                written_a = json.loads((root / f'plan-{S}/A.manifest.json').read_text())
                written_b = json.loads((root / f'plan-{S}/B.manifest.json').read_text())
                self.assertEqual(written_a['equal_samples']['duration_seconds'], 60)
                self.assertEqual(written_a['limits']['wall_seconds'], 90)
                self.assertNotEqual(written_a['limits']['wall_seconds'], spec['stop_timeout_seconds'])
                self.assertEqual(written_a['equal_samples']['sample_stream_version'], 1)
                self.assertEqual(written_b['equal_samples']['sample_stream_version'], 1)
                self.assertEqual(written_a['equal_samples']['context_sha256'], ctx['context_sha256'])
                self.assertEqual(written_b['equal_samples']['context_sha256'], ctx['context_sha256'])

    def test_build_e2_execution_plan_equal_samples_negatives(self):
        fixed_now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        ctx = build_e2_equal_samples_context(total_samples=6000, sample_rate=100, seed=84, now_utc=fixed_now)
        common = {
            'mode': 'remote', 'publish_timeout_ms': 3000,
            'tls': {'https_endpoint': 'https://127.0.0.1:443', 'https_ca_file': 'ca.pem',
                    'mqtt_tls_endpoint': 'mqtts://127.0.0.1:8883', 'mqtt_tls_ca_file': 'ca.pem',
                    'mqtt_tls_server_name': 'localhost'},
        }
        gw_files = {'gw-a': 'gw-a.json', 'gw-b': 'gw-b.json'}
        tele_files = {'A': 'tele-a.json', 'B': 'tele-b.json'}
        manifests = e2_equal_samples_manifests(
            context=ctx, common=common, targets=self.targets,
            gateway_identity_files=gw_files, telemetry_identity_files=tele_files, now_utc=fixed_now)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            pair = manifests['profiles']['100']

            def write_pair(mod_a=None, mod_b=None):
                a_data = json.loads(json.dumps(pair['A']))
                b_data = json.loads(json.dumps(pair['B']))
                if mod_a:
                    mod_a(a_data)
                if mod_b:
                    mod_b(b_data)
                pa = root / 'a.json'
                pb = root / 'b.json'
                pa.write_text(json.dumps(a_data))
                pb.write_text(json.dumps(b_data))
                pa.chmod(0o600)
                pb.chmod(0o600)
                return pa, pb

            # 1. Context hash mismatch between A and B
            pa, pb = write_pair(mod_b=lambda b: b['equal_samples'].update({'context_sha256': '0' * 64}))
            with self.assertRaises(ValueError):
                build_e2_execution_plan(output_dir=root / 'p1', manifest_a=pa, manifest_b=pb)

            # 2. Tampered context hash in A
            pa, pb = write_pair(mod_a=lambda a: a['equal_samples'].update({'context_sha256': 'f' * 64}))
            with self.assertRaises(ValueError):
                build_e2_execution_plan(output_dir=root / 'p2', manifest_a=pa, manifest_b=pb)

            # 3. Expected count mismatch in A
            pa, pb = write_pair(mod_a=lambda a: a['equal_samples'].update({'expected_samples': 9999}))
            with self.assertRaises(ValueError):
                build_e2_execution_plan(output_dir=root / 'p3', manifest_a=pa, manifest_b=pb)

            # 4. Sample stream version != 1
            pa, pb = write_pair(mod_a=lambda a: a['equal_samples'].update({'sample_stream_version': 2}))
            with self.assertRaises(ValueError):
                build_e2_execution_plan(output_dir=root / 'p4', manifest_a=pa, manifest_b=pb)

            # 5. Unknown nested key in equal_samples
            pa, pb = write_pair(mod_a=lambda a: a['equal_samples'].update({'unexpected_key': True}))
            with self.assertRaises(ValueError):
                build_e2_execution_plan(output_dir=root / 'p5', manifest_a=pa, manifest_b=pb)

            # 6. Missing key in equal_samples
            def drop_seed(a):
                a['equal_samples'] = dict(a['equal_samples'])
                del a['equal_samples']['seed']
            pa, pb = write_pair(mod_a=drop_seed)
            with self.assertRaises(ValueError):
                build_e2_execution_plan(output_dir=root / 'p6', manifest_a=pa, manifest_b=pb)

            # 7. S mismatch between A and B (A has 100, B has 1)
            b_s1 = dict(manifests['profiles']['1']['B'])
            pa = root / 'a_s100.json'
            pb = root / 'b_s1.json'
            pa.write_text(json.dumps(pair['A']))
            pb.write_text(json.dumps(b_s1))
            pa.chmod(0o600); pb.chmod(0o600)
            with self.assertRaises(ValueError):
                build_e2_execution_plan(output_dir=root / 'p7', manifest_a=pa, manifest_b=pb)

            # 8. Same gateway target rejected
            pa, pb = write_pair(mod_b=lambda b: (
                b.update({'targets': [{'gateway_id': 'gw-a', 'sensor_id': 's-other'}]}),
                b.update({'gateway_identity_files': {'gw-a': 'gw-a.json'}})))
            with self.assertRaisesRegex(ValueError, 'e2_gateway_targets_not_distinct'):
                build_e2_execution_plan(output_dir=root / 'p8', manifest_a=pa, manifest_b=pb)

            # 9. Limits tampered
            pa, pb = write_pair(mod_a=lambda a: a['limits'].update({'retries': 5}))
            with self.assertRaises(ValueError):
                build_e2_execution_plan(output_dir=root / 'p9', manifest_a=pa, manifest_b=pb)

    def test_prepare_remote_equal_samples_and_steady_burst(self):
        fixed_now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        ctx = build_e2_equal_samples_context(total_samples=6000, sample_rate=100, seed=84, now_utc=fixed_now)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            ctx_path = root / 'context.json'
            ctx_path.write_text(json.dumps(ctx))

            # Helper mock setup
            class FakeEndpoint:
                url = 'https://fixture.test:14443'
                ca_file = root / 'https-ca.pem'
            FakeEndpoint.ca_file.write_text('https-ca')

            class FakeSpec:
                def __init__(self, gw, s):
                    self.gateway_id = gw
                    self.sensor_id = s

            class FakeFixture:
                private = root / 'fixture-private'
            FakeFixture.private.mkdir(parents=True)
            (FakeFixture.private / 'certs').mkdir()
            (FakeFixture.private / 'certs/ca.crt').write_text('mqtt-ca')

            smoke = Mock()
            specs = [FakeSpec('gw-a', 's-a'), FakeSpec('gw-b', 's-b')]
            passwords = ['pass-a', 'pass-b']
            smoke.prepare_paper_workload.return_value = (
                'fake-pg', {}, {}, {}, 'ready', {'admin': 'token'}, specs, passwords, None)
            smoke.db_psql.return_value = '24:telemetry_ledger_timestamp'
            def fake_private_write(path, content):
                p = Path(path)
                p.write_text(content)
                p.chmod(0o600)
            smoke.private_write.side_effect = fake_private_write

            registry = Mock()
            registry.resolve.return_value = {
                'NetworkSettings': {'Ports': {'8883/tcp': [{'HostPort': '18883'}]}}}

            # 1. Negative: equal_samples requires context, must fail BEFORE prepare_paper_workload
            smoke.prepare_paper_workload.reset_mock()
            with self.assertRaisesRegex(ValueError, 'equal_samples_requires_context'):
                prepare_remote(
                    smoke, FakeFixture, FakeEndpoint, registry, {}, {}, {},
                    session_id='sess-1', samples_per_message=100,
                    output_root=root / 'out-err', bundle_root=root / 'bnd-err',
                    workload='equal_samples', context_path=None, context=None)
            smoke.prepare_paper_workload.assert_not_called()

            # 2. Negative: invalid context fails before prepare_paper_workload
            smoke.prepare_paper_workload.reset_mock()
            bad_ctx = dict(ctx, total_samples=5999)  # not divisible by 200
            with self.assertRaises(ValueError):
                prepare_remote(
                    smoke, FakeFixture, FakeEndpoint, registry, {}, {}, {},
                    session_id='sess-2', samples_per_message=100,
                    output_root=root / 'out-err2', bundle_root=root / 'bnd-err2',
                    workload='equal_samples', context=bad_ctx)
            smoke.prepare_paper_workload.assert_not_called()

            # 3. Positive: equal_samples valid preparation
            smoke.prepare_paper_workload.reset_mock()
            out_root = root / 'out-eq'
            bnd_root = root / 'bnd-eq'
            test_images = {'backend': 'sha256:' + 'a' * 64, 'gateway': 'sha256:' + 'b' * 64}
            prepared = prepare_remote(
                smoke, FakeFixture, FakeEndpoint, registry, {'mosquitto': 'mosq'}, test_images, {},
                session_id='sess-valid', samples_per_message=100,
                output_root=out_root, bundle_root=bnd_root,
                workload='equal_samples', context=ctx, reference_path='/ref/path.json',
                check_freshness=False)
            smoke.prepare_paper_workload.assert_called_once()
            self.assertEqual(prepared['phase_id'], 'equal-samples-100')
            self.assertEqual(prepared['seed'], ctx['seed'])
            self.assertEqual(prepared['workload'], 'equal_samples')
            self.assertEqual(prepared['context']['context_sha256'], ctx['context_sha256'])
            self.assertIn('expected_counts', prepared)
            self.assertEqual(prepared['expected_counts']['total_samples'], 6000)
            self.assertEqual(prepared['expected_counts']['total_messages'], 60)
            self.assertEqual(prepared['wall_seconds'], 60 + 20 + 10)
            self.assertEqual(prepared['reference_path'], '/ref/path.json')
            self.assertTrue(Path(prepared['bundle']['bundle_root']).is_dir())
            self.assertEqual(prepared.get('images'), test_images)
            self.assertIn('bundle_inventory', prepared)
            self.assertIsInstance(prepared['bundle_inventory'], list)
            self.assertTrue(any(row.get('path') == 'manifests/A.json' for row in prepared['bundle_inventory']))

            # 3b. Regression: prepare_remote defaults to check_freshness=True, rejecting expired context
            smoke.prepare_paper_workload.reset_mock()
            with self.assertRaisesRegex(ValueError, 'equal_samples_context_expired'):
                prepare_remote(
                    smoke, FakeFixture, FakeEndpoint, registry, {'mosquitto': 'mosq'}, test_images, {},
                    session_id='sess-expired', samples_per_message=100,
                    output_root=root / 'out-expired', bundle_root=root / 'bnd-expired',
                    workload='equal_samples', context=ctx)
            smoke.prepare_paper_workload.assert_not_called()

            # 4. Positive: steady_burst legacy preparation (default)
            smoke.prepare_paper_workload.reset_mock()
            out_root_sb = root / 'out-sb'
            bnd_root_sb = root / 'bnd-sb'
            prepared_sb = prepare_remote(
                smoke, FakeFixture, FakeEndpoint, registry, {'mosquitto': 'mosq'}, {}, {},
                session_id='sess-sb', samples_per_message=100,
                output_root=out_root_sb, bundle_root=bnd_root_sb)
            smoke.prepare_paper_workload.assert_called_once()
            self.assertEqual(prepared_sb['phase_id'], 'samples-100')
            self.assertEqual(prepared_sb['seed'], 84)
            self.assertEqual(prepared_sb['workload'], 'steady_burst')
            self.assertNotIn('context', prepared_sb)
            self.assertTrue(Path(prepared_sb['bundle']['bundle_root']).is_dir())

            # Official workload must pass the real portable export/schema path with
            # counts derived from manifests rather than the 24-message pilot.
            out_off = root / 'out-official'
            bnd_off = root / 'bnd-official'
            prepared_off = prepare_remote(
                smoke, FakeFixture, FakeEndpoint, registry, {'mosquitto': 'mosq'}, test_images, {},
                session_id='sess-official', samples_per_message=100,
                output_root=out_off, bundle_root=bnd_off, profile='official',
                official_opt_in=True, offered_rate=100, duration_seconds=10, burst_messages=7,
                pending_messages=1024, pending_bytes=8 << 20)
            portable = json.loads((bnd_off / 'bundle.json').read_text())
            from scripts import paper_p8_bundle
            self.assertTrue(paper_p8_bundle.inspect_bundle(bnd_off)['valid'])
            self.assertEqual(prepared_off['plan']['expected_counts'],
                {'unique':1007,'raw':100700,'temporal':1007,'ack':1007})
            self.assertEqual(portable['workload']['producer_rates_per_second'], {'A':50,'B':50})
            self.assertEqual(portable['workload']['pending_limits'],
                             {'pending_messages': 1024, 'pending_bytes': 8 << 20})
            self.assertEqual(portable['workload']['estimate']['unique_messages_estimate'], 1007)
            workspace = root / 'bound-official'
            paper_p8_bundle.bind_bundle(bundle_root=bnd_off, workspace_leaf=workspace)
            for arm in ('A', 'B'):
                manifest = json.loads((workspace / 'runtime' / 'manifests' / f'{arm}.json').read_text())
                self.assertEqual(manifest['limits']['pending_messages'], 1024)
                self.assertEqual(manifest['limits']['pending_bytes'], 8 << 20)
            with self.assertRaisesRegex(ValueError, 'bundle_workload_metadata_rejected'):
                bad_workload = json.loads(json.dumps(portable['workload']))
                bad_workload['pending_limits']['pending_messages'] = 64
                paper_p8_bundle._validate_e2_official_workload(bad_workload)
            with self.assertRaisesRegex(ValueError, 'bundle_workload_metadata_rejected'):
                paper_p8_bundle._validate_e2_official_workload(dict(portable['workload'], pending_limits=None))
            self.assertEqual(prepared_off['run_id'], prepared_off['manifests']['A']['run_id'])
            self.assertEqual(portable['workload']['expected_counts'], prepared_off['plan']['expected_counts'])
            with self.assertRaisesRegex(ValueError, 'bundle_workload_metadata_rejected'):
                bad_workload = dict(portable['workload'], offered_rate_per_second=250)
                paper_p8_bundle._validate_e2_official_workload(bad_workload)
            # Even a recomputed inventory digest cannot make a changed bound budget
            # agree with the independently declared workload provenance.
            manifest_path = bnd_off / 'manifests' / 'B.json'
            changed = json.loads(manifest_path.read_text())
            changed['limits']['pending_messages'] = 512
            data = json.dumps(changed, sort_keys=True, separators=(',', ':')).encode()
            manifest_path.write_bytes(data)
            for item in portable['inputs']:
                if item['path'] == 'manifests/B.json':
                    item['sha256'] = hashlib.sha256(data).hexdigest()
                    item['size_bytes'] = len(data)
            (bnd_off / 'bundle.json').write_text(json.dumps(portable))
            with self.assertRaisesRegex(ValueError, 'bundle_e2_pending_limits_mismatch'):
                paper_p8_bundle.inspect_bundle(bnd_off)

    def _create_test_equal_samples_bundle(self, root, ctx, *, binary_bytes=b'binary-123',
                                          session_id='sess-e2', run_id='p8e2-sess-100', arm='e2-test',
                                          phase_id='equal-samples-100', samples_per_message=100,
                                          profile='pilot',
                                          a_mod=None, b_mod=None, summary_mod=None, provenance_mod=None,
                                          generator_resources=None):
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
        binary_file = root / 'paperload_bin'
        binary_file.write_bytes(binary_bytes)
        binary_file.chmod(0o700)
        bin_hash = hashlib.sha256(binary_bytes).hexdigest()

        bundle_dir = root / 'result_bundle'
        bundle_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        results_dir = bundle_dir / 'results'
        results_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        (results_dir / 'runner').mkdir(mode=0o700, parents=True, exist_ok=True)
        (results_dir / 'workloads/A').mkdir(mode=0o700, parents=True, exist_ok=True)
        (results_dir / 'workloads/B').mkdir(mode=0o700, parents=True, exist_ok=True)

        exp_counts = calculate_e2_equal_samples_expected_counts(ctx, samples_per_message)
        expected_s = exp_counts['per_role']['A']['samples']
        expected_m = exp_counts['per_role']['A']['messages']

        def make_workload(role):
            mid = '550e8400-e29b-41d4-a716-446655440001' if role == 'A' else '550e8400-e29b-41d4-a716-446655440002'
            gw = 'gw-a' if role == 'A' else 'gw-b'
            s = 's-a' if role == 'A' else 's-b'
            return {
                'status': 'COMPLETE',
                'implementation_status': 'P8.4b telemetry synthetic-load; no durable restart assurance',
                'experiment': 'E2',
                'arm': role,
                'profile': profile,
                'mode': 'remote',
                'run_id': run_id,
                'durability_mode': 'synthetic-load',
                'started_at': '2026-10-31T12:00:00.000000000Z',
                'finished_at': '2026-10-31T12:01:00.000000000Z',
                'configured_phases': {'warmup_seconds': 0, 'measure_seconds': 60, 'drain_seconds': 20, 'cooldown_seconds': 0},
                'telemetry': {
                    'scheduled_opportunities': expected_m, 'generated_unique': expected_m, 'offered_unique': expected_m,
                    'application_acks': expected_m, 'pending_messages': 0, 'pending_bytes': 0,
                    'publish_attempts': expected_m, 'broker_pubacks': expected_m, 'retries': 0, 'invalid_acks': 0,
                },
                'identity_sampling_qualification': 'header_sha256_only; maximum 20 globally; not every payload; not a full-content audit',
                'identity_samples': [{
                    'viewer_id': '',
                    'gateway_id': gw,
                    'sensor_id': s,
                    'message_id': mid,
                    'boot_id': 'boot-1',
                    'first_sequence': '0',
                    'sample_count': 100,
                    'header_sha256': 'a' * 64,
                }],
                'equal_samples': {
                    'context_sha256': ctx['context_sha256'],
                    'workload_type': 'equal_samples',
                    'logical_producer': role,
                    'total_samples': ctx['total_samples'],
                    'sample_rate': ctx['sample_rate'],
                    'duration_seconds': ctx['duration_seconds'],
                    'producer_sample_rate': ctx['producer_sample_rate'],
                    'sample_interval_us': ctx['sample_interval_us'],
                    'samples_per_message': samples_per_message,
                    'expected_samples': expected_s,
                    'expected_messages': expected_m,
                    'generated_samples': expected_s,
                    'generated_messages': expected_m,
                    'offered_unique_samples': expected_s,
                    'offered_unique_messages': expected_m,
                    'application_acks': expected_m,
                    'offered_unique_bytes': 15000,
                    'attempted_bytes': 15000,
                    'schedule_misses': 0,
                    'scheduling_lag_max_ns': 1250000,
                    'completed_on_time': True,
                    'sampling_qualification': 'logical_offered_stream; not physical sensor acquisition latency; commit ack from application outbox only'
                }
            }

        workload_a = make_workload('A')
        if a_mod:
            a_mod(workload_a)
        workload_b = make_workload('B')
        if b_mod:
            b_mod(workload_b)

        gen_m_start = 0.0
        if generator_resources and isinstance(generator_resources, dict):
            gen_m_start = float(generator_resources.get('clock', {}).get('monotonic_start', 0.0))
        b_begin_ns = int((gen_m_start + 0.0) * 1e9) if gen_m_start else 1000
        b_end_ns = int((gen_m_start + 2.0) * 1e9) if gen_m_start else 61000
        a_begin_ns = b_begin_ns if gen_m_start else 1000
        a_end_ns = b_end_ns if gen_m_start else 61000

        run_json = {
            'status': 'COMPLETE',
            'phase_observations': {
                'status': 'observed',
                'events': [
                    {'child': 'paperload-telemetry:A.manifest.json', 'event': 'measure_begin', 'elapsed_ns': 100, 'observed_at_runner_monotonic_ns': a_begin_ns},
                    {'child': 'paperload-telemetry:A.manifest.json', 'event': 'measure_end', 'elapsed_ns': 60000000000, 'observed_at_runner_monotonic_ns': a_end_ns},
                    {'child': 'paperload-telemetry:B.manifest.json', 'event': 'measure_begin', 'elapsed_ns': 100, 'observed_at_runner_monotonic_ns': b_begin_ns},
                    {'child': 'paperload-telemetry:B.manifest.json', 'event': 'measure_end', 'elapsed_ns': 60000000000, 'observed_at_runner_monotonic_ns': b_end_ns}
                ]
            }
        }
        prov_json = {
            'binary_sha256': bin_hash,
            'binary_os': sys.platform,
            'binary_arch': platform.machine()
        }
        if provenance_mod:
            provenance_mod(prov_json)

        files_data = {
            'results/workloads/A/workload.json': json.dumps(workload_a, separators=(',', ':')).encode(),
            'results/workloads/B/workload.json': json.dumps(workload_b, separators=(',', ':')).encode(),
            'results/runner/run.json': json.dumps(run_json, separators=(',', ':')).encode(),
            'results/runner/provenance.json': json.dumps(prov_json, separators=(',', ':')).encode(),
        }
        if generator_resources is not None:
            files_data['results/runner/resources-generator.json'] = json.dumps(generator_resources, separators=(',', ':')).encode()
        inventory = []
        for rel_path, data in files_data.items():
            f_path = bundle_dir / rel_path
            f_path.write_bytes(data)
            f_path.chmod(0o600)
            inventory.append({
                'path': rel_path,
                'size_bytes': len(data),
                'sha256': hashlib.sha256(data).hexdigest()
            })

        bundle_sha256 = 'b' * 64
        bundle_inventory = [{'path': 'input.json', 'sha256': 'a' * 64}]
        manifest_a_hash = '1' * 64
        manifest_b_hash = '2' * 64
        run_spec_hash = '3' * 64

        results_summary = {
            'schema_version': 1,
            'session': {
                'session_id': session_id,
                'run_ids': [run_id],
                'experiment': 'E2',
                'arm': arm,
                'phase': phase_id
            },
            'source_bundle_sha256': bundle_sha256,
            'source_inventory': bundle_inventory,
            'bound_manifest_sha256': {'A': manifest_a_hash, 'B': manifest_b_hash},
            'bound_gateway_environment_sha256': {},
            'bound_run_spec_sha256': run_spec_hash,
            'binary': dict(prov_json),
            'files': inventory,
            'runner_status': 'COMPLETE',
            'expected_workload_roles': ['A', 'B'],
            'present_workload_roles': ['A', 'B'],
            'missing_workload_roles': [],
            'full_p8_accepted': False,
            'physical_ab': 'NOT RUN'
        }
        if summary_mod:
            summary_mod(results_summary)

        summary_file = bundle_dir / 'results.json'
        summary_file.write_text(json.dumps(results_summary))
        summary_file.chmod(0o600)

        prepared = {
            'session_id': session_id,
            'run_id': run_id,
            'arm': arm,
            'phase_id': phase_id,
            'profile': profile,
            'workload': 'equal_samples',
            'samples_per_message': samples_per_message,
            'bundle_sha256': bundle_sha256,
            'bundle_inventory': bundle_inventory,
            'bound_manifest_sha256': {'A': manifest_a_hash, 'B': manifest_b_hash},
            'context': ctx,
            'expected_counts': exp_counts,
            'seed': ctx['seed'],
        }
        return bundle_dir, binary_file, prepared

    def test_load_e2_result_bundle_equal_samples_positive_and_relocation(self):
        fixed_now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        ctx = build_e2_equal_samples_context(total_samples=6000, sample_rate=100, seed=84, now_utc=fixed_now)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            bundle_dir, binary_file, prepared = self._create_test_equal_samples_bundle(root, ctx)

            # Test relocation to another directory
            relocated = root / 'relocated_bundle'
            shutil.copytree(bundle_dir, relocated)

            imported = e2._load_e2_result_bundle(relocated, prepared, binary_file)
            self.assertIn('A', imported)
            self.assertIn('B', imported)
            self.assertIn('runner', imported)
            self.assertIn('binary_provenance', imported)

            eq_a = imported['A']['equal_samples']
            self.assertEqual(len(eq_a), 22)
            self.assertEqual(eq_a['logical_producer'], 'A')
            self.assertEqual(eq_a['context_sha256'], ctx['context_sha256'])
            self.assertEqual(eq_a['total_samples'], 6000)
            self.assertEqual(eq_a['expected_samples'], 3000)
            self.assertEqual(eq_a['expected_messages'], 30)

            eq_b = imported['B']['equal_samples']
            self.assertEqual(eq_b['logical_producer'], 'B')
            self.assertEqual(eq_b['context_sha256'], ctx['context_sha256'])

            # Test historical context from year 2024: no freshness rejection on historical result import
            hist_ctx = build_e2_equal_samples_context(
                timestamp_anchor='2024-01-01T00:00:00Z',
                now_utc=datetime(2024, 1, 1, 0, 5, 0, tzinfo=timezone.utc),
                seed=84,
            )
            hist_bundle, hist_bin, hist_prep = self._create_test_equal_samples_bundle(root / 'hist', hist_ctx)
            hist_imported = e2._load_e2_result_bundle(hist_bundle, hist_prep, hist_bin)
            self.assertEqual(hist_imported['A']['equal_samples']['context_sha256'], hist_ctx['context_sha256'])

    def test_load_e2_result_bundle_accepts_portable_session_profile(self):
        fixed_now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        ctx = build_e2_equal_samples_context(total_samples=6000, sample_rate=100, seed=84, now_utc=fixed_now)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)

            def add_profile(summary):
                summary['session']['profile'] = 'pilot'

            bundle_dir, binary_file, prepared = self._create_test_equal_samples_bundle(
                root, ctx, summary_mod=add_profile)
            e2._load_e2_result_bundle(bundle_dir, prepared, binary_file)

            def add_wrong_profile(summary):
                summary['session']['profile'] = 'official'

            bad_bundle, bad_binary, bad_prepared = self._create_test_equal_samples_bundle(
                root / 'wrong-profile', ctx, summary_mod=add_wrong_profile)
            with self.assertRaisesRegex(ValueError, 'e2_result_correlation_rejected'):
                e2._load_e2_result_bundle(bad_bundle, bad_prepared, bad_binary)

    def test_load_e2_result_bundle_equal_samples_negatives(self):
        fixed_now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        ctx = build_e2_equal_samples_context(total_samples=6000, sample_rate=100, seed=84, now_utc=fixed_now)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)

            # 1. Wrong context hash in workload
            b_dir, b_bin, prep = self._create_test_equal_samples_bundle(
                root / 'neg1', ctx, a_mod=lambda a: a['equal_samples'].update({'context_sha256': '0' * 64}))
            with self.assertRaisesRegex(ValueError, 'e2_equal_samples_context_hash_mismatch'):
                e2._load_e2_result_bundle(b_dir, prep, b_bin)

            # 2. Wrong profile in workload
            b_dir, b_bin, prep = self._create_test_equal_samples_bundle(
                root / 'neg2', ctx, a_mod=lambda a: a.update({'profile': 'official'}))
            with self.assertRaisesRegex(ValueError, 'e2_workload_provenance_mismatch'):
                e2._load_e2_result_bundle(b_dir, prep, b_bin)

            # 3. Wrong expected counts
            b_dir, b_bin, prep = self._create_test_equal_samples_bundle(
                root / 'neg3', ctx, a_mod=lambda a: a['equal_samples'].update({'expected_samples': 9999}))
            with self.assertRaisesRegex(ValueError, 'e2_equal_samples_expected_counts_mismatch'):
                e2._load_e2_result_bundle(b_dir, prep, b_bin)

            # 4. Wrong samples_per_message
            b_dir, b_bin, prep = self._create_test_equal_samples_bundle(
                root / 'neg4', ctx, a_mod=lambda a: a['equal_samples'].update({'samples_per_message': 1}))
            with self.assertRaisesRegex(ValueError, 'e2_equal_samples_samples_per_message_mismatch'):
                e2._load_e2_result_bundle(b_dir, prep, b_bin)

            # 5. Missing equal_samples in workload
            def drop_eq(a):
                del a['equal_samples']
            b_dir, b_bin, prep = self._create_test_equal_samples_bundle(root / 'neg5', ctx, a_mod=drop_eq)
            with self.assertRaisesRegex(ValueError, 'e2_equal_samples_missing_in_workload'):
                e2._load_e2_result_bundle(b_dir, prep, b_bin)

            # 6. Mode mismatch: steady_burst expected, but equal_samples in workload
            b_dir, b_bin, prep = self._create_test_equal_samples_bundle(root / 'neg6', ctx, phase_id='samples-100')
            prep_sb = dict(prep, workload='steady_burst', phase_id='samples-100')
            del prep_sb['context']
            del prep_sb['expected_counts']
            with self.assertRaisesRegex(ValueError, 'e2_workload_mode_mismatch'):
                e2._load_e2_result_bundle(b_dir, prep_sb, b_bin)

            # 7. Wrong binary SHA256 in provenance
            b_dir, b_bin, prep = self._create_test_equal_samples_bundle(
                root / 'neg7', ctx, provenance_mod=lambda p: p.update({'binary_sha256': 'f' * 64}))
            with self.assertRaisesRegex(ValueError, 'e2_binary_provenance_mismatch'):
                e2._load_e2_result_bundle(b_dir, prep, b_bin)

            # 8. Missing role B
            b_dir, b_bin, prep = self._create_test_equal_samples_bundle(
                root / 'neg8', ctx, summary_mod=lambda s: s.update({'expected_workload_roles': ['A'], 'present_workload_roles': ['A']}))
            with self.assertRaises(ValueError):
                e2._load_e2_result_bundle(b_dir, prep, b_bin)

            # 9. Invalid root permission (0o777)
            b_dir, b_bin, prep = self._create_test_equal_samples_bundle(root / 'neg9', ctx)
            b_dir.chmod(0o777)
            with self.assertRaisesRegex(ValueError, 'e2_result_root_rejected'):
                e2._load_e2_result_bundle(b_dir, prep, b_bin)
            b_dir.chmod(0o700)

            # 10. Symlink in results root
            symlink_dir = root / 'symlink_bundle'
            symlink_dir.symlink_to(b_dir, target_is_directory=True)
            with self.assertRaisesRegex(ValueError, 'e2_result_root_rejected'):
                e2._load_e2_result_bundle(symlink_dir, prep, b_bin)

    def test_finish_remote_collection_keeps_collector_running_on_import_failure_and_succeeds_on_retry(self):
        class MockCollector:
            def __init__(self):
                self.stopped = False
                self.metrics = []
                self.resources = []
                self.errors = []
            def stop(self, pid):
                self.stopped = True

        fixed_now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        ctx = build_e2_equal_samples_context(total_samples=6000, sample_rate=100, seed=84, now_utc=fixed_now)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            bundle_dir, binary_file, prepared = self._create_test_equal_samples_bundle(root, ctx)

            collector = MockCollector()
            prepared['collector'] = collector

            class FakeSpec:
                def __init__(self, gw, s):
                    self.gateway_id = gw
                    self.sensor_id = s
                    self.sensor_urn = f'urn:ngsi-ld:Sensor:{gw}:{s}'

            specs = [FakeSpec('gw-a', 's-a'), FakeSpec('gw-b', 's-b')]
            prepared['specs'] = specs
            prepared['pg'] = 'fake-pg'
            prepared['tokens'] = {'admin': 'admin-tok'}
            prepared['before_global'] = {'ledger': 0, 'raw': 0, 'temporal': 0, 'configuration': 0}
            prepared['before_targets'] = [{'configuration': 0}, {'configuration': 0}]
            prepared['baseline'] = {'accepted_batches': 0, 'received_messages': 0}

            class FakeFixture:
                artifacts = root / 'artifacts'
            FakeFixture.artifacts.mkdir(mode=0o700, parents=True)

            smoke = Mock()
            smoke.http_request.return_value = (200, {}, {'accepted_batches': 60, 'received_messages': 60,
                                                         'mqtt_ingress_batches_accepted': 60})
            smoke.paper_p0_stats.return_value = {'cpu': 10}
            smoke.paper_p1_count_db.return_value = {'ledger': 60, 'raw': 60, 'temporal': 60, 'configuration': 0}
            def fake_psql(reg, pg, sql, params):
                if 'FROM processed_messages p1' in sql:
                    return '0'
                if 'sample_count IS DISTINCT FROM (SELECT count(*)' in sql or 'sample_count <>' in sql:
                    return '0'
                if 'json_build_object' in sql and 'ledger_count' in sql:
                    return json.dumps({
                        'ledger_count': 30, 'min_first_sequence': 0, 'max_last_sequence': 2999,
                        'total_sample_count': 3000, 'invalid_sample_count': 0, 'invalid_sequence_bounds': 0,
                    })
                if 'FROM telemetry t' in sql and 'unique_sequence_count' in sql:
                    return json.dumps({
                        'raw_count': 3000, 'unique_sequence_count': 3000, 'min_sequence': 0,
                        'max_sequence': 2999, 'value_mismatches': 0, 'timestamp_mismatches': 0,
                    })
                if 'SELECT coalesce(json_agg' in sql or 'LIMIT 20' in sql:
                    return json.dumps([{'message_id': params.get('message_a', '550e8400-e29b-41d4-a716-446655440001'),
                                        'first_sequence': '0', 'last_sequence': '99', 'sample_count': 100}])
                return json.dumps({'ledger': 30, 'raw': 30, 'temporal': 30, 'configuration': 0})
            smoke.db_psql.side_effect = fake_psql
            smoke.durable_json = Mock()

            # Set invalid permissions on bundle_dir to cause _load_e2_result_bundle failure
            bundle_dir.chmod(0o777)

            with self.assertRaisesRegex(ValueError, 'e2_result_root_rejected'):
                e2.finish_remote_collection(
                    smoke, FakeFixture, Mock(url='https://127.0.0.1:443'), Mock(), {},
                    prepared, bundle_dir, phase_id=prepared['phase_id'], paperload_binary=binary_file)

            # Collector must NOT have been stopped!
            self.assertFalse(collector.stopped)

            # Now restore valid permissions and retry
            bundle_dir.chmod(0o700)

            result = e2.finish_remote_collection(
                smoke, FakeFixture, Mock(url='https://127.0.0.1:443'), Mock(), {},
                prepared, bundle_dir, phase_id=prepared['phase_id'], paperload_binary=binary_file)

            # On successful import, collector MUST have been stopped!
            self.assertTrue(collector.stopped)
            self.assertEqual(result['phase_id'], 'equal-samples-100')
            self.assertIn('A', result['workloads'])
            self.assertIn('B', result['workloads'])
            self.assertIn('equal_samples', result['workloads']['A'])
            self.assertEqual(result['workloads']['A']['equal_samples']['logical_producer'], 'A')
            self.assertEqual(result['workloads']['B']['equal_samples']['logical_producer'], 'B')

    def _build_test_equal_samples_validation_payload(
        self, ctx, samples_per_message=100, *,
        status_a='COMPLETE', status_b='COMPLETE',
        retries_a=0, retries_b=0,
        misses_a=0, misses_b=0,
        pending_a=0, pending_b=0,
        primary_error_a=None, primary_error_b=None,
        invalid_acks_a=0, invalid_acks_b=0,
        delta_override=None,
        by_gateway_override=None,
        raw_verification_override=None,
        phase_events_override=None,
        ingress_override=None,
        rate_limited=0,
        queue_full=0,
        exec_ctx_override=None,
        role_mapping=None,
    ):
        exp_counts = calculate_e2_equal_samples_expected_counts(ctx, samples_per_message)
        exp_s = exp_counts['per_role']['A']['samples']
        exp_m = exp_counts['per_role']['A']['messages']
        total_s = exp_counts['total_samples']
        total_m = exp_counts['total_messages']

        def make_workload(role, status, retries, misses, pending, primary_err, invalid_acks):
            w = {
                'status': status,
                'experiment': 'E2',
                'arm': role,
                'profile': 'pilot',
                'mode': 'remote',
                'run_id': f'test-run-{samples_per_message}',
                'telemetry': {
                    'generated_unique': exp_m,
                    'offered_unique': exp_m,
                    'application_acks': exp_m,
                    'pending_messages': pending,
                    'pending_bytes': pending * 100,
                    'publish_attempts': exp_m + retries,
                    'broker_pubacks': exp_m + retries,
                    'retries': retries,
                    'invalid_acks': invalid_acks,
                },
                'equal_samples': {
                    'context_sha256': ctx['context_sha256'],
                    'workload_type': 'equal_samples',
                    'logical_producer': role,
                    'total_samples': ctx['total_samples'],
                    'sample_rate': ctx['sample_rate'],
                    'duration_seconds': ctx['duration_seconds'],
                    'producer_sample_rate': ctx['producer_sample_rate'],
                    'sample_interval_us': ctx['sample_interval_us'],
                    'samples_per_message': samples_per_message,
                    'expected_samples': exp_s,
                    'expected_messages': exp_m,
                    'generated_samples': exp_s,
                    'generated_messages': exp_m,
                    'offered_unique_samples': exp_s,
                    'offered_unique_messages': exp_m,
                    'application_acks': exp_m,
                    'offered_unique_bytes': exp_s * 50,
                    'attempted_bytes': (exp_s + retries * samples_per_message) * 50,
                    'schedule_misses': misses,
                    'scheduling_lag_max_ns': 1000000,
                    'completed_on_time': True,
                    'sampling_qualification': 'logical_offered_stream; not physical sensor acquisition latency; commit ack from application outbox only',
                }
            }
            if primary_err:
                w['primary_error'] = primary_err
            return w

        wa = make_workload('A', status_a, retries_a, misses_a, pending_a, primary_error_a, invalid_acks_a)
        wb = make_workload('B', status_b, retries_b, misses_b, pending_b, primary_error_b, invalid_acks_b)

        mapping = role_mapping or {
            'A': {'gateway_id': 'gw-a', 'sensor_id': 's-a'},
            'B': {'gateway_id': 'gw-b', 'sensor_id': 's-b'},
        }

        database_delta = delta_override if delta_override is not None else {
            'ledger': total_m, 'raw': total_s, 'temporal': total_m, 'configuration': 0,
        }

        database_by_gateway = by_gateway_override if by_gateway_override is not None else {
            'gw-a': {'ledger': exp_m, 'raw': exp_s, 'temporal': exp_m, 'configuration': 0},
            'gw-b': {'ledger': exp_m, 'raw': exp_s, 'temporal': exp_m, 'configuration': 0},
        }

        raw_verification = raw_verification_override if raw_verification_override is not None else [
            {
                'gateway_id': 'gw-a',
                'sensor_id': 's-a',
                'ledger_count': exp_m,
                'raw_count': exp_s,
                'raw_identity_value_mismatches': 0,
                'timestamp_mismatches': 0,
                'sequence_gaps_or_overlaps': 0,
                'provenance_mismatches': 0,
                'identity_ranges': [{'message_id': '550e8400-e29b-41d4-a716-446655440001', 'first_sequence': '0', 'last_sequence': str(samples_per_message - 1), 'sample_count': samples_per_message}],
                'verified_exact': True,
            },
            {
                'gateway_id': 'gw-b',
                'sensor_id': 's-b',
                'ledger_count': exp_m,
                'raw_count': exp_s,
                'raw_identity_value_mismatches': 0,
                'timestamp_mismatches': 0,
                'sequence_gaps_or_overlaps': 0,
                'provenance_mismatches': 0,
                'identity_ranges': [{'message_id': '550e8400-e29b-41d4-a716-446655440002', 'first_sequence': '0', 'last_sequence': str(samples_per_message - 1), 'sample_count': samples_per_message}],
                'verified_exact': True,
            },
        ]

        phase_events = phase_events_override if phase_events_override is not None else [
            {'child': 'paperload-telemetry:A.manifest.json', 'event': 'measure_begin', 'elapsed_ns': 100, 'observed_at_runner_monotonic_ns': 1000},
            {'child': 'paperload-telemetry:A.manifest.json', 'event': 'measure_end', 'elapsed_ns': 60000000000, 'observed_at_runner_monotonic_ns': 60001000},
            {'child': 'paperload-telemetry:B.manifest.json', 'event': 'measure_begin', 'elapsed_ns': 100, 'observed_at_runner_monotonic_ns': 1000},
            {'child': 'paperload-telemetry:B.manifest.json', 'event': 'measure_end', 'elapsed_ns': 60000000000, 'observed_at_runner_monotonic_ns': 60001000},
        ]

        ingress_delta = ingress_override if ingress_override is not None else {
            'available': True,
            'delta': {
                'Committed': total_m,
                'SamplesNew': total_s,
                'ACKPublished': total_m,
                'Received': total_m,
                'Accepted': total_m,
                'RejectedNotRunning': 0,
                'RejectedStopping': 0,
                'RejectedRetained': 0,
                'RejectedOversized': 0,
                'RejectedInvalidTopic': 0,
                'QueueFull': queue_full,
                'ByteBudgetFull': 0,
                'ShaperRateLimited': rate_limited,
            }
        }

        execution_context = exec_ctx_override if exec_ctx_override is not None else {
            'workload': 'equal_samples',
            'samples_per_message': samples_per_message,
            'role_to_target': mapping,
            'context': ctx,
            'expected_counts': exp_counts,
        }

        return {
            'workload_a': wa,
            'workload_b': wb,
            'ingress_delta': ingress_delta,
            'database_delta': database_delta,
            'database_by_gateway': database_by_gateway,
            'phase_events': phase_events,
            'raw_verification': raw_verification,
            'execution_context': execution_context,
        }

    def test_validate_e2_equal_samples_result_valid_s1_and_s100(self):
        fixed_now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        ctx = build_e2_equal_samples_context(total_samples=6000, sample_rate=100, seed=84, now_utc=fixed_now)

        for S in (100, 1):
            payload = self._build_test_equal_samples_validation_payload(ctx, samples_per_message=S)
            res = validate_e2_equal_samples_result(**payload)
            self.assertTrue(res['valid'], f"Failed for S={S}: {res.get('failures')}")
            self.assertTrue(res['clean_comparison'], f"Contaminated for S={S}: {res.get('contamination_reasons')}")
            self.assertEqual(res['failures'], [])
            self.assertEqual(res['contamination_reasons'], [])
            self.assertFalse(res['full_p8_accepted'])
            self.assertEqual(res['generator_colocated'], 'unverified')
            self.assertEqual(res['physical_ab'], 'NOT RUN')
            self.assertEqual(res['availability'], 'available')

    def test_validate_e2_equal_samples_result_contaminated_but_functionally_valid(self):
        fixed_now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        ctx = build_e2_equal_samples_context(total_samples=6000, sample_rate=100, seed=84, now_utc=fixed_now)

        # 1. Contamination by telemetry retry in A
        p1 = self._build_test_equal_samples_validation_payload(ctx, samples_per_message=100, retries_a=2)
        r1 = validate_e2_equal_samples_result(**p1)
        self.assertTrue(r1['valid'], "Functionally valid data must have valid=True")
        self.assertFalse(r1['clean_comparison'], "Run with retries must not be clean_comparison")
        self.assertEqual(r1['failures'], [])
        self.assertTrue(any('A_retries_observed:2' in c for c in r1['contamination_reasons']))

        # 2. Contamination by schedule misses in B
        p2 = self._build_test_equal_samples_validation_payload(ctx, samples_per_message=100, misses_b=1)
        r2 = validate_e2_equal_samples_result(**p2)
        self.assertTrue(r2['valid'])
        self.assertFalse(r2['clean_comparison'])
        self.assertTrue(any('B_schedule_misses_observed:1' in c for c in r2['contamination_reasons']))

        # 3. Contamination by shaper rate limiting in ingress
        p3 = self._build_test_equal_samples_validation_payload(ctx, samples_per_message=100, rate_limited=5)
        r3 = validate_e2_equal_samples_result(**p3)
        self.assertTrue(r3['valid'])
        self.assertFalse(r3['clean_comparison'])
        self.assertTrue(any('ingress_shaper_rate_limited:5' in c for c in r3['contamination_reasons']))

        # 4. Contamination by collector errors
        p4 = self._build_test_equal_samples_validation_payload(ctx, samples_per_message=100)
        p4['resources'] = {'collector_errors': ['transient_scrape_timeout']}
        r4 = validate_e2_equal_samples_result(**p4)
        self.assertTrue(r4['valid'])
        self.assertFalse(r4['clean_comparison'])
        self.assertTrue(any('collector_errors' in c for c in r4['contamination_reasons']))

    def test_validate_e2_equal_samples_result_failures(self):
        fixed_now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        ctx = build_e2_equal_samples_context(total_samples=6000, sample_rate=100, seed=84, now_utc=fixed_now)

        # 1. Status not complete
        p = self._build_test_equal_samples_validation_payload(ctx, status_a='FAILED')
        r = validate_e2_equal_samples_result(**p)
        self.assertFalse(r['valid'])
        self.assertIn('workload_status', r['failures'])

        # 2. Telemetry counter mismatch (generated_unique wrong)
        p = self._build_test_equal_samples_validation_payload(ctx)
        p['workload_a']['telemetry']['generated_unique'] = 29
        r = validate_e2_equal_samples_result(**p)
        self.assertFalse(r['valid'])
        self.assertIn('A_generated_unique', r['failures'])

        # 3. Pending messages > 0
        p = self._build_test_equal_samples_validation_payload(ctx, pending_a=1)
        r = validate_e2_equal_samples_result(**p)
        self.assertFalse(r['valid'])
        self.assertIn('A_pending_messages', r['failures'])

        # 4. Primary error present
        p = self._build_test_equal_samples_validation_payload(ctx, primary_error_b='gateway_unreachable')
        r = validate_e2_equal_samples_result(**p)
        self.assertFalse(r['valid'])
        self.assertIn('B_primary_error', r['failures'])

        # 5. Telemetry vs equal_samples summary cross-check failure
        p = self._build_test_equal_samples_validation_payload(ctx)
        p['workload_a']['equal_samples']['offered_unique_messages'] = 999
        r = validate_e2_equal_samples_result(**p)
        self.assertFalse(r['valid'])
        self.assertIn('A_equal_samples_telemetry_cross_check_mismatch', r['failures'])

        # 6. Global DB delta mismatch (e.g. raw is 5999 instead of 6000)
        p = self._build_test_equal_samples_validation_payload(ctx, delta_override={'ledger': 60, 'raw': 5999, 'temporal': 60, 'configuration': 0})
        r = validate_e2_equal_samples_result(**p)
        self.assertFalse(r['valid'])
        self.assertIn('database_delta_mismatch', r['failures'])

        # 7. Scoped DB gateway delta mismatch (gw-a has wrong raw count)
        bad_by_gw = {
            'gw-a': {'ledger': 30, 'raw': 2999, 'temporal': 30, 'configuration': 0},
            'gw-b': {'ledger': 30, 'raw': 3000, 'temporal': 30, 'configuration': 0},
        }
        p = self._build_test_equal_samples_validation_payload(ctx, by_gateway_override=bad_by_gw)
        r = validate_e2_equal_samples_result(**p)
        self.assertFalse(r['valid'])
        self.assertIn('database_gateway_delta_mismatch', r['failures'])

        # 8. Scoped DB gateway scope mismatch (wrong gateway namespace)
        wrong_ns_by_gw = {
            'gw-a': {'ledger': 30, 'raw': 3000, 'temporal': 30, 'configuration': 0},
            'gw-unknown': {'ledger': 30, 'raw': 3000, 'temporal': 30, 'configuration': 0},
        }
        p = self._build_test_equal_samples_validation_payload(ctx, by_gateway_override=wrong_ns_by_gw)
        r = validate_e2_equal_samples_result(**p)
        self.assertFalse(r['valid'])
        self.assertIn('database_gateway_scope_mismatch', r['failures'])

        # 9. Raw verification failed (verified_exact is False)
        bad_raw = [
            {'gateway_id': 'gw-a', 'sensor_id': 's-a', 'ledger_count': 30, 'raw_count': 3000,
             'raw_identity_value_mismatches': 1, 'timestamp_mismatches': 0, 'verified_exact': False},
            {'gateway_id': 'gw-b', 'sensor_id': 's-b', 'ledger_count': 30, 'raw_count': 3000,
             'raw_identity_value_mismatches': 0, 'timestamp_mismatches': 0, 'verified_exact': True},
        ]
        p = self._build_test_equal_samples_validation_payload(ctx, raw_verification_override=bad_raw)
        r = validate_e2_equal_samples_result(**p)
        self.assertFalse(r['valid'])
        self.assertIn('equal_samples_raw_verification_failed', r['failures'])

        # 10. Phase intervals non-overlapping
        no_overlap_events = [
            {'child': 'paperload-telemetry:A.manifest.json', 'event': 'measure_begin', 'elapsed_ns': 100, 'observed_at_runner_monotonic_ns': 1000},
            {'child': 'paperload-telemetry:A.manifest.json', 'event': 'measure_end', 'elapsed_ns': 10000, 'observed_at_runner_monotonic_ns': 10000},
            {'child': 'paperload-telemetry:B.manifest.json', 'event': 'measure_begin', 'elapsed_ns': 20000, 'observed_at_runner_monotonic_ns': 20000},
            {'child': 'paperload-telemetry:B.manifest.json', 'event': 'measure_end', 'elapsed_ns': 30000, 'observed_at_runner_monotonic_ns': 30000},
        ]
        p = self._build_test_equal_samples_validation_payload(ctx, phase_events_override=no_overlap_events)
        r = validate_e2_equal_samples_result(**p)
        self.assertFalse(r['valid'])
        self.assertIn('a_measure_does_not_overlap_b_measure', r['failures'])

    def test_validate_e2_result_mode_dispatch(self):
        fixed_now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        ctx = build_e2_equal_samples_context(total_samples=6000, sample_rate=100, seed=84, now_utc=fixed_now)

        # 1. With workload='equal_samples', validate_e2_result dispatches to validate_e2_equal_samples_result
        payload = self._build_test_equal_samples_validation_payload(ctx, samples_per_message=100)
        res = validate_e2_result(**payload)
        self.assertTrue(res['valid'])
        self.assertEqual(res['generator_colocated'], 'unverified')
        self.assertIn('clean_comparison', res)

        # 2. Without equal_samples, validate_e2_result uses legacy validation (and fails on 30/3000 counts)
        legacy_exec_ctx = {'samples_per_message': 100, 'role_to_target': payload['execution_context']['role_to_target']}
        legacy_payload = dict(payload, execution_context=legacy_exec_ctx)
        # Drop equal_samples from workload dicts so dispatch detects steady_burst
        legacy_wa = dict(payload['workload_a'])
        del legacy_wa['equal_samples']
        legacy_wb = dict(payload['workload_b'])
        del legacy_wb['equal_samples']
        legacy_payload['workload_a'] = legacy_wa
        legacy_payload['workload_b'] = legacy_wb
        legacy_res = validate_e2_result(**legacy_payload)
        # Legacy requires 8 and 16 messages and burst overlap, so it will fail
        self.assertFalse(legacy_res['valid'])
        self.assertIn('pilot_raw_retention_verification', legacy_res['failures'])

    def test_verify_e2_equal_samples_raw_server_side_checks(self):
        smoke = Mock()
        registry = Mock()
        spec = Mock(gateway_id='gw-a', sensor_id='s-a')

        def setup_psql_mock(ledger_override=None, overlap_override='0',
                            prov_override='0', tele_override=None):
            def fake_db(reg, pg, sql, params):
                if 'FROM processed_messages p1' in sql:
                    return str(overlap_override)
                if 'json_build_object' in sql and 'ledger_count' in sql:
                    data = {
                        'ledger_count': 30,
                        'min_first_sequence': 0,
                        'max_last_sequence': 2999,
                        'total_sample_count': 3000,
                        'invalid_sample_count': 0,
                        'invalid_sequence_bounds': 0,
                    }
                    if ledger_override:
                        data.update(ledger_override)
                    return json.dumps(data)
                if 'sample_count IS DISTINCT FROM' in sql or 'sample_count <> (SELECT count(*)' in sql:
                    return str(prov_override)
                if 'FROM telemetry t' in sql and 'unique_sequence_count' in sql:
                    data = {
                        'raw_count': 3000,
                        'unique_sequence_count': 3000,
                        'min_sequence': 0,
                        'max_sequence': 2999,
                        'value_mismatches': 0,
                        'timestamp_mismatches': 0,
                    }
                    if tele_override:
                        data.update(tele_override)
                    return json.dumps(data)
                if 'LIMIT 20' in sql:
                    return json.dumps([
                        {'message_id': f'550e8400-e29b-41d4-a716-{i:012d}',
                         'first_sequence': str(i * 100), 'last_sequence': str((i + 1) * 100 - 1),
                         'sample_count': 100}
                        for i in range(20)
                    ])
                return '0'
            smoke.db_psql.side_effect = fake_db

        # 1. Positive: exact valid check
        setup_psql_mock()
        res = verify_e2_equal_samples_raw(
            smoke, registry, 'fake-pg', spec,
            expected_samples=3000, expected_messages=30, samples_per_message=100,
            timestamp_anchor='2026-10-31T11:58:00Z', sample_interval_us=20000)
        self.assertTrue(res['verified_exact'])
        self.assertEqual(res['raw_count'], 3000)
        self.assertEqual(res['ledger_count'], 30)
        self.assertEqual(res['raw_identity_value_mismatches'], 0)
        self.assertEqual(res['timestamp_mismatches'], 0)
        self.assertEqual(res['sequence_gaps_or_overlaps'], 0)
        self.assertEqual(len(res['identity_ranges']), 20)

        # 2. Negative: synthetic value mismatch
        setup_psql_mock(tele_override={'value_mismatches': 3})
        res = verify_e2_equal_samples_raw(
            smoke, registry, 'fake-pg', spec,
            expected_samples=3000, expected_messages=30, samples_per_message=100,
            timestamp_anchor='2026-10-31T11:58:00Z', sample_interval_us=20000)
        self.assertFalse(res['verified_exact'])
        self.assertEqual(res['raw_identity_value_mismatches'], 3)

        # 3. Negative: timestamp mismatch
        setup_psql_mock(tele_override={'timestamp_mismatches': 1})
        res = verify_e2_equal_samples_raw(
            smoke, registry, 'fake-pg', spec,
            expected_samples=3000, expected_messages=30, samples_per_message=100,
            timestamp_anchor='2026-10-31T11:58:00Z', sample_interval_us=20000)
        self.assertFalse(res['verified_exact'])
        self.assertEqual(res['timestamp_mismatches'], 1)

        # 4. Negative: sequence gap/overlap in telemetry
        setup_psql_mock(tele_override={'unique_sequence_count': 2999})
        res = verify_e2_equal_samples_raw(
            smoke, registry, 'fake-pg', spec,
            expected_samples=3000, expected_messages=30, samples_per_message=100,
            timestamp_anchor='2026-10-31T11:58:00Z', sample_interval_us=20000)
        self.assertFalse(res['verified_exact'])
        self.assertEqual(res['sequence_gaps_or_overlaps'], 1)

        # 5. Negative: ledger overlap
        setup_psql_mock(overlap_override='1')
        res = verify_e2_equal_samples_raw(
            smoke, registry, 'fake-pg', spec,
            expected_samples=3000, expected_messages=30, samples_per_message=100,
            timestamp_anchor='2026-10-31T11:58:00Z', sample_interval_us=20000)
        self.assertFalse(res['verified_exact'])
        self.assertEqual(res['sequence_gaps_or_overlaps'], 1)

    def test_verify_e2_equal_samples_raw_real_db_psql_adapter_and_param_validation(self):
        """Invoke REAL native_compose_smoke.db_psql to assert string validation and argv -v values."""
        smoke = smoke_runtime
        spec = SimpleNamespace(gateway_id='gw-a', sensor_id='s-a')

        captured_commands = []
        class FakeRegistry:
            def command(self, args, input=None):
                captured_commands.append((list(args), input))
                sql = input.decode() if input else ""
                if 'FROM processed_messages p1' in sql:
                    out = b'0\n'
                elif 'json_build_object' in sql and 'ledger_count' in sql:
                    out = json.dumps({
                        'ledger_count': 30,
                        'min_first_sequence': 0,
                        'max_last_sequence': 2999,
                        'total_sample_count': 3000,
                        'invalid_sample_count': 0,
                        'invalid_sequence_bounds': 0,
                    }).encode() + b'\n'
                elif 'sample_count IS DISTINCT FROM' in sql or 'sample_count <>' in sql:
                    out = b'0\n'
                elif 'FROM telemetry t' in sql and 'unique_sequence_count' in sql:
                    out = json.dumps({
                        'raw_count': 3000,
                        'unique_sequence_count': 3000,
                        'min_sequence': 0,
                        'max_sequence': 2999,
                        'value_mismatches': 0,
                        'timestamp_mismatches': 0,
                    }).encode() + b'\n'
                elif 'LIMIT 20' in sql:
                    out = json.dumps([
                        {'message_id': f'550e8400-e29b-41d4-a716-{i:012d}',
                         'first_sequence': str(i * 100), 'last_sequence': str((i + 1) * 100 - 1),
                         'sample_count': 100}
                        for i in range(20)
                    ]).encode() + b'\n'
                else:
                    out = b'0\n'
                return SimpleNamespace(returncode=0, stdout=out, stderr=b'')

        reg = FakeRegistry()
        res = verify_e2_equal_samples_raw(
            smoke, reg, 'fake-pg', spec,
            expected_samples=3000, expected_messages=30, samples_per_message=100,
            timestamp_anchor='2026-10-31T11:58:00Z', sample_interval_us=20000)

        self.assertTrue(res['verified_exact'])
        self.assertGreater(len(captured_commands), 0)

        # Assert argv -v values across every psql invocation
        for args, _ in captured_commands:
            v_pairs = {}
            for i in range(len(args) - 1):
                if args[i] == '-v' and '=' in args[i + 1]:
                    k, v = args[i + 1].split('=', 1)
                    v_pairs[k] = v
            self.assertEqual(v_pairs.get('gw'), 'gw-a')
            self.assertEqual(v_pairs.get('sensor'), 's-a')
            self.assertEqual(v_pairs.get('expected_samples'), '3000')
            self.assertEqual(v_pairs.get('expected_messages'), '30')
            self.assertEqual(v_pairs.get('samples_per_message'), '100')
            self.assertEqual(v_pairs.get('anchor'), '2026-10-31T11:58:00Z')
            self.assertEqual(v_pairs.get('interval_us'), '20000')

        # Guard input types and count bounds before conversion - do not coerce bool or invalid strings
        for bad_samples in (True, False, '3000', 3000.0, 0, -1, 10_000_001):
            with self.assertRaises(ValueError):
                verify_e2_equal_samples_raw(
                    smoke, reg, 'fake-pg', spec,
                    expected_samples=bad_samples, expected_messages=30, samples_per_message=100,
                    timestamp_anchor='2026-10-31T11:58:00Z', sample_interval_us=20000)

        for bad_messages in (True, False, '30', 30.5, 0, -5):
            with self.assertRaises(ValueError):
                verify_e2_equal_samples_raw(
                    smoke, reg, 'fake-pg', spec,
                    expected_samples=3000, expected_messages=bad_messages, samples_per_message=100,
                    timestamp_anchor='2026-10-31T11:58:00Z', sample_interval_us=20000)

        for bad_spm in (True, False, '100', 0, -100):
            with self.assertRaises(ValueError):
                verify_e2_equal_samples_raw(
                    smoke, reg, 'fake-pg', spec,
                    expected_samples=3000, expected_messages=30, samples_per_message=bad_spm,
                    timestamp_anchor='2026-10-31T11:58:00Z', sample_interval_us=20000)

        for bad_interval in (True, False, '20000', 0, -20, (1 << 53)):
            with self.assertRaises(ValueError):
                verify_e2_equal_samples_raw(
                    smoke, reg, 'fake-pg', spec,
                    expected_samples=3000, expected_messages=30, samples_per_message=100,
                    timestamp_anchor='2026-10-31T11:58:00Z', sample_interval_us=bad_interval)

    def test_verify_e2_equal_samples_raw_numeric_parsing_fails_closed(self):
        """Non-digit or corrupt count strings must fail closed (raise ValueError), never defaulting to 0."""
        import native_compose_smoke as smoke
        spec = SimpleNamespace(gateway_id='gw-a', sensor_id='s-a')

        def make_reg(overlap_out=b'0\n', prov_out=b'0\n', ledger_out=None, tele_out=None):
            class FakeRegistry:
                def command(self, args, input=None):
                    sql = input.decode() if input else ""
                    if 'FROM processed_messages p1' in sql:
                        out = overlap_out
                    elif 'json_build_object' in sql and 'ledger_count' in sql:
                        out = ledger_out if ledger_out is not None else json.dumps({
                            'ledger_count': 30, 'min_first_sequence': 0, 'max_last_sequence': 2999,
                            'total_sample_count': 3000, 'invalid_sample_count': 0, 'invalid_sequence_bounds': 0,
                        }).encode() + b'\n'
                    elif 'sample_count IS DISTINCT FROM' in sql or 'sample_count <>' in sql:
                        out = prov_out
                    elif 'FROM telemetry t' in sql and 'unique_sequence_count' in sql:
                        out = tele_out if tele_out is not None else json.dumps({
                            'raw_count': 3000, 'unique_sequence_count': 3000, 'min_sequence': 0,
                            'max_sequence': 2999, 'value_mismatches': 0, 'timestamp_mismatches': 0,
                        }).encode() + b'\n'
                    elif 'LIMIT 20' in sql:
                        out = json.dumps([
                            {'message_id': f'550e8400-e29b-41d4-a716-{i:012d}',
                             'first_sequence': str(i * 100), 'last_sequence': str((i + 1) * 100 - 1),
                             'sample_count': 100}
                            for i in range(20)
                        ]).encode() + b'\n'
                    else:
                        out = b'0\n'
                    return SimpleNamespace(returncode=0, stdout=out, stderr=b'')
            return FakeRegistry()

        # Non-digit overlap must fail closed
        for bad_overlap in (b'', b'   \n', b'invalid\n', b'None\n', b'-1\n', b'NaN\n'):
            with self.assertRaisesRegex(ValueError, 'raw_audit_invalid_overlap_count'):
                verify_e2_equal_samples_raw(
                    smoke, make_reg(overlap_out=bad_overlap), 'fake-pg', spec,
                    expected_samples=3000, expected_messages=30, samples_per_message=100,
                    timestamp_anchor='2026-10-31T11:58:00Z', sample_interval_us=20000)

        # Non-digit provenance mismatches must fail closed
        for bad_prov in (b'', b'   \n', b'corrupted\n', b'None\n', b'-5\n'):
            with self.assertRaisesRegex(ValueError, 'raw_audit_invalid_provenance_count'):
                verify_e2_equal_samples_raw(
                    smoke, make_reg(prov_out=bad_prov), 'fake-pg', spec,
                    expected_samples=3000, expected_messages=30, samples_per_message=100,
                    timestamp_anchor='2026-10-31T11:58:00Z', sample_interval_us=20000)

        # Corrupt JSON in ledger or telemetry stats must fail closed
        with self.assertRaises(ValueError):
            verify_e2_equal_samples_raw(
                smoke, make_reg(ledger_out=b'NOT_JSON\n'), 'fake-pg', spec,
                expected_samples=3000, expected_messages=30, samples_per_message=100,
                timestamp_anchor='2026-10-31T11:58:00Z', sample_interval_us=20000)

        with self.assertRaises(ValueError):
            verify_e2_equal_samples_raw(
                smoke, make_reg(tele_out=b'NOT_JSON\n'), 'fake-pg', spec,
                expected_samples=3000, expected_messages=30, samples_per_message=100,
                timestamp_anchor='2026-10-31T11:58:00Z', sample_interval_us=20000)

    def test_verify_e2_equal_samples_raw_sql_queries_inspect_null_comparisons(self):
        """All SQL queries must use IS DISTINCT FROM or explicit IS NULL checks to prevent hiding mismatches."""
        import native_compose_smoke as smoke
        spec = SimpleNamespace(gateway_id='gw-a', sensor_id='s-a')
        executed_sqls = []

        class QueryCapturingRegistry:
            def command(self, args, input=None):
                if input:
                    executed_sqls.append(input.decode())
                if len(executed_sqls) == 1:
                    out = json.dumps({
                        'ledger_count': 30, 'min_first_sequence': 0, 'max_last_sequence': 2999,
                        'total_sample_count': 3000, 'invalid_sample_count': 0, 'invalid_sequence_bounds': 0,
                    }).encode() + b'\n'
                elif len(executed_sqls) in (2, 3):
                    out = b'0\n'
                elif len(executed_sqls) == 4:
                    out = json.dumps({
                        'raw_count': 3000, 'unique_sequence_count': 3000, 'min_sequence': 0,
                        'max_sequence': 2999, 'value_mismatches': 0, 'timestamp_mismatches': 0,
                    }).encode() + b'\n'
                else:
                    out = b'[]\n'
                return SimpleNamespace(returncode=0, stdout=out, stderr=b'')

        verify_e2_equal_samples_raw(
            smoke, QueryCapturingRegistry(), 'fake-pg', spec,
            expected_samples=3000, expected_messages=30, samples_per_message=100,
            timestamp_anchor='2026-10-31T11:58:00Z', sample_interval_us=20000)

        self.assertEqual(len(executed_sqls), 5)
        q1_ledger, q2_overlap, q3_prov, q4_tele, q5_identity = executed_sqls

        # Query 1: invalid_sample_count must use IS DISTINCT FROM, invalid_sequence_bounds must guard NULLs
        self.assertIn("p.sample_count IS DISTINCT FROM :'samples_per_message'::int", q1_ledger)
        self.assertIn('p.first_sequence IS NULL', q1_ledger)
        self.assertIn('p.last_sequence IS NULL', q1_ledger)

        # Query 3: provenance comparisons must use IS DISTINCT FROM so missing telemetry rows (NULL) are detected
        self.assertIn('p.sample_count IS DISTINCT FROM (SELECT count(*)', q3_prov)
        self.assertIn('p.first_sequence IS DISTINCT FROM (SELECT min(t.sequence_num)', q3_prov)
        self.assertIn('p.last_sequence IS DISTINCT FROM (SELECT max(t.sequence_num)', q3_prov)
        self.assertIn('p.timestamp IS DISTINCT FROM (SELECT min(t.measured_at)', q3_prov)
        self.assertIn('t.sample_index IS DISTINCT FROM (t.sequence_num - p.first_sequence)', q3_prov)
        self.assertIn('t.boot_id IS DISTINCT FROM p.boot_id', q3_prov)

        # Query 4: telemetry NULLs must not be hidden in value or timestamp filters
        self.assertIn('t.value IS NULL', q4_tele)
        self.assertIn('t.measured_at IS NULL', q4_tele)

    def test_actual_shaped_images_and_five_service_roles(self):
        """Test extraction from actual inspect dicts with capital Id across all 5 service roles."""
        images_actual_dict = {
            'backend': {'Id': 'sha256:' + '1' * 64},
            'gateway': {'Id': 'sha256:' + '2' * 64},
            'postgres': {'Id': 'sha256:' + '3' * 64},
            'mosquitto': {'Id': 'sha256:' + '4' * 64},
            'nginx': {'Id': 'sha256:' + '5' * 64},
            'supabase-auth': {'Id': 'sha256:' + '6' * 64},
            'supabase-envoy': {'Id': 'sha256:' + '7' * 64},
        }

        # validate_e2_reference requires all 5 roles and handles 'Id'
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            from scripts.tests.test_paper_p8_e2_reference import _make_sample_context, _make_sample_reference
            ctx = _make_sample_context()
            ref_file, report, _ = _make_sample_reference(td_path, context=ctx)

            # Update report with actual-shaped images dict
            rep = json.loads(ref_file.read_text())
            rep['images'] = images_actual_dict
            del rep['image_ids']
            ref_file.write_text(json.dumps(rep))

            validated = e2.validate_e2_reference(ref_file, ctx)
            self.assertEqual(validated['status'], 'PASS')
            for role in ('backend', 'gateway', 'postgres', 'mosquitto', 'nginx'):
                self.assertIn(role, validated['image_ids'])
                self.assertTrue(validated['image_ids'][role].startswith('sha256:'))

            # Missing one of the 5 roles must fail closed
            bad_images = dict(images_actual_dict)
            del bad_images['nginx']
            rep['images'] = bad_images
            bad_file = td_path / 'bad_ref.json'
            bad_file.write_text(json.dumps(rep))
            bad_file.chmod(0o600)
            with self.assertRaisesRegex(ValueError, 'e2_reference_images_invalid'):
                e2.validate_e2_reference(bad_file, ctx)

    def test_finish_remote_collection_actual_shaped_images_quota_cleanup_and_consumers(self):
        """finish_remote_collection produces finish report compatible with reference and compare consumers."""
        fixed_now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        ctx = build_e2_equal_samples_context(total_samples=6000, sample_rate=100, seed=84, now_utc=fixed_now)

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            bundle_dir, binary_file, prepared = self._create_test_equal_samples_bundle(root, ctx, profile='official')

            # Actual-shaped image inspect output with service role keys and capital Id
            actual_images = {
                'backend': {'Id': 'sha256:' + '1' * 64},
                'gateway': {'Id': 'sha256:' + '2' * 64},
                'postgres': {'Id': 'sha256:' + '3' * 64},
                'mosquitto': {'Id': 'sha256:' + '4' * 64},
                'nginx': {'Id': 'sha256:' + '5' * 64},
                'supabase-auth': {'Id': 'sha256:' + '6' * 64},
                'supabase-envoy': {'Id': 'sha256:' + '7' * 64},
            }
            prepared['images'] = actual_images
            prepared['quota'] = {'cpus': 2, 'memory_bytes': 4294967296}
            prepared['cleanup_verified'] = True
            prepared['exact_cleanup_verified'] = True

            specs = [
                SimpleNamespace(gateway_id='gw_a', sensor_id='s_a', sensor_urn='urn:ngsi-ld:Sensor:gw_a:s_a'),
                SimpleNamespace(gateway_id='gw_b', sensor_id='s_b', sensor_urn='urn:ngsi-ld:Sensor:gw_b:s_b'),
            ]
            prepared['specs'] = specs
            prepared['pg'] = 'fake-pg'
            prepared['tokens'] = {'admin': 'admin-tok'}
            prepared['before_global'] = {'ledger': 0, 'raw': 0, 'temporal': 0, 'configuration': 0}
            ingress_fields = (
                'Received', 'ReceivedBytes', 'Accepted', 'AcceptedBytes',
                'RejectedNotRunning', 'RejectedStopping', 'RejectedRetained',
                'RejectedOversized', 'RejectedInvalidTopic', 'QueueFull',
                'ByteBudgetFull', 'ShaperPassed', 'ShaperRateLimited',
                'ShaperStateCapacity', 'Processed', 'ValidationRejected',
                'PersistFailures', 'CommitUnconfirmed', 'IdentityMismatch',
                'ACKBuildFailures', 'Committed', 'DuplicateCommitted',
                'SamplesNew', 'ACKPublished', 'ACKPublishFailures'
            )
            base_ing = {f: 0 for f in ingress_fields}
            term_ing = {f: 0 for f in ingress_fields}
            term_ing.update({
                'Received': 60,
                'Accepted': 60,
                'Committed': 60,
                'ACKPublished': 60,
                'SamplesNew': 6000,
            })
            prepared['baseline'] = {'process_instance': 'p-1', 'ingress': base_ing}
            prepared['before_targets'] = [
                {'ledger': 0, 'raw': 0, 'temporal': 0, 'configuration': 0},
                {'ledger': 0, 'raw': 0, 'temporal': 0, 'configuration': 0},
            ]

            class FakeCollector:
                def __init__(self):
                    self.stopped = False
                    self.errors = []
                    self.metrics = []
                    self.resources = []
                def stop(self, _):
                    self.stopped = True
                def record_terminal(self, a, r):
                    pass

            collector = e2.EqualSamplesCompactSampler(scrape=lambda: {}, max_snapshots=8,
                byte_cap=4096, deadline_seconds=10)
            prepared['collector'] = collector
            prepared['profile'] = 'official'
            prepared['manifests'] = {}
            collector.side_sampler = SimpleNamespace(samples=[], coverage=lambda: {},
                stop=lambda **kwargs: True, _thread=None, _stop=threading.Event())

            class FakeFixture:
                artifacts = root / 'artifacts'
                private = root / 'private'
            FakeFixture.artifacts.mkdir(mode=0o700, parents=True)
            FakeFixture.private.mkdir(mode=0o700, parents=True)

            smoke = Mock()
            smoke.ROOT = str(root)
            smoke.durable_json = lambda p, d: Path(p).write_text(json.dumps(d))
            smoke.http_request.return_value = (200, {}, {
                'process_instance': 'p-1',
                'ingress': term_ing,
            })
            smoke.paper_p0_stats.return_value = {'cpu': 10}
            smoke.paper_p1_count_db.return_value = {'ledger': 60, 'raw': 6000, 'temporal': 60, 'configuration': 0}

            def fake_psql(reg, pg, sql, params=None):
                if 'FROM processed_messages p1' in sql:
                    return '0'
                if 'json_build_object' in sql and 'ledger_count' in sql:
                    return json.dumps({
                        'ledger_count': 30, 'min_first_sequence': 0, 'max_last_sequence': 2999,
                        'total_sample_count': 3000, 'invalid_sample_count': 0, 'invalid_sequence_bounds': 0,
                    })
                if 'sample_count IS DISTINCT FROM' in sql or 'sample_count <>' in sql:
                    return '0'
                if 'FROM telemetry t' in sql and 'unique_sequence_count' in sql:
                    return json.dumps({
                        'raw_count': 3000, 'unique_sequence_count': 3000, 'min_sequence': 0,
                        'max_sequence': 2999, 'value_mismatches': 0, 'timestamp_mismatches': 0,
                    })
                if 'LIMIT 20' in sql:
                    return json.dumps([{'message_id': '550e8400-e29b-41d4-a716-446655440001',
                                        'first_sequence': '0', 'last_sequence': '99', 'sample_count': 100}])
                return json.dumps({'ledger': 30, 'raw': 3000, 'temporal': 30, 'configuration': 0})
            smoke.db_psql.side_effect = fake_psql

            registry = Mock()
            registry.resolve.return_value = {'Id': 'owned-postgres'}
            container_manifest = {'postgres': {'kind': 'container', 'name': 'owned-postgres', 'receipt': {'id': 'owned-postgres'}}}
            with patch.object(e2, 'read_container_resources', return_value={}) as docker_read, \
                 patch.object(e2, '_read_diagnostic_sources', return_value={}) as diag_read:
                result = e2.finish_remote_collection(
                    smoke, FakeFixture, Mock(url='https://127.0.0.1:443'), registry, container_manifest,
                    prepared, bundle_dir, phase_id=prepared['phase_id'], paperload_binary=binary_file)
            self.assertTrue(any(call.kwargs == {'timeout': 0.5, 'read_only': True}
                                for call in docker_read.call_args_list))
            self.assertTrue(any(call.kwargs == {'timeout': 2.0, 'read_only': True}
                                for call in registry.resolve.call_args_list))

            # 1. Check extracted image IDs: must be valid strings, not None
            self.assertIsNotNone(result.get('image_ids'))
            for svc in ('backend', 'gateway', 'postgres', 'mosquitto', 'nginx'):
                self.assertIn(svc, result['image_ids'])
                val = result['image_ids'][svc]
                self.assertIsInstance(val, str)
                self.assertTrue(val.startswith('sha256:'))

            # 2. Check quota and cleanup keys
            self.assertEqual(result.get('quota'), {'cpus': 2, 'memory_bytes': 4294967296})
            self.assertTrue(result.get('cleanup_verified'))
            self.assertTrue(result.get('exact_cleanup_verified'))

            # 3. Reference consumer accepts report
            report_path = FakeFixture.artifacts / f'p8-e2-{prepared["phase_id"]}-remote-result.json'
            self.assertTrue(report_path.is_file())
            report_path.chmod(0o600)
            art_bin = FakeFixture.artifacts / 'bin'
            art_bin.mkdir(mode=0o700, parents=True, exist_ok=True)
            shutil.copy2(binary_file, art_bin / 'paperload')
            (art_bin / 'paperload').chmod(0o700)
            ref_val = e2.validate_e2_reference(report_path, ctx)
            self.assertEqual(ref_val['status'], 'PASS')

            # 4. Compare consumer accepts reports
            rep_1 = json.loads(report_path.read_text())
            rep_100 = copy.deepcopy(rep_1)
            rep_1['samples_per_message'] = 1
            rep_100['samples_per_message'] = 100
            comp_res = e2.compare_e2_reports(rep_1, rep_100)
            self.assertEqual(comp_res['status'], 'COMPARABLE_CLEAN')
            self.assertTrue(comp_res['comparable'])
            self.assertEqual(comp_res['mismatch_reasons'], [])

    def test_load_e2_result_bundle_relocated_manifest_hashes_differ(self):
        fixed_now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        ctx = build_e2_equal_samples_context(total_samples=6000, sample_rate=100, seed=84, now_utc=fixed_now)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            # Create manifest files on host A with source bytes
            a_source_manifest = root / 'A.source.manifest.json'
            b_source_manifest = root / 'B.source.manifest.json'
            a_source_manifest.write_text(json.dumps({'experiment': 'E2', 'arm': 'A', 'source_host': 'host_a_path'}))
            b_source_manifest.write_text(json.dumps({'experiment': 'E2', 'arm': 'B', 'source_host': 'host_a_path'}))
            a_source_manifest.chmod(0o600); b_source_manifest.chmod(0o600)
            a_source_hash = hashlib.sha256(a_source_manifest.read_bytes()).hexdigest()
            b_source_hash = hashlib.sha256(b_source_manifest.read_bytes()).hexdigest()

            # Bound manifests on host B have DIFFERENT hashes because bundle relocation rewrote paths
            a_bound_hash = 'a' * 64
            b_bound_hash = 'b' * 64
            self.assertNotEqual(a_source_hash, a_bound_hash)
            self.assertNotEqual(b_source_hash, b_bound_hash)

            # Create test bundle on host B where bound_manifest_sha256 has the bound hashes
            b_dir, b_bin, prep = self._create_test_equal_samples_bundle(
                root / 'relocated_run', ctx,
                summary_mod=lambda s: s.update({
                    'bound_manifest_sha256': {'A': a_bound_hash, 'B': b_bound_hash},
                    'source_inventory': [
                        {'path': 'manifests/A.json', 'sha256': a_source_hash},
                        {'path': 'manifests/B.json', 'sha256': b_source_hash},
                    ],
                }))

            # On host A, prepared has manifest_paths pointing to A's source manifests
            prep['manifest_paths'] = {'A': a_source_manifest, 'B': b_source_manifest}
            prep['bundle_inventory'] = [
                {'path': 'manifests/A.json', 'sha256': a_source_hash},
                {'path': 'manifests/B.json', 'sha256': b_source_hash},
            ]
            # prepared must NOT require direct byte equality with bound manifests
            prep.pop('bound_manifest_sha256', None)

            # Loading the result bundle on host A MUST succeed
            imported = e2._load_e2_result_bundle(b_dir, prep, b_bin)
            self.assertIn('A', imported)
            self.assertIn('B', imported)

    def test_load_e2_result_bundle_real_exported_portable_bundle_sha_provenance(self):
        """Newly exported real portable bundle: source != portable != bound SHA, loader authenticates bundle_root."""
        fixed_now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        ctx = build_e2_equal_samples_context(total_samples=6000, sample_rate=100, seed=84, now_utc=fixed_now)

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            ca_root = root / 'ca'
            ca_root.mkdir(mode=0o700)
            https_ca = ca_root / 'https.pem'; https_ca.write_text('fake https ca'); https_ca.chmod(0o600)
            mqtt_ca = ca_root / 'mqtt.pem'; mqtt_ca.write_text('fake mqtt ca'); mqtt_ca.chmod(0o600)

            gw_dir = root / 'gateways'
            gw_dir.mkdir(mode=0o700)
            gw_a = gw_dir / 'gw_a.json'; gw_a.write_text(json.dumps({'username': 'gw_a_user', 'password': 'p'})); gw_a.chmod(0o600)
            gw_b = gw_dir / 'gw_b.json'; gw_b.write_text(json.dumps({'username': 'gw_b_user', 'password': 'p'})); gw_b.chmod(0o600)

            id_dir = root / 'identities'
            id_dir.mkdir(mode=0o700)
            id_a = id_dir / 'telemetry-A.json'; id_a.write_text('{}'); id_a.chmod(0o600)
            id_b = id_dir / 'telemetry-B.json'; id_b.write_text('{}'); id_b.chmod(0o600)

            gateway_files = {'gw_a': str(gw_a), 'gw_b': str(gw_b)}
            telemetry_files = {'A': str(id_a), 'B': str(id_b)}
            targets = [
                {'gateway_id': 'gw_a', 'sensor_id': 's1'},
                {'gateway_id': 'gw_b', 'sensor_id': 's2'},
            ]
            common = {
                'mode': 'remote',
                'bundle_path': str(root), 'publish_timeout_ms': 3000,
                'tls': {'https_endpoint': 'https://localhost:8443', 'https_ca_file': str(https_ca),
                        'mqtt_tls_endpoint': 'mqtts://localhost:8883',
                        'mqtt_tls_ca_file': str(mqtt_ca), 'mqtt_tls_server_name': 'localhost'}
            }

            manifests = e2.e2_equal_samples_manifests(
                context=ctx, common=common, targets=targets,
                gateway_identity_files=gateway_files, telemetry_identity_files=telemetry_files,
                now_utc=fixed_now, check_freshness=False
            )
            pair = manifests['profiles']['100']

            manifest_paths = {}
            for arm in ('A', 'B'):
                path = root / f'{arm}.manifest.json'
                path.write_text(json.dumps(pair[arm], sort_keys=True, separators=(',', ':')))
                path.chmod(0o600)
                manifest_paths[arm] = path

            plan_root = root / 'plan'
            plan = e2.build_e2_execution_plan(output_dir=plan_root, manifest_a=manifest_paths['A'], manifest_b=manifest_paths['B'], seed=ctx['seed'])

            bundle_root = root / 'client-bundle'
            prepared = {
                'session_id': 's123', 'run_id': pair['A']['run_id'], 'arm': 'E2-pilot',
                'samples_per_message': 100, 'profile': 'pilot', 'workload': 'equal_samples',
                'root': root, 'bundle_root': bundle_root, 'plan': plan, 'manifest_paths': manifest_paths,
                'manifests': pair, 'gateway_files': gateway_files, 'telemetry_files': telemetry_files,
                'https_ca_file': https_ca, 'mqtt_ca_file': mqtt_ca, 'phase_id': 'equal-samples-100',
                'seed': ctx['seed'], 'context': ctx, 'duration_seconds': 60, 'drain_seconds': 20,
                'grace_seconds': 10, 'wall_seconds': 90, 'expected_counts': manifests['expected_counts']['100'],
            }

            e2.export_prepared_bundle(prepared)

            # Assert SHA differences between source, portable, and bound
            source_sha_a = hashlib.sha256(manifest_paths['A'].read_bytes()).hexdigest()
            portable_sha_a = hashlib.sha256((bundle_root / 'manifests/A.json').read_bytes()).hexdigest()

            workspace = root / 'workspace'
            paper_p8_bundle.bind_bundle(bundle_root=bundle_root, workspace_leaf=workspace)
            bound_sha_a = hashlib.sha256((workspace / 'runtime/manifests/A.json').read_bytes()).hexdigest()
            bound_sha_b = hashlib.sha256((workspace / 'runtime/manifests/B.json').read_bytes()).hexdigest()

            self.assertNotEqual(source_sha_a, portable_sha_a)
            self.assertNotEqual(portable_sha_a, bound_sha_a)
            self.assertNotEqual(source_sha_a, bound_sha_a)

            bundle_meta = json.loads((bundle_root / 'bundle.json').read_text())
            prepared['bundle_inventory'] = [{'path': r['path'], 'sha256': r['sha256']} for r in bundle_meta['inputs']]
            prepared['bundle_sha256'] = hashlib.sha256((bundle_root / 'bundle.json').read_bytes()).hexdigest()

            bin_file = root / 'paperload_bin'
            bin_file.write_bytes(b'paperload-bin-exec')
            bin_file.chmod(0o700)
            bin_sha = hashlib.sha256(b'paperload-bin-exec').hexdigest()

            results_root = root / 'results_export'
            results_root.mkdir(mode=0o700)
            (results_root / 'results/workloads/A').mkdir(parents=True, mode=0o700)
            (results_root / 'results/workloads/B').mkdir(parents=True, mode=0o700)
            (results_root / 'results/runner').mkdir(parents=True, mode=0o700)

            def make_workload(arm):
                return {
                    'status': 'COMPLETE', 'mode': 'remote', 'experiment': 'E2', 'arm': arm,
                    'profile': 'pilot', 'run_id': pair['A']['run_id'], 'seed': ctx['seed'],
                    'telemetry': {'scheduled_opportunities': 30, 'generated_unique': 30, 'offered_unique': 30, 'application_acks': 30},
                    'equal_samples': {
                        'workload_type': 'equal_samples', 'logical_producer': arm,
                        'context_sha256': ctx['context_sha256'], 'samples_per_message': 100,
                        'total_samples': 6000, 'sample_rate': 100, 'duration_seconds': 60,
                        'producer_sample_rate': 50, 'sample_interval_us': 20000,
                        'expected_samples': 3000, 'expected_messages': 30,
                        'generated_samples': 3000, 'generated_messages': 30,
                        'offered_unique_samples': 3000, 'offered_unique_messages': 30,
                        'application_acks': 30, 'attempted_bytes': 30000, 'offered_unique_bytes': 30000,
                        'completed_on_time': True, 'schedule_misses': 0, 'scheduling_lag_max_ns': 1000,
                        'sampling_qualification': 'logical_offered_stream; not physical sensor acquisition latency; commit ack from application outbox only'
                    }
                }

            workload_a = make_workload('A')
            workload_b = make_workload('B')
            prov_json = {'binary_sha256': bin_sha, 'binary_os': sys.platform, 'binary_arch': platform.machine()}
            run_json = {'status': 'COMPLETE', 'phase_observations': {'status': 'observed', 'events': []}}

            files_data = {
                'results/workloads/A/workload.json': json.dumps(workload_a).encode(),
                'results/workloads/B/workload.json': json.dumps(workload_b).encode(),
                'results/runner/run.json': json.dumps(run_json).encode(),
                'results/runner/provenance.json': json.dumps(prov_json).encode(),
            }
            inventory = []
            for rel_path, data in files_data.items():
                p = results_root / rel_path
                p.write_bytes(data)
                p.chmod(0o600)
                inventory.append({'path': rel_path, 'size_bytes': len(data), 'sha256': hashlib.sha256(data).hexdigest()})

            results_summary = {
                'session': {'session_id': 's123', 'run_ids': [pair['A']['run_id']], 'experiment': 'E2', 'arm': 'E2-pilot', 'phase': 'equal-samples-100'},
                'source_bundle_sha256': prepared['bundle_sha256'],
                'source_inventory': prepared['bundle_inventory'],
                'bound_manifest_sha256': {'A': bound_sha_a, 'B': bound_sha_b},
                'bound_gateway_environment_sha256': {},
                'bound_run_spec_sha256': hashlib.sha256((workspace / 'runtime/execution/run-spec.json').read_bytes()).hexdigest(),
                'binary': prov_json,
                'files': inventory,
                'runner_status': 'COMPLETE',
                'expected_workload_roles': ['A', 'B'],
                'present_workload_roles': ['A', 'B'],
                'missing_workload_roles': [],
                'full_p8_accepted': False,
                'physical_ab': 'NOT RUN'
            }
            summary_file = results_root / 'results.json'
            summary_file.write_text(json.dumps(results_summary))
            summary_file.chmod(0o600)

            # Positive authentication
            imported = e2._load_e2_result_bundle(results_root, prepared, bin_file)
            self.assertIn('A', imported)
            self.assertIn('B', imported)
            self.assertIn('runner', imported)

            # Negatives:
            # 1. Missing portable manifest rejects
            manifest_a = bundle_root / 'manifests/A.json'
            manifest_a.unlink()
            with self.assertRaisesRegex(ValueError, 'e2_source_manifest_inventory_mismatch'):
                e2._load_e2_result_bundle(results_root, prepared, bin_file)

            # 2. Corrupted portable manifest hash mismatch rejects
            manifest_a.write_text('corrupted')
            manifest_a.chmod(0o600)
            with self.assertRaisesRegex(ValueError, 'e2_source_manifest_inventory_mismatch'):
                e2._load_e2_result_bundle(results_root, prepared, bin_file)

            # 3. Symlink portable manifest rejects
            manifest_a.unlink()
            target_f = root / 'target_manifest.json'; target_f.write_text('target'); target_f.chmod(0o600)
            manifest_a.symlink_to(target_f)
            with self.assertRaisesRegex(ValueError, 'e2_source_manifest_inventory_mismatch'):
                e2._load_e2_result_bundle(results_root, prepared, bin_file)

    def test_compare_e2_reports_clean_git_tree_source_fingerprint(self):
        """When repo is clean, dirty_source_sha256 is None; reports must compare without field missing error."""
        from scripts.tests.test_paper_p8_e2_compare import _make_report
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            clean_fp = {'revision': 'a' * 40, 'branch': 'feat/x', 'dirty': False, 'dirty_source_sha256': None}
            _, r1 = _make_report(td_path, samples_per_message=1, source_fingerprint=clean_fp)
            _, r100 = _make_report(td_path, samples_per_message=100, source_fingerprint=clean_fp)

            res = e2.compare_e2_reports(r1, r100)
            self.assertEqual(res['status'], 'COMPARABLE_CLEAN')
            self.assertTrue(res['comparable'])
            self.assertNotIn('missing_required_source_fingerprint_field:dirty_source_sha256', res['mismatch_reasons'])

    def test_finish_remote_collection_binary_authentication_and_sha256(self):
        """finish_remote_collection requires valid binary path, calculates SHA256 once, and rejects invalid binaries."""
        fixed_now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        ctx = build_e2_equal_samples_context(total_samples=6000, sample_rate=100, seed=84, now_utc=fixed_now)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            bundle_dir, binary_file, prepared = self._create_test_equal_samples_bundle(root, ctx)

            class FakeCollector:
                def __init__(self):
                    self.stopped = False
                    self.errors = []
                    self.metrics = []
                    self.resources = []
                def stop(self, error):
                    self.stopped = True
                def record_terminal(self, admin, res):
                    pass

            collector = FakeCollector()
            prepared['collector'] = collector
            prepared['tokens'] = {'admin': 'fake-token'}
            prepared['specs'] = [
                Mock(gateway_id='gw-a', sensor_id='s-a', sensor_urn='urn:ngsi-ld:Sensor:gw-a:s-a'),
                Mock(gateway_id='gw-b', sensor_id='s-b', sensor_urn='urn:ngsi-ld:Sensor:gw-b:s-b'),
            ]
            prepared['before_global'] = {'ledger': 0, 'raw': 0, 'temporal': 0, 'configuration': 0}
            prepared['before_targets'] = [{'configuration': 0}, {'configuration': 0}]
            prepared['baseline'] = {'process_instance': 'proc-1', 'ingress': {'Received': 0, 'Committed': 0, 'Accepted': 0}}
            prepared['pg'] = 'fake-pg'

            class FakeFixture:
                artifacts = root / 'artifacts'
            FakeFixture.artifacts.mkdir(mode=0o700, exist_ok=True)

            smoke = Mock()
            smoke.ROOT = str(root)
            smoke.db_psql.return_value = '0'
            smoke.http_request.return_value = (200, {}, {
                'process_instance': 'proc-1',
                'ingress': {'Received': 60, 'Committed': 60, 'Accepted': 60},
                'persistence': {'copy_calls': 30, 'rows': 6000},
            })
            smoke.paper_p1_count_db.return_value = {'ledger': 30, 'raw': 3000, 'temporal': 30, 'configuration': 0}

            def fake_psql(reg, pg, sql, params=None):
                if 'FROM processed_messages p1' in sql:
                    return '0'
                if 'json_build_object' in sql and 'ledger_count' in sql:
                    return json.dumps({
                        'ledger_count': 30, 'min_first_sequence': 0, 'max_last_sequence': 2999,
                        'total_sample_count': 3000, 'invalid_sample_count': 0, 'invalid_sequence_bounds': 0,
                    })
                if 'sample_count IS DISTINCT FROM' in sql or 'sample_count <>' in sql:
                    return '0'
                if 'FROM telemetry t' in sql and 'unique_sequence_count' in sql:
                    return json.dumps({
                        'raw_count': 3000, 'unique_sequence_count': 3000, 'min_sequence': 0,
                        'max_sequence': 2999, 'value_mismatches': 0, 'timestamp_mismatches': 0,
                    })
                if 'LIMIT 20' in sql:
                    return json.dumps([{'message_id': '550e8400-e29b-41d4-a716-446655440001', 'first_sequence': '0', 'last_sequence': '99', 'sample_count': 100}])
                return json.dumps({'ledger': 30, 'raw': 3000, 'temporal': 30, 'configuration': 0})
            smoke.db_psql.side_effect = fake_psql

            # 1. Missing binary file rejected
            non_existent = root / 'non_existent_bin'
            with self.assertRaisesRegex(ValueError, 'e2_binary_file_rejected'):
                e2.finish_remote_collection(
                    smoke, FakeFixture, Mock(url='https://127.0.0.1:443'), Mock(), {},
                    prepared, bundle_dir, phase_id=prepared['phase_id'], paperload_binary=non_existent)

            # 2. Symlink binary rejected
            symlink_bin = root / 'symlink_bin'
            symlink_bin.symlink_to(binary_file)
            with self.assertRaisesRegex(ValueError, 'e2_binary_file_rejected'):
                e2.finish_remote_collection(
                    smoke, FakeFixture, Mock(url='https://127.0.0.1:443'), Mock(), {},
                    prepared, bundle_dir, phase_id=prepared['phase_id'], paperload_binary=symlink_bin)

            # 3. Valid executable file succeeds and hashes match
            expected_hash = hashlib.sha256(binary_file.read_bytes()).hexdigest()
            result = e2.finish_remote_collection(
                smoke, FakeFixture, Mock(url='https://127.0.0.1:443'), Mock(), {},
                prepared, bundle_dir, phase_id=prepared['phase_id'], paperload_binary=binary_file)
            self.assertEqual(result['binary_sha256'], expected_hash)
            self.assertEqual(result['paperload_binary_sha256'], expected_hash)

    def test_load_e2_result_bundle_strict_safe_equal_samples_and_seed_check(self):
        fixed_now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        ctx = build_e2_equal_samples_context(total_samples=6000, sample_rate=100, seed=84, now_utc=fixed_now)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)

            # 1. Tampered sampling qualification rejected by strict _safe_equal_samples
            b_dir, b_bin, prep = self._create_test_equal_samples_bundle(
                root / 'strict1', ctx,
                a_mod=lambda a: a['equal_samples'].update({'sampling_qualification': 'fake_qualification'}))
            with self.assertRaises(ValueError):
                e2._load_e2_result_bundle(b_dir, prep, b_bin)

            # 2. Negative counter rejected by strict _safe_equal_samples
            b_dir, b_bin, prep = self._create_test_equal_samples_bundle(
                root / 'strict2', ctx,
                a_mod=lambda a: a['equal_samples'].update({'schedule_misses': -1}))
            with self.assertRaises(ValueError):
                e2._load_e2_result_bundle(b_dir, prep, b_bin)

            # 3. Seed mismatch in workload rejected
            b_dir, b_bin, prep = self._create_test_equal_samples_bundle(
                root / 'strict3', ctx,
                a_mod=lambda a: a.update({'seed': 99999}))
            with self.assertRaisesRegex(ValueError, 'e2_seed_mismatch'):
                e2._load_e2_result_bundle(b_dir, prep, b_bin)

    def test_finish_remote_collection_equal_samples_artifact_and_dispatch(self):
        class MockCollector:
            def __init__(self):
                self.stopped = False
                self.metrics = []
                self.resources = []
                self.errors = []
            def stop(self, pid):
                self.stopped = True

        fixed_now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        ctx = build_e2_equal_samples_context(total_samples=6000, sample_rate=100, seed=84, now_utc=fixed_now)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            bundle_dir, binary_file, prepared = self._create_test_equal_samples_bundle(root, ctx)

            collector = MockCollector()
            prepared['collector'] = collector

            class FakeSpec:
                def __init__(self, gw, s):
                    self.gateway_id = gw
                    self.sensor_id = s
                    self.sensor_urn = f'urn:ngsi-ld:Sensor:{gw}:{s}'

            specs = [FakeSpec('gw-a', 's-a'), FakeSpec('gw-b', 's-b')]
            prepared['specs'] = specs
            prepared['pg'] = 'fake-pg'
            prepared['tokens'] = {'admin': 'admin-tok'}
            prepared['before_global'] = {'ledger': 0, 'raw': 0, 'temporal': 0, 'configuration': 0}
            ingress_fields = (
                'Received', 'ReceivedBytes', 'Accepted', 'AcceptedBytes',
                'RejectedNotRunning', 'RejectedStopping', 'RejectedRetained',
                'RejectedOversized', 'RejectedInvalidTopic', 'QueueFull',
                'ByteBudgetFull', 'ShaperPassed', 'ShaperRateLimited',
                'ShaperStateCapacity', 'Processed', 'ValidationRejected',
                'PersistFailures', 'CommitUnconfirmed', 'IdentityMismatch',
                'ACKBuildFailures', 'Committed', 'DuplicateCommitted',
                'SamplesNew', 'ACKPublished', 'ACKPublishFailures'
            )
            base_ing = {f: 0 for f in ingress_fields}
            term_ing = {f: 0 for f in ingress_fields}
            term_ing.update({
                'Received': 60,
                'Accepted': 60,
                'Committed': 60,
                'ACKPublished': 60,
                'SamplesNew': 6000,
            })
            prepared['baseline'] = {'process_instance': 'p-1', 'ingress': base_ing}
            prepared['before_targets'] = [{'ledger': 0, 'raw': 0, 'temporal': 0, 'configuration': 0},
                                          {'ledger': 0, 'raw': 0, 'temporal': 0, 'configuration': 0}]
            class FakeFixture:
                artifacts = root / 'artifacts'
            FakeFixture.artifacts.mkdir(mode=0o700, parents=True)

            smoke = Mock()
            smoke.http_request.return_value = (200, {}, {
                'process_instance': 'p-1',
                'ingress': term_ing,
            })
            smoke.paper_p0_stats.return_value = {'cpu': 10}
            smoke.paper_p1_count_db.return_value = {'ledger': 60, 'raw': 6000, 'temporal': 60, 'configuration': 0}
            def fake_psql(reg, pg, sql, params):
                if 'FROM processed_messages p1' in sql:
                    return '0'
                if 'json_build_object' in sql and 'ledger_count' in sql:
                    return json.dumps({
                        'ledger_count': 30, 'min_first_sequence': 0, 'max_last_sequence': 2999,
                        'total_sample_count': 3000, 'invalid_sample_count': 0, 'invalid_sequence_bounds': 0,
                    })
                if 'sample_count IS DISTINCT FROM' in sql or 'sample_count <> (SELECT count(*)' in sql:
                    return '0'
                if 'FROM telemetry t' in sql and 'unique_sequence_count' in sql:
                    return json.dumps({
                        'raw_count': 3000, 'unique_sequence_count': 3000, 'min_sequence': 0,
                        'max_sequence': 2999, 'value_mismatches': 0, 'timestamp_mismatches': 0,
                    })
                if 'LIMIT 20' in sql:
                    return json.dumps([{'message_id': '550e8400-e29b-41d4-a716-446655440001',
                                        'first_sequence': '0', 'last_sequence': '99', 'sample_count': 100}])
                return json.dumps({'ledger': 30, 'raw': 3000, 'temporal': 30, 'configuration': 0})
            smoke.db_psql.side_effect = fake_psql

            result = e2.finish_remote_collection(
                smoke, FakeFixture, Mock(url='https://127.0.0.1:443'), Mock(), {},
                prepared, bundle_dir, phase_id=prepared['phase_id'], paperload_binary=binary_file)

            self.assertTrue(collector.stopped)
            self.assertEqual(result['phase_id'], 'equal-samples-100')
            self.assertEqual(result['workload'], 'equal_samples')
            self.assertEqual(result['status'], 'PASS')
            self.assertTrue(result['clean_comparison'])
            self.assertEqual(result['contamination_reasons'], [])
            self.assertEqual(result['generator_colocated'], 'unverified')
            self.assertEqual(result['physical_ab'], 'NOT RUN')
            self.assertIn('context', result)
            self.assertIn('expected_counts', result)
            self.assertIn('actual_counters', result)
            self.assertIn('binary_provenance', result)
            # Verify artifact written with equal-samples name
            artifact_file = FakeFixture.artifacts / 'p8-e2-equal-samples-100-remote-result.json'
            smoke.durable_json.assert_called_with(artifact_file, result)

    def test_e2_compact_sampler_delayed_start_beyond_64_snapshots(self):
        """Sampler must continue sampling past 64 snapshots up to deadline/cap without stopping."""
        clock = {'mono': 100.0, 'wall': datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)}
        def fake_mono():
            return clock['mono']
        def fake_wall():
            return clock['wall'].isoformat()
        def fake_scrape():
            return {
                'process_instance': 'proc-1',
                'ingress': {'Received': 0, 'Committed': 0, 'Accepted': 0},
                'persistence': {'copy_calls': 0},
            }

        sampler = e2.EqualSamplesCompactSampler(
            scrape=fake_scrape,
            interval=1.0,
            max_snapshots=3600,
            deadline_seconds=1800,
            time_fn=fake_mono,
            wall_fn=fake_wall,
        )
        sampler.start_manual()
        for i in range(70):
            clock['mono'] += 1.0
            clock['wall'] += timedelta(seconds=1)
            sampler.sample_step(phase=f'sample_{i}')

        coverage = sampler.get_coverage()
        self.assertEqual(coverage['snapshots_captured'], 71)  # initial + 70 steps
        self.assertEqual(coverage['snapshots_retained'], 71)
        self.assertEqual(coverage['snapshots_evicted'], 0)
        self.assertEqual(sampler.errors, [])
        self.assertGreater(len(sampler.retained_snapshots), 64)

    def test_e2_compact_sampler_retains_real_db_counters_and_process_windows(self):
        counters = {'begin_attempts': 4, 'begin_succeeded': 3, 'confirmed_commits': 2,
                    'copy_calls': 1, 'copy_returned_rows': 100, 'confirmed_new_rows': 100}
        baseline = {'process_instance': 'proc-a', 'monotonic': 10,
                    'persistence': counters, 'resources': {'backend': {
                        'cpu_cumulative_seconds': 2.0, 'memory_bytes': 100}}}
        sampler = e2.EqualSamplesCompactSampler(scrape=lambda: {
            'process_instance': 'proc-a', 'persistence': counters}, baseline_metrics=baseline,
            baseline_resources=baseline['resources'])
        sampler.start_manual()
        counters.update(begin_attempts=9, begin_succeeded=8, confirmed_commits=7,
                        copy_calls=3, copy_returned_rows=500, confirmed_new_rows=500)
        sampler.sample_step()
        sampler.record_terminal({'process_instance': 'proc-a', 'monotonic': 12,
                                 'persistence': counters}, {'backend': {
                                     'cpu_cumulative_seconds': 5.0, 'memory_bytes': 200}})

        self.assertEqual(sampler.baseline_metrics['persistence']['begin_attempts'], 4)
        self.assertEqual(sampler.terminal_metrics['persistence']['confirmed_commits'], 7)
        self.assertEqual(sampler.terminal_metrics['persistence']['copy_calls'], 3)
        self.assertEqual(sampler.baseline_metrics['process_instance'], 'proc-a')
        self.assertEqual(sampler.terminal_metrics['process_instance'], 'proc-a')
        self.assertEqual(sampler.baseline_resources['backend']['cpu_cumulative_seconds'], 2.0)
        self.assertEqual(sampler.terminal_resources['backend']['cpu_cumulative_seconds'], 5.0)

    def test_e2_terminal_process_reset_and_missing_resources_stay_unavailable(self):
        sampler = e2.EqualSamplesCompactSampler(scrape=lambda: {
            'process_instance': 'proc-a', 'persistence': {'confirmed_commits': 1}},
            baseline_metrics={'process_instance': 'proc-a', 'persistence': {'confirmed_commits': 0}},
            baseline_resources={'backend': {'cpu_cumulative_seconds': 1.0, 'available': True}})
        sampler.start_manual()
        sampler.record_terminal({'process_instance': 'proc-b', 'persistence': {'confirmed_commits': 2}}, None)
        self.assertEqual(sampler.terminal_metrics['process_instance'], 'proc-b')
        self.assertIsNone(sampler.terminal_resources)
        self.assertIsNone(sampler.terminal_metrics.get('resources'))

    def test_e2_db_counter_window_uses_real_counters_and_refuses_reset_or_process_change(self):
        before = {'process_instance': 'p1', 'persistence': {
            'begin_attempts': 2, 'begin_succeeded': 2, 'confirmed_commits': 1,
            'copy_calls': 0, 'copy_returned_rows': 0, 'confirmed_new_rows': 0,
            'fallback_attempts': 0, 'rollback_attempts': 0, 'confirmed_rollbacks': 0,
            'commit_unconfirmed': 0}}
        after = {'process_instance': 'p1', 'persistence': {
            'begin_attempts': 5, 'begin_succeeded': 4, 'confirmed_commits': 3,
            'copy_calls': 1, 'copy_returned_rows': 200, 'confirmed_new_rows': 200,
            'fallback_attempts': 1, 'rollback_attempts': 1, 'confirmed_rollbacks': 1,
            'commit_unconfirmed': 0}}
        result = e2.e2_database_counter_window(before, after)
        self.assertTrue(result['available'])
        self.assertEqual(result['deltas']['begin_attempts'], 3)
        self.assertEqual(result['deltas']['confirmed_commits'], 2)
        self.assertEqual(result['deltas']['copy_calls'], 1)
        self.assertEqual(result['deltas']['copy_returned_rows'], 200)
        self.assertFalse(e2.e2_database_counter_window(
            before, dict(after, process_instance='p2'))['available'])
        reset = dict(after, persistence=dict(after['persistence'], confirmed_commits=0))
        self.assertEqual(e2.e2_database_counter_window(before, reset)['reason'],
                         'db_counter_missing_or_reset:confirmed_commits')
        missing = dict(after, persistence=dict(after['persistence']))
        del missing['persistence']['begin_attempts']
        self.assertFalse(e2.e2_database_counter_window(before, missing)['available'])

    def test_e2_compact_sampler_ring_eviction_preserves_baseline_and_terminal(self):
        """Ring buffer eviction drops oldest rolling samples but keeps baseline/terminal outside ring."""
        clock = {'mono': 100.0, 'wall': datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)}
        baseline_m = {'process_instance': 'proc-1', 'ingress': {'Committed': 0}}
        baseline_r = {'backend': {'cpu_cumulative_seconds': 1.0, 'memory_bytes': 1000}}
        terminal_m = {'process_instance': 'proc-1', 'ingress': {'Committed': 60}}
        terminal_r = {'backend': {'cpu_cumulative_seconds': 5.0, 'memory_bytes': 1500}}

        sampler = e2.EqualSamplesCompactSampler(
            scrape=lambda: {'process_instance': 'proc-1', 'ingress': {'Committed': 10}},
            interval=1.0,
            max_snapshots=5,
            deadline_seconds=1800,
            baseline_metrics=baseline_m,
            baseline_resources=baseline_r,
            time_fn=lambda: clock['mono'],
            wall_fn=lambda: clock['wall'].isoformat(),
        )
        sampler.start_manual()
        for i in range(10):
            clock['mono'] += 1.0
            clock['wall'] += timedelta(seconds=1)
            sampler.sample_step(phase=f'sample_{i}')
        sampler.record_terminal(terminal_m, terminal_r)

        coverage = sampler.get_coverage()
        self.assertEqual(coverage['snapshots_retained'], 5)
        self.assertEqual(coverage['snapshots_evicted'], 6)  # 1 initial + 10 steps = 11 total; 11 - 5 = 6 evicted
        # Baseline and terminal must be strictly preserved outside the ring
        self.assertEqual(sampler.baseline_metrics['ingress']['Committed'], 0)
        self.assertEqual(sampler.baseline_resources['backend']['cpu_cumulative_seconds'], 1.0)
        self.assertEqual(sampler.terminal_metrics['ingress']['Committed'], 60)
        self.assertEqual(sampler.terminal_resources['backend']['cpu_cumulative_seconds'], 5.0)

    def test_e2_compact_sampler_explicit_byte_cap_eviction(self):
        """Byte cap eviction evicts older snapshots when serialized ring size exceeds byte cap."""
        clock = {'mono': 100.0, 'wall': datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)}
        sampler = e2.EqualSamplesCompactSampler(
            scrape=lambda: {'process_instance': 'proc-1', 'ingress': {'Committed': 10, 'Received': 10}},
            interval=1.0,
            max_snapshots=100,
            byte_cap=1200,  # small byte cap
            deadline_seconds=1800,
            time_fn=lambda: clock['mono'],
            wall_fn=lambda: clock['wall'].isoformat(),
        )
        sampler.start_manual()
        for i in range(15):
            clock['mono'] += 1.0
            clock['wall'] += timedelta(seconds=1)
            sampler.sample_step(phase=f'sample_{i}')

        coverage = sampler.get_coverage()
        self.assertGreater(coverage['snapshots_evicted'], 0)
        self.assertGreater(coverage['evicted_bytes'], 0)
        self.assertLessEqual(coverage['ring_bytes_used'], 1200)

    def test_e2_active_window_counter_reset_inconclusive(self):
        """Active window inference rejects counter decrease as counter_reset -> INCONCLUSIVE."""
        baseline = {'timestamp': 't0', 'monotonic': 100.0, 'process_instance': 'p1',
                    'ingress': {'Committed': 0, 'Received': 0, 'SamplesNew': 0}}
        s1 = {'timestamp': 't1', 'monotonic': 101.0, 'process_instance': 'p1',
              'ingress': {'Committed': 10, 'Received': 10, 'SamplesNew': 1000}}
        s2 = {'timestamp': 't2', 'monotonic': 102.0, 'process_instance': 'p1',
              'ingress': {'Committed': 5, 'Received': 10, 'SamplesNew': 1000}}  # Reset!
        terminal = {'timestamp': 't3', 'monotonic': 103.0, 'process_instance': 'p1',
                    'ingress': {'Committed': 60, 'Received': 60, 'SamplesNew': 6000}}

        res = e2.infer_host_a_active_window(
            snapshots=[s1, s2],
            baseline=baseline,
            terminal=terminal,
            expected_total_messages=60,
            expected_total_samples=6000,
            sampling_interval=1.0,
            evicted_count=0,
        )
        self.assertEqual(res['status'], 'INCONCLUSIVE')
        self.assertEqual(res['reason'], 'counter_reset')

    def test_e2_active_window_container_identity_change_inconclusive(self):
        """Active window inference rejects container restart / ID change as container_identity_changed."""
        baseline = {'timestamp': 't0', 'monotonic': 100.0, 'process_instance': 'p1',
                    'ingress': {'Committed': 0, 'Received': 0, 'SamplesNew': 0},
                    'resources': {'backend': {'container_id': 'cid-1'}}}
        s1 = {'timestamp': 't1', 'monotonic': 101.0, 'process_instance': 'p1',
              'ingress': {'Committed': 30, 'Received': 30, 'SamplesNew': 3000},
              'resources': {'backend': {'container_id': 'cid-1'}}}
        s2 = {'timestamp': 't2', 'monotonic': 102.0, 'process_instance': 'p1',
              'ingress': {'Committed': 60, 'Received': 60, 'SamplesNew': 6000},
              'resources': {'backend': {'container_id': 'cid-2'}}}  # Restart!
        terminal = {'timestamp': 't3', 'monotonic': 103.0, 'process_instance': 'p1',
                    'ingress': {'Committed': 60, 'Received': 60, 'SamplesNew': 6000},
                    'resources': {'backend': {'container_id': 'cid-2'}}}

        res = e2.infer_host_a_active_window(
            snapshots=[s1, s2],
            baseline=baseline,
            terminal=terminal,
            expected_total_messages=60,
            expected_total_samples=6000,
            sampling_interval=1.0,
            evicted_count=0,
        )
        self.assertEqual(res['status'], 'INCONCLUSIVE')
        self.assertEqual(res['reason'], 'container_identity_changed')

    def test_e2_active_window_missing_terminal_or_post_boundary_inconclusive(self):
        """Active window inference requires stable post-boundary observation."""
        baseline = {'timestamp': 't0', 'monotonic': 100.0, 'process_instance': 'p1',
                    'ingress': {'Committed': 0, 'Received': 0, 'SamplesNew': 0}}
        s1 = {'timestamp': 't1', 'monotonic': 101.0, 'process_instance': 'p1',
              'ingress': {'Committed': 0, 'Received': 0, 'SamplesNew': 0}}
        s2 = {'timestamp': 't2', 'monotonic': 102.0, 'process_instance': 'p1',
              'ingress': {'Committed': 30, 'Received': 30, 'SamplesNew': 3000}}
        s3 = {'timestamp': 't3', 'monotonic': 103.0, 'process_instance': 'p1',
              'ingress': {'Committed': 60, 'Received': 60, 'SamplesNew': 6000}}

        # terminal missing, and no post snapshot confirming flatness
        res = e2.infer_host_a_active_window(
            snapshots=[s1, s2, s3],
            baseline=baseline,
            terminal=None,
            expected_total_messages=60,
            expected_total_samples=6000,
            sampling_interval=1.0,
            evicted_count=0,
        )
        self.assertEqual(res['status'], 'INCONCLUSIVE')
        self.assertIn(res['reason'], ('post_boundary_missing', 'terminal_missing'))

    def test_e2_active_window_short_burst_inconclusive(self):
        """When all messages land in a single snapshot interval, window is marked INCONCLUSIVE."""
        baseline = {'timestamp': 't0', 'monotonic': 100.0, 'process_instance': 'p1',
                    'ingress': {'Committed': 0, 'Received': 0, 'SamplesNew': 0}}
        s1 = {'timestamp': 't1', 'monotonic': 101.0, 'process_instance': 'p1',
              'ingress': {'Committed': 0, 'Received': 0, 'SamplesNew': 0}}
        # Everything committed in single snapshot interval
        s2 = {'timestamp': 't2', 'monotonic': 102.0, 'process_instance': 'p1',
              'ingress': {'Committed': 60, 'Received': 60, 'SamplesNew': 6000}}
        s3 = {'timestamp': 't3', 'monotonic': 103.0, 'process_instance': 'p1',
              'ingress': {'Committed': 60, 'Received': 60, 'SamplesNew': 6000}}
        terminal = {'timestamp': 't4', 'monotonic': 104.0, 'process_instance': 'p1',
                    'ingress': {'Committed': 60, 'Received': 60, 'SamplesNew': 6000}}

        res = e2.infer_host_a_active_window(
            snapshots=[s1, s2, s3],
            baseline=baseline,
            terminal=terminal,
            expected_total_messages=60,
            expected_total_samples=6000,
            sampling_interval=1.0,
            evicted_count=0,
        )
        self.assertEqual(res['status'], 'INCONCLUSIVE')
        self.assertIn(res['reason'], ('sparse_observations', 'short_burst_unresolved'))

    def test_e2_active_window_valid_window_and_normalized_metrics(self):
        """Normal progressive workload yields valid active window and normalized metrics."""
        baseline = {
            'timestamp': '2026-01-01T12:00:00Z', 'monotonic': 100.0, 'process_instance': 'p1',
            'ingress': {'Committed': 0, 'Received': 0, 'AcceptedBytes': 0, 'SamplesNew': 0},
            'persistence': {'copy_calls': 0},
            'resources': {
                'backend': {'cpu_cumulative_seconds': 10.0, 'memory_bytes': 10000000},
                'postgres': {'cpu_cumulative_seconds': 5.0, 'memory_bytes': 20000000},
                'mosquitto': {'cpu_cumulative_seconds': 2.0, 'memory_bytes': 5000000},
                'nginx': {'cpu_cumulative_seconds': 1.0, 'memory_bytes': 3000000},
            }
        }
        s1 = {
            'timestamp': '2026-01-01T12:00:01Z', 'monotonic': 101.0, 'process_instance': 'p1',
            'ingress': {'Committed': 0, 'Received': 0, 'AcceptedBytes': 0, 'SamplesNew': 0},
            'persistence': {'copy_calls': 0},
            'resources': {
                'backend': {'cpu_cumulative_seconds': 10.1, 'memory_bytes': 10000000},
                'postgres': {'cpu_cumulative_seconds': 5.0, 'memory_bytes': 20000000},
                'mosquitto': {'cpu_cumulative_seconds': 2.0, 'memory_bytes': 5000000},
                'nginx': {'cpu_cumulative_seconds': 1.0, 'memory_bytes': 3000000},
            }
        }
        s2 = {
            'timestamp': '2026-01-01T12:00:02Z', 'monotonic': 102.0, 'process_instance': 'p1',
            'ingress': {'Committed': 20, 'Received': 20, 'AcceptedBytes': 20000, 'SamplesNew': 2000},
            'persistence': {'copy_calls': 20},
            'resources': {
                'backend': {'cpu_cumulative_seconds': 11.0, 'memory_bytes': 12000000},
                'postgres': {'cpu_cumulative_seconds': 6.0, 'memory_bytes': 22000000},
                'mosquitto': {'cpu_cumulative_seconds': 2.5, 'memory_bytes': 5500000},
                'nginx': {'cpu_cumulative_seconds': 1.2, 'memory_bytes': 3200000},
            }
        }
        s3 = {
            'timestamp': '2026-01-01T12:00:03Z', 'monotonic': 103.0, 'process_instance': 'p1',
            'ingress': {'Committed': 40, 'Received': 40, 'AcceptedBytes': 40000, 'SamplesNew': 4000},
            'persistence': {'copy_calls': 40},
            'resources': {
                'backend': {'cpu_cumulative_seconds': 12.0, 'memory_bytes': 13000000},
                'postgres': {'cpu_cumulative_seconds': 7.0, 'memory_bytes': 23000000},
                'mosquitto': {'cpu_cumulative_seconds': 3.0, 'memory_bytes': 5600000},
                'nginx': {'cpu_cumulative_seconds': 1.5, 'memory_bytes': 3300000},
            }
        }
        s4 = {
            'timestamp': '2026-01-01T12:00:04Z', 'monotonic': 104.0, 'process_instance': 'p1',
            'ingress': {'Committed': 60, 'Received': 60, 'AcceptedBytes': 60000, 'SamplesNew': 6000},
            'persistence': {'copy_calls': 60},
            'resources': {
                'backend': {'cpu_cumulative_seconds': 13.0, 'memory_bytes': 14000000},
                'postgres': {'cpu_cumulative_seconds': 8.0, 'memory_bytes': 24000000},
                'mosquitto': {'cpu_cumulative_seconds': 3.5, 'memory_bytes': 5800000},
                'nginx': {'cpu_cumulative_seconds': 1.8, 'memory_bytes': 3400000},
            }
        }
        s5 = {
            'timestamp': '2026-01-01T12:00:05Z', 'monotonic': 105.0, 'process_instance': 'p1',
            'ingress': {'Committed': 60, 'Received': 60, 'AcceptedBytes': 60000, 'SamplesNew': 6000},
            'persistence': {'copy_calls': 60},
            'resources': {
                'backend': {'cpu_cumulative_seconds': 13.1, 'memory_bytes': 14000000},
                'postgres': {'cpu_cumulative_seconds': 8.0, 'memory_bytes': 24000000},
                'mosquitto': {'cpu_cumulative_seconds': 3.5, 'memory_bytes': 5800000},
                'nginx': {'cpu_cumulative_seconds': 1.8, 'memory_bytes': 3400000},
            }
        }
        terminal = {
            'timestamp': '2026-01-01T12:00:06Z', 'monotonic': 106.0, 'process_instance': 'p1',
            'ingress': {'Committed': 60, 'Received': 60, 'AcceptedBytes': 60000, 'SamplesNew': 6000},
            'persistence': {'copy_calls': 60},
            'resources': {
                'backend': {'cpu_cumulative_seconds': 13.5, 'memory_bytes': 14000000},
                'postgres': {'cpu_cumulative_seconds': 8.2, 'memory_bytes': 24000000},
                'mosquitto': {'cpu_cumulative_seconds': 3.6, 'memory_bytes': 5800000},
                'nginx': {'cpu_cumulative_seconds': 1.9, 'memory_bytes': 3400000},
            }
        }

        win = e2.infer_host_a_active_window(
            snapshots=[s1, s2, s3, s4, s5],
            baseline=baseline,
            terminal=terminal,
            expected_total_messages=60,
            expected_total_samples=6000,
            sampling_interval=1.0,
            evicted_count=0,
        )
        self.assertEqual(win['status'], 'AVAILABLE')
        self.assertEqual(win['start_sample_index'], 1)  # s2 (at index 1) is first change
        self.assertEqual(win['end_sample_index'], 3)    # s4 (at index 3) reached 60 commits
        self.assertAlmostEqual(win['duration_seconds'], 2.0)
        self.assertLessEqual(win['duration_uncertainty_seconds'], 1.0)
        self.assertAlmostEqual(win['active_cpu_seconds']['total'], 8.2)

        norm = e2.calculate_equal_samples_normalized_metrics(
            actual_n=6000,
            total_payload_bytes=60000,
            total_db_transactions=60,
            active_window=win,
            lifecycle_window={'duration_seconds': 6.0, 'baseline_cpu_seconds': 18.0, 'terminal_cpu_seconds': 27.2},
        )
        self.assertAlmostEqual(norm['bytes_per_sample'], 10.0)
        self.assertAlmostEqual(norm['db_transactions_per_sample'], 0.01)
        self.assertTrue(norm['active_cpu_available'])
        self.assertAlmostEqual(norm['active_cpu_seconds_per_sample'], win['active_cpu_seconds']['total'] / 6000)

    def test_docker_cumulative_cpu_readback_unsupported_honesty(self):
        """When Docker cumulative CPU is unsupported/unreadable, return None and available=False."""
        mock_registry = Mock()
        containers = {
            'backend': {'receipt': {'id': 'cid-b'}, 'name': 'c-backend'},
            'postgres': {'receipt': {'id': 'cid-p'}, 'name': 'c-postgres'},
            'mosquitto': {'receipt': {'id': 'cid-m'}, 'name': 'c-mosquitto'},
            'nginx': {'receipt': {'id': 'cid-n'}, 'name': 'c-nginx'},
        }
        # Docker API returns connection error / failure
        res = e2.read_container_resources(
            mock_registry,
            containers,
            docker_client=lambda cid: {'available': False, 'reason': 'docker_socket_unreachable'},
        )
        self.assertEqual(res['available'], False)
        for svc in ('backend', 'postgres', 'mosquitto', 'nginx'):
            self.assertIsNone(res['services'][svc]['cpu_cumulative_seconds'])
            self.assertFalse(res['services'][svc]['available'])
            self.assertEqual(res['services'][svc]['reason'], 'docker_socket_unreachable')

    def test_finish_remote_collection_equal_samples_reports_measurement_quality_and_resources(self):
        """finish_remote_collection builds measurement_quality and resources separately from functional validity."""
        fixed_now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        ctx = build_e2_equal_samples_context(total_samples=6000, sample_rate=100, seed=84, now_utc=fixed_now)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            bundle_dir, binary_file, prepared = self._create_test_equal_samples_bundle(root, ctx)

            class FakeSampler:
                def __init__(self):
                    self.stopped = False
                    self.errors = []
                    self.metrics = []
                    self.resources = []
                    self.evicted_count = 0
                def stop(self, pid):
                    self.stopped = True
                def get_coverage(self):
                    return {
                        'sampling_interval_seconds': 1.0,
                        'deadline_seconds': 1800.0,
                        'snapshots_captured': 5,
                        'snapshots_retained': 5,
                        'snapshots_evicted': 0,
                        'evicted_bytes': 0,
                        'ring_byte_cap': 2097152,
                        'ring_bytes_used': 2500,
                        'collector_errors': [],
                    }
                @property
                def retained_snapshots(self):
                    return self.metrics
                def record_terminal(self, m, r):
                    self.terminal_metrics = m
                    self.terminal_resources = r

            sampler = FakeSampler()
            prepared['collector'] = sampler
            prepared['specs'] = [Mock(gateway_id='gw-a', sensor_id='s-a', sensor_urn='urn:ngsi-ld:Sensor:gw-a:s-a'),
                                 Mock(gateway_id='gw-b', sensor_id='s-b', sensor_urn='urn:ngsi-ld:Sensor:gw-b:s-b')]
            prepared['pg'] = 'fake-pg'
            prepared['tokens'] = {'admin': 'admin-tok'}
            prepared['before_global'] = {'ledger': 0, 'raw': 0, 'temporal': 0, 'configuration': 0}
            ingress_fields = (
                'Received', 'ReceivedBytes', 'Accepted', 'AcceptedBytes',
                'RejectedNotRunning', 'RejectedStopping', 'RejectedRetained',
                'RejectedOversized', 'RejectedInvalidTopic', 'QueueFull',
                'ByteBudgetFull', 'ShaperPassed', 'ShaperRateLimited',
                'ShaperStateCapacity', 'Processed', 'ValidationRejected',
                'PersistFailures', 'CommitUnconfirmed', 'IdentityMismatch',
                'ACKBuildFailures', 'Committed', 'DuplicateCommitted',
                'SamplesNew', 'ACKPublished', 'ACKPublishFailures'
            )
            base_ing = {f: 0 for f in ingress_fields}
            term_ing = {f: 0 for f in ingress_fields}
            term_ing.update({
                'Received': 60, 'Accepted': 60, 'AcceptedBytes': 60000,
                'Committed': 60, 'ACKPublished': 60, 'SamplesNew': 6000,
            })
            prepared['baseline'] = {'process_instance': 'p-1', 'ingress': base_ing, 'monotonic': 100.0,
                                    'timestamp': '2026-01-01T12:00:00Z'}
            prepared['before_targets'] = [{'ledger': 0, 'raw': 0, 'temporal': 0, 'configuration': 0},
                                          {'ledger': 0, 'raw': 0, 'temporal': 0, 'configuration': 0}]
            class FakeFixture:
                artifacts = root / 'artifacts'
            FakeFixture.artifacts.mkdir(mode=0o700, parents=True)

            smoke = Mock()
            smoke.http_request.return_value = (200, {}, {
                'process_instance': 'p-1', 'ingress': term_ing,
            })
            smoke.paper_p0_stats.return_value = {'cpu': 10}
            smoke.paper_p1_count_db.return_value = {'ledger': 60, 'raw': 6000, 'temporal': 60, 'configuration': 0}
            def fake_psql(reg, pg, sql, params):
                if 'FROM processed_messages p1' in sql: return '0'
                if 'json_build_object' in sql and 'ledger_count' in sql:
                    return json.dumps({'ledger_count': 30, 'min_first_sequence': 0, 'max_last_sequence': 2999,
                                       'total_sample_count': 3000, 'invalid_sample_count': 0, 'invalid_sequence_bounds': 0})
                if 'sample_count IS DISTINCT FROM' in sql or 'sample_count <> (SELECT count(*)' in sql: return '0'
                if 'FROM telemetry t' in sql and 'unique_sequence_count' in sql:
                    return json.dumps({'raw_count': 3000, 'unique_sequence_count': 3000, 'min_sequence': 0,
                                       'max_sequence': 2999, 'value_mismatches': 0, 'timestamp_mismatches': 0})
                if 'LIMIT 20' in sql:
                    return json.dumps([{'message_id': '550e8400-e29b-41d4-a716-446655440001', 'first_sequence': '0', 'last_sequence': '99', 'sample_count': 100}])
                return json.dumps({'ledger': 30, 'raw': 3000, 'temporal': 30, 'configuration': 0})
            smoke.db_psql.side_effect = fake_psql

            result = e2.finish_remote_collection(
                smoke, FakeFixture, Mock(url='https://127.0.0.1:443'), Mock(), {},
                prepared, bundle_dir, phase_id=prepared['phase_id'], paperload_binary=binary_file)

            self.assertEqual(result['status'], 'PASS')
            self.assertIn('measurement_quality', result)
            self.assertIn('resources', result)
            self.assertIn('active_window', result['measurement_quality'])
            self.assertIn('lifecycle_window', result['measurement_quality'])
            self.assertIn('coverage', result['measurement_quality'])
            self.assertIn('normalized_metrics', result['measurement_quality'])
            # ES.5b: No generator resources provided -> unavailable with reason, not DEFERRED_ES5B
            self.assertEqual(result['resources']['generator']['status'], 'UNAVAILABLE')
            self.assertEqual(result['resources']['generator']['availability'], 'unavailable')
            self.assertEqual(result['resources']['generator']['reason'], 'no_sampler_evidence')
            self.assertIsNone(result['resources']['generator']['lifecycle_cpu_seconds'])

    def test_finish_remote_collection_with_generator_resources_available(self):
        """When generator resources are in bundle, finish_remote_collection wires generator metrics."""
        fixed_now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        ctx = build_e2_equal_samples_context(total_samples=6000, sample_rate=100, seed=84, now_utc=fixed_now)
        gen_res_data = self._sample_generator_resources()

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            bundle_dir, binary_file, prepared = self._create_test_equal_samples_bundle(
                root, ctx, generator_resources=gen_res_data)

            class FakeSampler:
                def __init__(self):
                    self.stopped = False
                    self.errors = []
                    self.metrics = []
                    self.resources = []
                    self.evicted_count = 0
                def stop(self, pid):
                    self.stopped = True
                def get_coverage(self):
                    return {
                        'sampling_interval_seconds': 1.0,
                        'deadline_seconds': 1800.0,
                        'snapshots_captured': 5,
                        'snapshots_retained': 5,
                        'snapshots_evicted': 0,
                        'evicted_bytes': 0,
                        'ring_byte_cap': 2097152,
                        'ring_bytes_used': 2500,
                        'collector_errors': [],
                    }
                @property
                def retained_snapshots(self):
                    return self.metrics
                def record_terminal(self, m, r):
                    self.terminal_metrics = m
                    self.terminal_resources = r

            prepared['collector'] = FakeSampler()
            prepared['specs'] = [Mock(gateway_id='gw-a', sensor_id='s-a', sensor_urn='urn:ngsi-ld:Sensor:gw-a:s-a'),
                                 Mock(gateway_id='gw-b', sensor_id='s-b', sensor_urn='urn:ngsi-ld:Sensor:gw-b:s-b')]
            prepared['pg'] = 'fake-pg'
            prepared['tokens'] = {'admin': 'admin-tok'}
            prepared['before_global'] = {'ledger': 0, 'raw': 0, 'temporal': 0, 'configuration': 0}
            ingress_fields = (
                'Received', 'ReceivedBytes', 'Accepted', 'AcceptedBytes',
                'RejectedNotRunning', 'RejectedStopping', 'RejectedRetained',
                'RejectedOversized', 'RejectedInvalidTopic', 'QueueFull',
                'ByteBudgetFull', 'ShaperPassed', 'ShaperRateLimited',
                'ShaperStateCapacity', 'Processed', 'ValidationRejected',
                'PersistFailures', 'CommitUnconfirmed', 'IdentityMismatch',
                'ACKBuildFailures', 'Committed', 'DuplicateCommitted',
                'SamplesNew', 'ACKPublished', 'ACKPublishFailures'
            )
            base_ing = {f: 0 for f in ingress_fields}
            term_ing = {f: 0 for f in ingress_fields}
            term_ing.update({
                'Received': 60, 'Accepted': 60, 'AcceptedBytes': 60000,
                'Committed': 60, 'ACKPublished': 60, 'SamplesNew': 6000,
            })
            prepared['baseline'] = {'process_instance': 'p-1', 'ingress': base_ing, 'monotonic': 100.0,
                                    'timestamp': '2026-01-01T12:00:00Z',
                                    'persistence': {'confirmed_commits': 0}}
            prepared['before_targets'] = [{'ledger': 0, 'raw': 0, 'temporal': 0, 'configuration': 0},
                                          {'ledger': 0, 'raw': 0, 'temporal': 0, 'configuration': 0}]
            class FakeFixture:
                artifacts = root / 'artifacts'
            FakeFixture.artifacts.mkdir(mode=0o700, parents=True)

            smoke = Mock()
            smoke.http_request.return_value = (200, {}, {
                'process_instance': 'p-1', 'ingress': term_ing,
                'persistence': {'confirmed_commits': 60},
            })
            smoke.paper_p0_stats.return_value = {'cpu': 10}
            smoke.paper_p1_count_db.return_value = {'ledger': 60, 'raw': 6000, 'temporal': 60, 'configuration': 0}
            def fake_psql(reg, pg, sql, params):
                if 'FROM processed_messages p1' in sql: return '0'
                if 'json_build_object' in sql and 'ledger_count' in sql:
                    return json.dumps({'ledger_count': 30, 'min_first_sequence': 0, 'max_last_sequence': 2999,
                                       'total_sample_count': 3000, 'invalid_sample_count': 0, 'invalid_sequence_bounds': 0})
                if 'sample_count IS DISTINCT FROM' in sql or 'sample_count <> (SELECT count(*)' in sql: return '0'
                if 'FROM telemetry t' in sql and 'unique_sequence_count' in sql:
                    return json.dumps({'raw_count': 3000, 'unique_sequence_count': 3000, 'min_sequence': 0,
                                       'max_sequence': 2999, 'value_mismatches': 0, 'timestamp_mismatches': 0})
                if 'LIMIT 20' in sql:
                    return json.dumps([{'message_id': '550e8400-e29b-41d4-a716-446655440001', 'first_sequence': '0', 'last_sequence': '99', 'sample_count': 100}])
                return json.dumps({'ledger': 30, 'raw': 3000, 'temporal': 30, 'configuration': 0})
            smoke.db_psql.side_effect = fake_psql

            result = e2.finish_remote_collection(
                smoke, FakeFixture, Mock(url='https://127.0.0.1:443'), Mock(), {},
                prepared, bundle_dir, phase_id=prepared['phase_id'], paperload_binary=binary_file)

            self.assertEqual(result['status'], 'PASS')
            gen = result['resources']['generator']
            self.assertEqual(gen['status'], 'AVAILABLE')
            self.assertEqual(gen['availability'], 'available')
            self.assertEqual(gen['role'], 'generator_B')
            self.assertIsNone(gen['reason'])
            # 200 ticks / 100 clk_tck = 2.0 seconds
            self.assertAlmostEqual(gen['lifecycle_cpu_seconds'], 2.0)
            self.assertEqual(gen['cpu_seconds_unit'], 'seconds')
            self.assertEqual(gen['rss_unit'], 'bytes')
            self.assertEqual(gen['sampled_max_rss_bytes'], 35000000)
            self.assertIsNone(gen['absolute_peak_rss_bytes'])
            self.assertIn('sampled_max_rss_not_peak', gen['peak_qualification'])

            # Normalized generator metrics
            norm = result['measurement_quality']['normalized_metrics']
            self.assertTrue(norm['generator_lifecycle_cpu_available'])
            self.assertAlmostEqual(norm['generator_lifecycle_cpu_seconds_per_sample'], 2.0 / 6000)

            # Per-metric quality
            per_metric = result['measurement_quality']['per_metric']
            self.assertEqual(per_metric['payload_bytes']['status'], 'AVAILABLE')
            self.assertEqual(per_metric['db_transactions']['status'], 'AVAILABLE')
            self.assertEqual(per_metric['generator_lifecycle_cpu']['status'], 'AVAILABLE')

    def test_load_e2_result_bundle_rejects_bad_generator_resources_hash_and_shape(self):
        """Tampered generator resource hash or invalid shape is rejected."""
        fixed_now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        ctx = build_e2_equal_samples_context(total_samples=6000, sample_rate=100, seed=84, now_utc=fixed_now)
        gen_res = self._sample_generator_resources()

        # 1. Digest mismatch
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            bundle_dir, binary_file, prepared = self._create_test_equal_samples_bundle(
                root, ctx, generator_resources=gen_res)
            # Tamper the file content
            gen_file = bundle_dir / 'results/runner/resources-generator.json'
            gen_file.write_bytes(b'{"tampered": true}')
            with self.assertRaises(ValueError) as ctx_err:
                e2._load_e2_result_bundle(bundle_dir, prepared, binary_file)
            self.assertIn('e2_result_digest_mismatch', str(ctx_err.exception))

        # 2. Bad shape (unknown keys or invalid role)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            bad_gen_res = dict(gen_res, role='invalid_role')
            bundle_dir, binary_file, prepared = self._create_test_equal_samples_bundle(
                root, ctx, generator_resources=bad_gen_res)
            with self.assertRaises(ValueError):
                e2._load_e2_result_bundle(bundle_dir, prepared, binary_file)

    def test_calculate_equal_samples_normalized_metrics_denominator_and_units(self):
        """calculate_equal_samples_normalized_metrics requires positive N and labels units."""
        # Non-positive N
        with self.assertRaises(ValueError) as ctx_err:
            e2.calculate_equal_samples_normalized_metrics(
                actual_n=0, total_payload_bytes=1000, total_db_transactions=10,
                active_window={}, lifecycle_window={})
        self.assertIn('actual_n_must_be_positive_int', str(ctx_err.exception))

        with self.assertRaises(ValueError):
            e2.calculate_equal_samples_normalized_metrics(
                actual_n=-10, total_payload_bytes=1000, total_db_transactions=10,
                active_window={}, lifecycle_window={})

        # Valid N with generator resources
        gen_res = {
            'availability': 'available',
            'lifecycle_cpu_seconds': 3.0,
            'active_cpu_seconds': 1.5,
        }
        res = e2.calculate_equal_samples_normalized_metrics(
            actual_n=3000, total_payload_bytes=60000, total_db_transactions=30,
            active_window={'status': 'AVAILABLE', 'active_cpu_seconds': {'total': 6.0}},
            lifecycle_window={'baseline_cpu_seconds': 10.0, 'terminal_cpu_seconds': 20.0},
            generator_resources=gen_res,
        )
        self.assertEqual(res['actual_samples'], 3000)
        self.assertAlmostEqual(res['bytes_per_sample'], 20.0)
        self.assertAlmostEqual(res['db_transactions_per_sample'], 0.01)
        self.assertAlmostEqual(res['active_cpu_seconds_per_sample'], 6.0 / 3000)
        self.assertAlmostEqual(res['lifecycle_cpu_seconds_per_sample'], 10.0 / 3000)
        self.assertAlmostEqual(res['generator_lifecycle_cpu_seconds_per_sample'], 3.0 / 3000)
        self.assertAlmostEqual(res['generator_active_cpu_seconds_per_sample'], 1.5 / 3000)
        self.assertIn('not_wire_traffic_not_tls', res['payload_bytes_qualification'])
        self.assertIn('not_sensor_acquisition_latency', res['latency_qualification'])

    def test_sparse_window_reports_per_metric_inconclusive_without_suppressing_clean_payload(self):
        """When server active window is sparse/inconclusive, server CPU is INCONCLUSIVE but payload/DB are AVAILABLE."""
        # Empty snapshots -> sparse_observations
        win = e2.infer_host_a_active_window(
            snapshots=[], baseline={'ingress': {'Committed': 0}}, terminal={},
            expected_total_messages=60, expected_total_samples=6000)
        self.assertEqual(win['status'], 'INCONCLUSIVE')
        self.assertEqual(win['reason'], 'sparse_observations')

        norm = e2.calculate_equal_samples_normalized_metrics(
            actual_n=6000, total_payload_bytes=60000, total_db_transactions=60,
            active_window=win, lifecycle_window={})
        self.assertFalse(norm['active_cpu_available'])
        self.assertEqual(norm['active_cpu_unavailable_reason'], 'sparse_observations')
        self.assertAlmostEqual(norm['bytes_per_sample'], 10.0)
        self.assertAlmostEqual(norm['db_transactions_per_sample'], 0.01)

    def test_sampler_projection_retains_confirmed_commits(self):
        """EqualSamplesCompactSampler._project_metrics must project confirmed_commits and supported persistence keys."""
        raw = {
            'persistence': {
                'confirmed_commits': 42,
                'single_attempts': 1,
                'group_attempts': 2,
                'group_members': 3,
                'copy_calls': 4,
                'confirmed_new_rows': 5,
            }
        }
        proj = e2.EqualSamplesCompactSampler._project_metrics(raw)
        self.assertIn('confirmed_commits', proj['persistence'])
        self.assertEqual(proj['persistence']['confirmed_commits'], 42)
        self.assertEqual(proj['persistence']['single_attempts'], 1)
        self.assertEqual(proj['persistence']['group_attempts'], 2)

    def test_active_window_unbracketed_final_commits_returns_inconclusive(self):
        """When snapshots do not observe final commit target with timestamped resources, return INCONCLUSIVE."""
        base = {'monotonic': 100.0, 'timestamp': '2026-10-31T00:00:00Z', 'ingress': {'Committed': 0, 'Received': 0, 'SamplesNew': 0},
                'resources': {'backend': {'cpu_cumulative_seconds': 1.0}, 'postgres': {'cpu_cumulative_seconds': 1.0},
                              'mosquitto': {'cpu_cumulative_seconds': 1.0}, 'nginx': {'cpu_cumulative_seconds': 1.0}}}
        snaps = [
            {'monotonic': 108.0, 'timestamp': '2026-10-31T00:00:08Z', 'ingress': {'Committed': 0, 'Received': 0, 'SamplesNew': 0},
             'resources': {'backend': {'cpu_cumulative_seconds': 1.1}, 'postgres': {'cpu_cumulative_seconds': 1.1},
                           'mosquitto': {'cpu_cumulative_seconds': 1.1}, 'nginx': {'cpu_cumulative_seconds': 1.1}}},
            {'monotonic': 116.0, 'timestamp': '2026-10-31T00:00:16Z', 'ingress': {'Committed': 10, 'Received': 10, 'SamplesNew': 10},
             'resources': {'backend': {'cpu_cumulative_seconds': 2.0}, 'postgres': {'cpu_cumulative_seconds': 2.0},
                           'mosquitto': {'cpu_cumulative_seconds': 2.0}, 'nginx': {'cpu_cumulative_seconds': 2.0}}},
            {'monotonic': 124.0, 'timestamp': '2026-10-31T00:00:24Z', 'ingress': {'Committed': 50, 'Received': 50, 'SamplesNew': 50},
             'resources': {'backend': {'cpu_cumulative_seconds': 3.0}, 'postgres': {'cpu_cumulative_seconds': 3.0},
                           'mosquitto': {'cpu_cumulative_seconds': 3.0}, 'nginx': {'cpu_cumulative_seconds': 3.0}}},
        ]
        term = {'monotonic': None, 'timestamp': None, 'ingress': {'Committed': 60, 'Received': 60, 'SamplesNew': 60}, 'resources': None}

        win = e2.infer_host_a_active_window(
            snapshots=snaps, baseline=base, terminal=term,
            expected_total_messages=60, expected_total_samples=6000, sampling_interval=1.0)
        self.assertEqual(win['status'], 'INCONCLUSIVE')
        self.assertEqual(win['reason'], 'final_commit_boundary_not_observed')

    def test_calculate_normalized_metrics_missing_commit_counter(self):
        """When total_db_transactions is None, db_transactions_per_sample is None and unavailable."""
        res = e2.calculate_equal_samples_normalized_metrics(
            actual_n=6000, total_payload_bytes=60000, total_db_transactions=None,
            active_window={}, lifecycle_window={},
            db_transactions_unavailable_reason='db_transactions_counter_not_retained_in_sampler',
            ledger_rows=6000,
        )
        self.assertIsNone(res['db_transactions_per_sample'])
        self.assertFalse(res['db_transactions_available'])
        self.assertEqual(res['db_transactions_unavailable_reason'], 'db_transactions_counter_not_retained_in_sampler')
        self.assertEqual(res['ledger_rows_per_sample'], 1.0)

    def test_finish_remote_collection_legacy_steady_burst_exact_canonical_delta_pass(self):
        """finish_remote_collection in legacy steady_burst mode derives canonical 4-key delta and passes validation."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            bundle_dir, binary_file, prepared = self._create_test_legacy_bundle(root, samples_per_message=100)
            smoke, FakeFixture, collector = self._setup_legacy_finish_env(root, prepared)

            result = e2.finish_remote_collection(
                smoke, FakeFixture, Mock(url='https://127.0.0.1:443'), Mock(), {},
                prepared, bundle_dir, phase_id=prepared['phase_id'], paperload_binary=binary_file)

            # Assert exact canonical 4 keys in delta
            expected_delta = {'ledger': 24, 'raw': 2400, 'temporal': 24, 'configuration': 0}
            self.assertEqual(result['database_delta'], expected_delta)
            self.assertEqual(set(result['database_delta'].keys()), {'ledger', 'raw', 'temporal', 'configuration'})
            self.assertNotIn('processed', result['database_delta'])
            self.assertNotIn('telemetry', result['database_delta'])

            # Canonical counts alone do not prove exact raw retention. This legacy
            # fixture only verifies sampled identity ranges, so acceptance fails closed.
            self.assertEqual(result['status'], 'FAIL')
            self.assertEqual(result['workload_outcome'], 'VALIDATION_FAILED')
            self.assertFalse(result['validation']['valid'])
            self.assertEqual(result['validation']['failures'], ['pilot_raw_retention_verification'])
            self.assertTrue(collector.stopped)

    def test_finish_remote_collection_writes_full_official_diagnostics_envelope(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            bundle_dir, binary_file, prepared = self._create_test_legacy_bundle(root, samples_per_message=100)
            smoke, Fixture, _ = self._setup_legacy_finish_env(root, prepared)
            output = Fixture.artifacts
            def durable(path, payload):
                Path(path).write_text(json.dumps(payload))
            smoke.durable_json.side_effect = durable
            collector = e2.EqualSamplesCompactSampler(scrape=lambda: {'ingress': {'Received': 1}},
                interval=0.1, max_snapshots=8, byte_cap=4096, deadline_seconds=10,
                baseline_metrics={'ingress': {'Received': 0}}, baseline_resources={'backend': {'available': True}})
            collector.sample_step()
            collector.side_sampler = e2.IndependentSideSampler(reader={
                'postgres': lambda: {'sample': 1}, 'linux': lambda: {'sample': 1},
                'docker': lambda: {'services': {'backend': {'available': True}}}})
            collector.side_sampler.sample_step()
            prepared['collector'] = collector
            prepared.update({'source_provenance': {'revision': 'test-rev'},
                'diagnostic_baseline': {'postgres': {'baseline': True}},
                'manifest_sha256': ['manifest']})
            with patch.object(e2, 'read_postgres_diagnostics', return_value={'terminal': 'pg'}), \
                 patch.object(e2, 'read_linux_resources', return_value={'terminal': 'linux'}), \
                 patch.object(e2, 'read_container_resources', return_value={'services': {'backend': {'available': True}}}):
                e2.finish_remote_collection(smoke, Fixture, Mock(url='https://fixture'), Mock(), {},
                    prepared, bundle_dir, phase_id=prepared['phase_id'], paperload_binary=binary_file)
            artifact = json.loads((output / 'p8-e2-diagnostics.json').read_text())
            self.assertEqual(artifact['source'], {'revision': 'test-rev'})
            self.assertEqual(artifact['full_p8_accepted'], False)
            self.assertIn('snapshots', artifact); self.assertIn('coverage', artifact)
            self.assertIn('resource_samples', artifact); self.assertIn('postgres_linux', artifact)
            self.assertEqual(artifact['postgres_linux']['terminal']['postgres'], {'terminal': 'pg'})

    def test_finish_remote_collection_nonzero_configuration_triggers_failure(self):
        """finish_remote_collection captures actual configuration difference and triggers validation failure."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            bundle_dir, binary_file, prepared = self._create_test_legacy_bundle(root, samples_per_message=100)
            smoke, FakeFixture, collector = self._setup_legacy_finish_env(root, prepared)
            # Simulate DB configuration count increased from 0 to 1
            smoke.paper_p1_count_db.return_value = {'ledger': 24, 'raw': 2400, 'temporal': 24, 'configuration': 1}

            result = e2.finish_remote_collection(
                smoke, FakeFixture, Mock(url='https://127.0.0.1:443'), Mock(), {},
                prepared, bundle_dir, phase_id=prepared['phase_id'], paperload_binary=binary_file)

            # Delta must reflect actual configuration difference, not fake 0
            self.assertEqual(result['database_delta']['configuration'], 1)
            self.assertEqual(result['status'], 'FAIL')
            self.assertFalse(result['validation']['valid'])
            self.assertIn('database_delta_mismatch', result['validation']['failures'])

    def test_finish_remote_collection_count_validation_fails_closed(self):
        """finish_remote_collection fails closed on missing, boolean, negative, or reset count input."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            bundle_dir, binary_file, prepared = self._create_test_legacy_bundle(root, samples_per_message=100)

            # 1. Missing key in before_global
            smoke, FakeFixture, collector = self._setup_legacy_finish_env(root, prepared)
            prepared['before_global'] = {'ledger': 0, 'raw': 0, 'temporal': 0}
            with self.assertRaises(ValueError) as ctx:
                e2.finish_remote_collection(
                    smoke, FakeFixture, Mock(url='https://127.0.0.1:443'), Mock(), {},
                    prepared, bundle_dir, phase_id=prepared['phase_id'], paperload_binary=binary_file)
            self.assertIn('missing_key', str(ctx.exception).lower())

            # 2. Missing key in after_global
            smoke, FakeFixture, collector = self._setup_legacy_finish_env(root, prepared)
            smoke.paper_p1_count_db.return_value = {'ledger': 24, 'raw': 2400, 'temporal': 24}
            with self.assertRaises(ValueError) as ctx:
                e2.finish_remote_collection(
                    smoke, FakeFixture, Mock(url='https://127.0.0.1:443'), Mock(), {},
                    prepared, bundle_dir, phase_id=prepared['phase_id'], paperload_binary=binary_file)
            self.assertIn('missing_key', str(ctx.exception).lower())

            # 3. Boolean count value in before_global (bool rejected)
            smoke, FakeFixture, collector = self._setup_legacy_finish_env(root, prepared)
            prepared['before_global'] = {'ledger': True, 'raw': 0, 'temporal': 0, 'configuration': 0}
            with self.assertRaises(ValueError) as ctx:
                e2.finish_remote_collection(
                    smoke, FakeFixture, Mock(url='https://127.0.0.1:443'), Mock(), {},
                    prepared, bundle_dir, phase_id=prepared['phase_id'], paperload_binary=binary_file)
            self.assertIn('not_int', str(ctx.exception).lower())

            # 4. Boolean count value in after_global
            smoke, FakeFixture, collector = self._setup_legacy_finish_env(root, prepared)
            smoke.paper_p1_count_db.return_value = {'ledger': 24, 'raw': False, 'temporal': 24, 'configuration': 0}
            with self.assertRaises(ValueError) as ctx:
                e2.finish_remote_collection(
                    smoke, FakeFixture, Mock(url='https://127.0.0.1:443'), Mock(), {},
                    prepared, bundle_dir, phase_id=prepared['phase_id'], paperload_binary=binary_file)
            self.assertIn('not_int', str(ctx.exception).lower())

            # 5. Negative count value
            smoke, FakeFixture, collector = self._setup_legacy_finish_env(root, prepared)
            smoke.paper_p1_count_db.return_value = {'ledger': 24, 'raw': -100, 'temporal': 24, 'configuration': 0}
            with self.assertRaises(ValueError) as ctx:
                e2.finish_remote_collection(
                    smoke, FakeFixture, Mock(url='https://127.0.0.1:443'), Mock(), {},
                    prepared, bundle_dir, phase_id=prepared['phase_id'], paperload_binary=binary_file)
            self.assertIn('negative', str(ctx.exception).lower())

            # 6. Counter reset (after < before)
            smoke, FakeFixture, collector = self._setup_legacy_finish_env(root, prepared)
            prepared['before_global'] = {'ledger': 100, 'raw': 0, 'temporal': 0, 'configuration': 0}
            smoke.paper_p1_count_db.return_value = {'ledger': 24, 'raw': 2400, 'temporal': 24, 'configuration': 0}
            with self.assertRaises(ValueError) as ctx:
                e2.finish_remote_collection(
                    smoke, FakeFixture, Mock(url='https://127.0.0.1:443'), Mock(), {},
                    prepared, bundle_dir, phase_id=prepared['phase_id'], paperload_binary=binary_file)
            self.assertIn('reset', str(ctx.exception).lower())

    def _create_test_legacy_bundle(self, root, samples_per_message=100):
        session_id = 'sess-legacy-e2'
        run_id = 'run-legacy-e2'
        arm = 'A'
        phase_id = f'samples-{samples_per_message}'
        profile = f'S{samples_per_message}'
        bundle_dir = root / 'results'
        bundle_dir.mkdir(parents=True, mode=0o700)
        (bundle_dir / 'results' / 'workloads' / 'A').mkdir(parents=True, mode=0o700)
        (bundle_dir / 'results' / 'workloads' / 'B').mkdir(parents=True, mode=0o700)
        (bundle_dir / 'results' / 'runner').mkdir(parents=True, mode=0o700)

        binary_file = root / 'paperload'
        binary_file.write_bytes(b'fake_binary_content_for_legacy_testing')
        binary_file.chmod(0o700)
        bin_hash = hashlib.sha256(b'fake_binary_content_for_legacy_testing').hexdigest()

        def make_legacy_workload(role):
            expected_m = 8 if role == 'A' else 16
            gw = f'gw_{role.lower()}'
            s = f's_{role.lower()}'
            identity_samples = []
            for i in range(min(expected_m, 20)):
                mid = f'550e8400-e29b-41d4-a716-4466554400{i:02d}'
                identity_samples.append({
                    'viewer_id': '',
                    'gateway_id': gw,
                    'sensor_id': s,
                    'message_id': mid,
                    'boot_id': 'boot-1',
                    'first_sequence': str(i * samples_per_message),
                    'sample_count': samples_per_message,
                    'header_sha256': 'a' * 64,
                })
            return {
                'schema_version': 1,
                'status': 'COMPLETE',
                'implementation_status': 'P8.4b telemetry synthetic-load; no durable restart assurance',
                'experiment': 'E2',
                'arm': role,
                'profile': profile,
                'mode': 'remote',
                'run_id': run_id,
                'durability_mode': 'synthetic-load',
                'role': role,
                'gateway_id': gw,
                'sensor_id': s,
                'started_at': '2026-10-31T12:00:00.000000000Z',
                'finished_at': '2026-10-31T12:01:00.000000000Z',
                'configured_phases': {'warmup_seconds': 0, 'measure_seconds': 60, 'drain_seconds': 20, 'cooldown_seconds': 0},
                'telemetry': {
                    'scheduled_opportunities': expected_m,
                    'generated_unique': expected_m,
                    'offered_unique': expected_m,
                    'application_acks': expected_m,
                    'pending_messages': 0,
                    'pending_bytes': 0,
                    'publish_attempts': expected_m,
                    'broker_pubacks': expected_m,
                    'retries': 0,
                    'invalid_acks': 0,
                },
                'identity_sampling_qualification': 'header_sha256_only; maximum 20 globally',
                'identity_samples': identity_samples,
            }

        workload_a = make_legacy_workload('A')
        workload_b = make_legacy_workload('B')

        run_json = {
            'status': 'COMPLETE',
            'phase_observations': {
                'status': 'observed',
                'events': [
                    {'child': 'paperload-telemetry:A.manifest.json', 'event': 'measure_begin', 'elapsed_ns': 100, 'observed_at_runner_monotonic_ns': 1000},
                    {'child': 'paperload-telemetry:A.manifest.json', 'event': 'measure_end', 'elapsed_ns': 60000000000, 'observed_at_runner_monotonic_ns': 61000},
                    {'child': 'paperload-telemetry:B.manifest.json', 'event': 'burst_begin', 'elapsed_ns': 200, 'observed_at_runner_monotonic_ns': 2000},
                    {'child': 'paperload-telemetry:B.manifest.json', 'event': 'burst_end', 'elapsed_ns': 50000000000, 'observed_at_runner_monotonic_ns': 51000},
                ]
            }
        }
        prov_json = {
            'binary_sha256': bin_hash,
            'binary_os': sys.platform,
            'binary_arch': platform.machine()
        }

        files_data = {
            'results/workloads/A/workload.json': json.dumps(workload_a, separators=(',', ':')).encode(),
            'results/workloads/B/workload.json': json.dumps(workload_b, separators=(',', ':')).encode(),
            'results/runner/run.json': json.dumps(run_json, separators=(',', ':')).encode(),
            'results/runner/provenance.json': json.dumps(prov_json, separators=(',', ':')).encode(),
        }
        inventory = []
        for rel_path, data in files_data.items():
            f_path = bundle_dir / rel_path
            f_path.write_bytes(data)
            f_path.chmod(0o600)
            inventory.append({
                'path': rel_path,
                'size_bytes': len(data),
                'sha256': hashlib.sha256(data).hexdigest()
            })

        bundle_sha256 = 'b' * 64
        bundle_inventory = [{'path': 'input.json', 'sha256': 'a' * 64}]
        manifest_a_hash = '1' * 64
        manifest_b_hash = '2' * 64
        run_spec_hash = '3' * 64

        results_summary = {
            'schema_version': 1,
            'session': {
                'session_id': session_id,
                'run_ids': [run_id],
                'experiment': 'E2',
                'arm': arm,
                'phase': phase_id
            },
            'source_bundle_sha256': bundle_sha256,
            'source_inventory': bundle_inventory,
            'bound_manifest_sha256': {'A': manifest_a_hash, 'B': manifest_b_hash},
            'bound_gateway_environment_sha256': {},
            'bound_run_spec_sha256': run_spec_hash,
            'binary': dict(prov_json),
            'files': inventory,
            'runner_status': 'COMPLETE',
            'expected_workload_roles': ['A', 'B'],
            'present_workload_roles': ['A', 'B'],
            'missing_workload_roles': [],
            'full_p8_accepted': False,
            'physical_ab': 'NOT RUN'
        }

        summary_file = bundle_dir / 'results.json'
        summary_file.write_text(json.dumps(results_summary))
        summary_file.chmod(0o600)

        prepared = {
            'session_id': session_id,
            'run_id': run_id,
            'arm': arm,
            'phase_id': phase_id,
            'profile': profile,
            'workload': 'steady_burst',
            'samples_per_message': samples_per_message,
            'bundle_sha256': bundle_sha256,
            'bundle_inventory': bundle_inventory,
            'bound_manifest_sha256': {'A': manifest_a_hash, 'B': manifest_b_hash},
        }
        return bundle_dir, binary_file, prepared

    def _setup_legacy_finish_env(self, root, prepared):
        specs = [
            SimpleNamespace(gateway_id='gw_a', sensor_id='s_a', sensor_urn='urn:ngsi-ld:Sensor:gw_a:s_a'),
            SimpleNamespace(gateway_id='gw_b', sensor_id='s_b', sensor_urn='urn:ngsi-ld:Sensor:gw_b:s_b'),
        ]
        prepared['specs'] = specs
        prepared['pg'] = 'fake-pg'
        prepared['tokens'] = {'admin': 'admin-tok'}
        prepared['before_global'] = {'ledger': 0, 'raw': 0, 'temporal': 0, 'configuration': 0}
        prepared['before_targets'] = [
            {'ledger': 0, 'raw': 0, 'temporal': 0, 'configuration': 0},
            {'ledger': 0, 'raw': 0, 'temporal': 0, 'configuration': 0},
        ]
        ingress_fields = (
            'Received', 'ReceivedBytes', 'Accepted', 'AcceptedBytes',
            'RejectedNotRunning', 'RejectedStopping', 'RejectedRetained',
            'RejectedOversized', 'RejectedInvalidTopic', 'QueueFull',
            'ByteBudgetFull', 'ShaperPassed', 'ShaperRateLimited',
            'ShaperStateCapacity', 'Processed', 'ValidationRejected',
            'PersistFailures', 'CommitUnconfirmed', 'IdentityMismatch',
            'ACKBuildFailures', 'Committed', 'DuplicateCommitted',
            'SamplesNew', 'ACKPublished', 'ACKPublishFailures'
        )
        base_ing = {f: 0 for f in ingress_fields}
        term_ing = {f: 0 for f in ingress_fields}
        term_ing.update({
            'Received': 24, 'Accepted': 24, 'Committed': 24,
            'ACKPublished': 24, 'SamplesNew': 2400,
            'ShaperRateLimited': 27,
        })
        prepared['baseline'] = {'process_instance': 'proc-1', 'ingress': base_ing}

        class FakeCollector:
            def __init__(self):
                self.stopped = False
                self.errors = []
                self.metrics = []
                self.resources = []
            def stop(self, _):
                self.stopped = True

        collector = FakeCollector()
        prepared['collector'] = collector

        class FakeFixture:
            artifacts = root / 'artifacts'
        FakeFixture.artifacts.mkdir(mode=0o700, parents=True, exist_ok=True)

        smoke = Mock()
        smoke.ROOT = str(root)
        smoke.durable_json = lambda p, d: Path(p).write_text(json.dumps(d))
        smoke.http_request.return_value = (200, {}, {
            'process_instance': 'proc-1',
            'ingress': term_ing,
        })
        smoke.paper_p0_stats.return_value = {'cpu': 5}
        smoke.paper_p1_count_db.return_value = {'ledger': 24, 'raw': 2400, 'temporal': 24, 'configuration': 0}

        def fake_psql(reg, pg, sql, params=None):
            if 'FROM processed_messages WHERE gateway_id=:' in sql or 'SELECT count(*) FROM processed_messages WHERE gateway_id=:' in sql:
                if params.get('gw') == 'gw_a':
                    return json.dumps({'ledger': 8, 'raw': 800, 'temporal': 8, 'configuration': 0})
                return json.dumps({'ledger': 16, 'raw': 1600, 'temporal': 16, 'configuration': 0})
            if 'FROM processed_messages p WHERE' in sql and 'coalesce(json_agg' in sql:
                count = 8 if params.get('gw') == 'gw_a' else 16
                rows = [{'message_id': f'550e8400-e29b-41d4-a716-4466554400{i:02d}',
                         'first_sequence': str(i * 100), 'last_sequence': str((i + 1) * 100 - 1),
                         'sample_count': 100} for i in range(count)]
                return json.dumps(rows)
            if 'FROM processed_messages p WHERE' in sql and 'sample_count <>' in sql:
                return '0'
            return json.dumps({'ledger': 8, 'raw': 800, 'temporal': 8, 'configuration': 0})

        smoke.db_psql.side_effect = fake_psql
        return smoke, FakeFixture, collector

    @staticmethod
    def _sample_generator_resources():
        return {
            'schema_version': 1,
            'role': 'generator_B',
            'cpu_normalization': 'host_percent',
            'cpu_normalization_description': 'ticks_delta / CLK_TCK / interval_seconds / host_cpus * 100',
            'host_cpus': 4,
            'clk_tck': 100,
            'scopes': {
                'process_tree_scope': 'owned_children_capped_at_32',
                'fd_scope': 'root_process_only',
                'rss_scope': 'sum_sampled_RSS_may_double_count_shared_pages_not_peak',
                'cpu_scope': 'monotonic_process_tree_ticks_delta_same_keys_only',
            },
            'units': {
                'rss_bytes': 'bytes (sum_sampled_RSS_may_double_count_shared_pages)',
                'cpu_percent': 'percent',
                'elapsed_seconds': 'seconds',
                'interval_seconds': 'seconds',
                'delta_ticks': 'ticks',
                'process_count': 'count',
                'current_fd_count': 'count (root_process_only)',
            },
            'clock': {
                'monotonic_start': 500.0,
                'sample_interval_seconds': 1.0,
                'deadline_seconds': 120.0,
                'max_snapshots': 128,
            },
            'summary': {
                'total_samples': 3,
                'available_samples': 3,
                'unavailable_samples': 0,
                'cpu_available_samples': 2,
                'max_sample_rss_bytes': 35000000,
                'max_sample_rss_qualification': 'sum_sampled_RSS_may_double_count_shared_pages_not_peak',
                'fd_scope': 'root_process_only',
                'missing_reasons': [],
                'errors': [],
            },
            'samples': [
                {
                    'sample_index': 0,
                    'timestamp': '2026-01-01T12:00:00Z',
                    'elapsed_seconds': 0.0,
                    'interval_seconds': None,
                    'available': True,
                    'cpu_percent': None,
                    'delta_ticks': None,
                    'rss_bytes': 30000000,
                    'process_count': 3,
                    'current_fd_count': 12,
                    'reason': None,
                },
                {
                    'sample_index': 1,
                    'timestamp': '2026-01-01T12:00:01Z',
                    'elapsed_seconds': 1.0,
                    'interval_seconds': 1.0,
                    'available': True,
                    'cpu_percent': 25.0,
                    'delta_ticks': 100,
                    'rss_bytes': 35000000,
                    'process_count': 3,
                    'current_fd_count': 12,
                    'reason': None,
                },
                {
                    'sample_index': 2,
                    'timestamp': '2026-01-01T12:00:02Z',
                    'elapsed_seconds': 2.0,
                    'interval_seconds': 1.0,
                    'available': True,
                    'cpu_percent': 25.0,
                    'delta_ticks': 100,
                    'rss_bytes': 34000000,
                    'process_count': 3,
                    'current_fd_count': 12,
                    'reason': None,
                },
            ],
        }


class GeneratorResourceQualificationTests(unittest.TestCase):
    """Test suite for generator resource qualification, clock origin alignment, and interval validation."""

    def test_qualify_generator_resources_shifted_origin(self):
        """Host B monotonic alignment correctly computes active window despite shifted generator clock origin.

        Child-local Go elapsed_ns (e.g. 0.005s) must NEVER be used directly;
        observed_at_runner_monotonic_ns / 1e9 - clock.monotonic_start gives true generator elapsed timeline.
        """
        # Generator sampler started at monotonic 980.0
        gen_data = {
            'schema_version': 1,
            'role': 'generator_B',
            'clk_tck': 100,
            'clock': {
                'monotonic_start': 980.0,
                'sample_interval_seconds': 1.0,
                'deadline_seconds': 120.0,
                'max_snapshots': 128,
            },
            'samples': [
                {
                    'sample_index': 0,
                    'elapsed_seconds': 0.0,
                    'interval_seconds': None,
                    'available': True,
                    'delta_ticks': None,
                    'reason': 'first_sample_baseline_unavailable',
                    'rss_bytes': 30000000,
                }
            ] + [
                {
                    'sample_index': i,
                    'elapsed_seconds': float(i),
                    'interval_seconds': 1.0,
                    'available': True,
                    'delta_ticks': 100,
                    'reason': None,
                    'rss_bytes': 35000000,
                }
                for i in range(1, 85)
            ]
        }

        # Runner events on Host B monotonic timeline:
        # measure_begin at 1000.5s -> active_start = 1000.5 - 980.0 = 20.5s
        # measure_end at 1060.5s -> active_end = 1060.5 - 980.0 = 80.5s
        events = [
            {
                'child': 'paperload-telemetry:B.json',
                'event': 'measure_begin',
                'elapsed_ns': 5000000,  # 0.005s process-local child elapsed
                'observed_at_runner_monotonic_ns': 1000500000000,
                'clock_qualification': 'runner observed pipe receipt; observation lag unknown; child elapsed is process-local',
            },
            {
                'child': 'paperload-telemetry:B.json',
                'event': 'measure_end',
                'elapsed_ns': 60005000000,  # 60.005s process-local child elapsed
                'observed_at_runner_monotonic_ns': 1060500000000,
                'clock_qualification': 'runner observed pipe receipt; observation lag unknown; child elapsed is process-local',
            },
        ]

        res = e2.qualify_generator_resources(gen_data, events)
        self.assertEqual(res['status'], 'AVAILABLE')
        self.assertTrue(res['active_cpu_available'])
        self.assertAlmostEqual(res['active_window']['start_seconds'], 20.5)
        self.assertAlmostEqual(res['active_window']['end_seconds'], 80.5)
        self.assertTrue(res['active_window']['aligned_on_host_b_monotonic'])
        self.assertIn('pipe_receipt_lag_unknown', res['clock_qualification'])
        self.assertIn('aligned on host B monotonic timeline', res['clock_qualification'])
        # Samples with 20.5 <= elapsed_seconds <= 80.5 are samples 21 through 80 = 60 samples
        # 60 * 100 delta_ticks / 100 clk_tck = 60.0 seconds
        self.assertAlmostEqual(res['active_cpu_seconds'], 60.0)

    def test_qualify_generator_resources_invalid_interval_and_gap(self):
        """Invalid intervals, gaps, and process changes fail closed to INCONCLUSIVE."""
        base_clock = {
            'monotonic_start': 100.0,
            'sample_interval_seconds': 1.0,
            'deadline_seconds': 120.0,
            'max_snapshots': 128,
        }

        # 1. Reversed interval: begin > end
        events_rev = [
            {'child': 'paperload-telemetry:B.json', 'event': 'measure_begin', 'observed_at_runner_monotonic_ns': 160000000000},
            {'child': 'paperload-telemetry:B.json', 'event': 'measure_end', 'observed_at_runner_monotonic_ns': 120000000000},
        ]
        gen_data = {
            'schema_version': 1, 'role': 'generator_B', 'clk_tck': 100,
            'clock': base_clock,
            'samples': [{'sample_index': 0, 'elapsed_seconds': 0.0, 'available': True, 'delta_ticks': None}],
        }
        res = e2.qualify_generator_resources(gen_data, events_rev)
        self.assertEqual(res['status'], 'INCONCLUSIVE')
        self.assertFalse(res['active_cpu_available'])
        self.assertEqual(res['active_cpu_unavailable_reason'], 'active_window_reversed_or_empty')

        # 2. Negative start
        events_neg = [
            {'child': 'paperload-telemetry:B.json', 'event': 'measure_begin', 'observed_at_runner_monotonic_ns': 90000000000},
            {'child': 'paperload-telemetry:B.json', 'event': 'measure_end', 'observed_at_runner_monotonic_ns': 150000000000},
        ]
        res = e2.qualify_generator_resources(gen_data, events_neg)
        self.assertEqual(res['status'], 'INCONCLUSIVE')
        self.assertFalse(res['active_cpu_available'])
        self.assertTrue(res['active_cpu_unavailable_reason'].startswith('active_start_negative'))

        # 3. Window start not bracketed
        events_valid = [
            {'child': 'paperload-telemetry:B.json', 'event': 'measure_begin', 'observed_at_runner_monotonic_ns': 110000000000},
            {'child': 'paperload-telemetry:B.json', 'event': 'measure_end', 'observed_at_runner_monotonic_ns': 150000000000},
        ]
        # First sample is at 15.0s, which is after active_start (10.0s)
        gen_unbracketed_start = {
            'schema_version': 1, 'role': 'generator_B', 'clk_tck': 100,
            'clock': base_clock,
            'samples': [
                {'sample_index': 0, 'elapsed_seconds': 15.0, 'available': True, 'delta_ticks': None},
                {'sample_index': 1, 'elapsed_seconds': 60.0, 'available': True, 'delta_ticks': 100},
            ],
        }
        res = e2.qualify_generator_resources(gen_unbracketed_start, events_valid)
        self.assertEqual(res['status'], 'INCONCLUSIVE')
        self.assertFalse(res['active_cpu_available'])
        self.assertEqual(res['active_cpu_unavailable_reason'], 'active_window_start_not_bracketed')

        # 4. Window end not bracketed
        # Last sample is at 40.0s, which is before active_end (50.0s)
        gen_unbracketed_end = {
            'schema_version': 1, 'role': 'generator_B', 'clk_tck': 100,
            'clock': base_clock,
            'samples': [
                {'sample_index': 0, 'elapsed_seconds': 0.0, 'available': True, 'delta_ticks': None},
                {'sample_index': 1, 'elapsed_seconds': 40.0, 'available': True, 'delta_ticks': 100},
            ],
        }
        res = e2.qualify_generator_resources(gen_unbracketed_end, events_valid)
        self.assertEqual(res['status'], 'INCONCLUSIVE')
        self.assertFalse(res['active_cpu_available'])
        self.assertEqual(res['active_cpu_unavailable_reason'], 'active_window_end_not_bracketed')

        # 5. Sampling gap within active window
        gen_gap = {
            'schema_version': 1, 'role': 'generator_B', 'clk_tck': 100,
            'clock': base_clock,
            'samples': [
                {'sample_index': 0, 'elapsed_seconds': 0.0, 'available': True, 'delta_ticks': None},
                {'sample_index': 1, 'elapsed_seconds': 12.0, 'interval_seconds': 12.0, 'available': True, 'delta_ticks': 100, 'reason': None},
                {'sample_index': 2, 'elapsed_seconds': 55.0, 'interval_seconds': 43.0, 'available': True, 'delta_ticks': 100, 'reason': None},
            ],
        }
        res = e2.qualify_generator_resources(gen_gap, events_valid)
        self.assertEqual(res['status'], 'INCONCLUSIVE')
        self.assertFalse(res['active_cpu_available'])
        self.assertTrue(res['active_cpu_unavailable_reason'].startswith('sampling_gap_within_active_window'))

        # 6. Process change in active window (process_appeared)
        gen_proc_appeared = {
            'schema_version': 1, 'role': 'generator_B', 'clk_tck': 100,
            'clock': base_clock,
            'samples': [
                {'sample_index': 0, 'elapsed_seconds': 0.0, 'available': True, 'delta_ticks': None, 'reason': 'first_sample_baseline_unavailable'},
                {'sample_index': 1, 'elapsed_seconds': 20.0, 'interval_seconds': 1.0, 'available': True, 'delta_ticks': None, 'reason': 'process_appeared'},
                {'sample_index': 2, 'elapsed_seconds': 55.0, 'interval_seconds': 1.0, 'available': True, 'delta_ticks': 100, 'reason': None},
            ],
        }
        res = e2.qualify_generator_resources(gen_proc_appeared, events_valid)
        self.assertEqual(res['status'], 'INCONCLUSIVE')
        self.assertFalse(res['active_cpu_available'])
        self.assertEqual(res['active_cpu_unavailable_reason'], 'invalid_delta_ticks_in_active_window:process_appeared')

    def test_qualify_generator_resources_max_120s_boundary(self):
        """Deadlines, samples, or active windows exceeding 120s max boundary fail closed."""
        events_120 = [
            {'child': 'paperload-telemetry:B.json', 'event': 'measure_begin', 'observed_at_runner_monotonic_ns': 100000000000},
            {'child': 'paperload-telemetry:B.json', 'event': 'measure_end', 'observed_at_runner_monotonic_ns': 225000000000},
        ]
        # 1. Deadline exceeds 120s
        gen_bad_deadline = {
            'schema_version': 1, 'role': 'generator_B', 'clk_tck': 100,
            'clock': {'monotonic_start': 100.0, 'sample_interval_seconds': 1.0, 'deadline_seconds': 125.0, 'max_snapshots': 128},
            'samples': [{'sample_index': 0, 'elapsed_seconds': 0.0, 'available': True, 'delta_ticks': None}],
        }
        res = e2.qualify_generator_resources(gen_bad_deadline, events_120)
        self.assertEqual(res['status'], 'INCONCLUSIVE')
        self.assertTrue('max_120s_boundary_exceeded' in res['active_cpu_unavailable_reason'])

        # 2. Active window exceeds 120s (active_end = 225 - 100 = 125s)
        gen_window_125 = {
            'schema_version': 1, 'role': 'generator_B', 'clk_tck': 100,
            'clock': {'monotonic_start': 100.0, 'sample_interval_seconds': 1.0, 'deadline_seconds': 120.0, 'max_snapshots': 128},
            'samples': [
                {'sample_index': 0, 'elapsed_seconds': 0.0, 'available': True, 'delta_ticks': None},
                {'sample_index': 1, 'elapsed_seconds': 125.0, 'available': True, 'delta_ticks': 100},
            ],
        }
        res = e2.qualify_generator_resources(gen_window_125, events_120)
        self.assertEqual(res['status'], 'INCONCLUSIVE')
        self.assertTrue('max_120s_boundary_exceeded' in res['active_cpu_unavailable_reason'])

        # 3. Sample elapsed exceeds 120s
        events_ok = [
            {'child': 'paperload-telemetry:B.json', 'event': 'measure_begin', 'observed_at_runner_monotonic_ns': 110000000000},
            {'child': 'paperload-telemetry:B.json', 'event': 'measure_end', 'observed_at_runner_monotonic_ns': 150000000000},
        ]
        gen_sample_121 = {
            'schema_version': 1, 'role': 'generator_B', 'clk_tck': 100,
            'clock': {'monotonic_start': 100.0, 'sample_interval_seconds': 1.0, 'deadline_seconds': 120.0, 'max_snapshots': 128},
            'samples': [
                {'sample_index': 0, 'elapsed_seconds': 0.0, 'available': True, 'delta_ticks': None},
                {'sample_index': 1, 'elapsed_seconds': 121.0, 'available': True, 'delta_ticks': 100},
            ],
        }
        res = e2.qualify_generator_resources(gen_sample_121, events_ok)
        self.assertEqual(res['status'], 'INCONCLUSIVE')
        self.assertTrue('max_120s_boundary_exceeded' in res['active_cpu_unavailable_reason'])

    def test_qualify_generator_resources_complete_valid_interval(self):
        """Complete valid intervals compute CPU; CPU=0 is valid only with complete evidence; partial observed CPU labeled."""
        base_clock = {
            'monotonic_start': 500.0,
            'sample_interval_seconds': 1.0,
            'deadline_seconds': 120.0,
            'max_snapshots': 128,
        }
        events = [
            {'child': 'paperload-telemetry:B.json', 'event': 'measure_begin', 'observed_at_runner_monotonic_ns': 510000000000},  # 10.0s
            {'child': 'paperload-telemetry:B.json', 'event': 'measure_end', 'observed_at_runner_monotonic_ns': 530000000000},    # 30.0s
        ]

        # 1. Complete valid interval with CPU > 0
        samples_valid = [
            {'sample_index': 0, 'elapsed_seconds': 0.0, 'interval_seconds': None, 'available': True, 'delta_ticks': None, 'reason': 'first_sample_baseline_unavailable'},
        ] + [
            {'sample_index': i, 'elapsed_seconds': float(i), 'interval_seconds': 1.0, 'available': True, 'delta_ticks': 50, 'reason': None}
            for i in range(1, 35)
        ]
        gen_data_valid = {
            'schema_version': 1, 'role': 'generator_B', 'clk_tck': 100,
            'clock': base_clock,
            'samples': samples_valid,
        }
        res = e2.qualify_generator_resources(gen_data_valid, events)
        self.assertEqual(res['status'], 'AVAILABLE')
        self.assertTrue(res['active_cpu_available'])
        # Active window [10.0, 30.0]: samples 11..30 (covering 10.0 to 30.0s = 20s) = 20 samples * 50 ticks / 100 = 10.0s
        self.assertAlmostEqual(res['active_cpu_seconds'], 10.0)
        self.assertTrue(res['lifecycle_cpu_available'])
        # Lifecycle samples 1..34 = 34 samples * 50 ticks / 100 = 17.0s
        self.assertAlmostEqual(res['lifecycle_cpu_seconds'], 17.0)
        self.assertAlmostEqual(res['partial_observed_lifecycle_cpu_seconds'], 17.0)

        # 2. CPU=0 valid only when evidence complete
        samples_zero = [
            {'sample_index': 0, 'elapsed_seconds': 0.0, 'interval_seconds': None, 'available': True, 'delta_ticks': None, 'reason': 'first_sample_baseline_unavailable'},
        ] + [
            {'sample_index': i, 'elapsed_seconds': float(i), 'interval_seconds': 1.0, 'available': True, 'delta_ticks': 0, 'reason': None}
            for i in range(1, 35)
        ]
        gen_data_zero = {
            'schema_version': 1, 'role': 'generator_B', 'clk_tck': 100,
            'clock': base_clock,
            'samples': samples_zero,
        }
        res_zero = e2.qualify_generator_resources(gen_data_zero, events)
        self.assertEqual(res_zero['status'], 'AVAILABLE')
        self.assertTrue(res_zero['active_cpu_available'])
        self.assertEqual(res_zero['active_cpu_seconds'], 0.0)
        self.assertTrue(res_zero['lifecycle_cpu_available'])
        self.assertEqual(res_zero['lifecycle_cpu_seconds'], 0.0)

        # 3. Partial observed lifecycle CPU when startup process change occurs
        samples_partial = [
            {'sample_index': 0, 'elapsed_seconds': 0.0, 'interval_seconds': None, 'available': True, 'delta_ticks': None, 'reason': 'first_sample_baseline_unavailable'},
            {'sample_index': 1, 'elapsed_seconds': 1.0, 'interval_seconds': 1.0, 'available': True, 'delta_ticks': None, 'reason': 'process_appeared'},
        ] + [
            {'sample_index': i, 'elapsed_seconds': float(i), 'interval_seconds': 1.0, 'available': True, 'delta_ticks': 100, 'reason': None}
            for i in range(2, 35)
        ]
        gen_data_partial = {
            'schema_version': 1, 'role': 'generator_B', 'clk_tck': 100,
            'clock': base_clock,
            'samples': samples_partial,
        }
        res_partial = e2.qualify_generator_resources(gen_data_partial, events)
        # Sample 1 (outside active window [10.0, 30.0]) has process_appeared, so full lifecycle CPU is unavailable
        self.assertFalse(res_partial['lifecycle_cpu_available'])
        self.assertIsNone(res_partial['lifecycle_cpu_seconds'])
        self.assertEqual(res_partial['lifecycle_cpu_unavailable_reason'], 'process_change_samples_lack_delta:process_appeared')
        # Partial observed lifecycle CPU is sum of available deltas: 33 samples * 100 / 100 = 33.0s
        self.assertAlmostEqual(res_partial['partial_observed_lifecycle_cpu_seconds'], 33.0)
        # But active window [10.0, 30.0] has all valid samples (index 11..30 = 20 samples), so active CPU IS available
        self.assertTrue(res_partial['active_cpu_available'])
        self.assertAlmostEqual(res_partial['active_cpu_seconds'], 20.0)






if __name__ == '__main__':
    unittest.main()
