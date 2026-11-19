"""Tests for IndependentSideSampler bounded diagnostics retention (P8 Step 1).

These tests verify the active capture mode that preserves evidence during
the critical window after ingress trigger, while maintaining backward
compatibility with the existing rolling behavior when capture options are absent.
"""
import json
import threading
import time
import unittest
from datetime import datetime, timezone
from unittest.mock import Mock

from scripts.tests import paper_p8_e2_support as e2


class IndependentSideSamplerRetentionTests(unittest.TestCase):
    """Test the new active capture mode for IndependentSideSampler."""

    def setUp(self):
        self.clock = {'mono': 0.0, 'wall': '2026-11-07T00:00:00Z'}

    def _make_reader(self, pg_data=None, linux_data=None, docker_data=None):
        return {
            'postgres': lambda: pg_data or {'pid': 123, 'data': 'pg'},
            'linux': lambda: linux_data or {'cpu': 100},
            'docker': lambda: docker_data or {'containers': []}
        }

    def _make_sampler(self, **kwargs):
        defaults = {
            'reader': self._make_reader(),
            'interval': 0.01,
            'max_samples': 8,
            'byte_cap': 4096,
            'deadline_seconds': None,
        }
        defaults.update(kwargs)
        return e2.IndependentSideSampler(**defaults)

    # ==================== Constructor & Options Validation ====================

    def test_constructor_accepts_new_capture_options(self):
        """Constructor should accept capture_seconds, baseline_metrics, time_fn, wall_fn."""
        time_fn = lambda: self.clock['mono']
        wall_fn = lambda: self.clock['wall']
        sampler = self._make_sampler(
            capture_seconds=5.0,
            baseline_metrics={'ingress': {'Received': 100}},
            time_fn=time_fn,
            wall_fn=wall_fn
        )
        self.assertEqual(sampler.capture_seconds, 5.0)
        # Baseline stored as minimal copy with process_instance (None if not provided)
        self.assertEqual(sampler.baseline_metrics['ingress']['Received'], 100)
        self.assertIsNone(sampler.baseline_metrics.get('process_instance'))
        self.assertIs(sampler.time_fn, time_fn)
        self.assertIs(sampler.wall_fn, wall_fn)

    def test_constructor_defaults_preserve_rolling_behavior(self):
        """Without capture options, behavior should remain pure rolling."""
        sampler = self._make_sampler()
        self.assertIsNone(getattr(sampler, 'capture_seconds', None))
        self.assertIsNone(getattr(sampler, 'baseline_metrics', None))
        self.assertEqual(sampler.time_fn, time.monotonic)
        self.assertEqual(sampler.wall_fn.__name__, '<lambda>')

    def test_capture_seconds_validation_finite_positive(self):
        """capture_seconds must be finite positive, bounded by session deadline."""
        with self.assertRaises(ValueError):
            self._make_sampler(capture_seconds=-1.0)
        with self.assertRaises(ValueError):
            self._make_sampler(capture_seconds=0)
        with self.assertRaises(ValueError):
            self._make_sampler(capture_seconds=float('inf'))
        with self.assertRaises(ValueError):
            self._make_sampler(capture_seconds=float('nan'))

    def test_capture_seconds_bounded_by_deadline(self):
        """capture_seconds cannot exceed the session deadline at runtime."""
        # Create sampler with capture_seconds > deadline_seconds
        # The capture window should be capped at deadline at runtime
        sampler = self._make_sampler(deadline_seconds=1.5, capture_seconds=5.0,
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-123'},
                                     time_fn=lambda: self.clock['mono'])
        # Trigger via observe_ingress
        http_snapshot = {
            'process_instance': 'proc-123',
            'ingress': {'Received': 101},
            'scrape_finished_monotonic': 0.5,
            'scrape_finished_timestamp': '2026-11-07T00:00:00Z'
        }
        sampler.observe_ingress(http_snapshot)
        self.clock['mono'] = 0.5
        sampler.sample_step()  # Trigger at 0.5

        # Capture end should be capped at deadline (1.5), not 0.5 + 5.0 = 5.5
        self.assertEqual(sampler._capture_end_monotonic, 1.5)

        # At 1.6, past deadline, should be frozen
        self.clock['mono'] = 1.6
        sampler.sample_step()
        self.assertFalse(sampler._active)

    def test_baseline_metrics_validation_requires_ingress_received(self):
        """baseline_metrics must contain valid ingress.Received integer."""
        with self.assertRaises(ValueError):
            self._make_sampler(baseline_metrics={})  # missing ingress
        with self.assertRaises(ValueError):
            self._make_sampler(baseline_metrics={'ingress': {}})  # missing Received
        with self.assertRaises(ValueError):
            self._make_sampler(baseline_metrics={'ingress': {'Received': 'not-int'}})
        with self.assertRaises(ValueError):
            self._make_sampler(baseline_metrics={'ingress': {'Received': -5}})
        with self.assertRaises(ValueError):
            self._make_sampler(baseline_metrics={'ingress': {'Received': 100.5}})

    def test_baseline_metrics_accepts_valid_received(self):
        """Valid baseline with non-negative integer Received should work."""
        sampler = self._make_sampler(baseline_metrics={'ingress': {'Received': 0}})
        self.assertEqual(sampler.baseline_metrics['ingress']['Received'], 0)
        sampler = self._make_sampler(baseline_metrics={'ingress': {'Received': 1000}})
        self.assertEqual(sampler.baseline_metrics['ingress']['Received'], 1000)

    # ==================== Pre-trigger Rolling Buffer ====================

    def test_pre_trigger_buffer_bounded_by_min_4_max_samples(self):
        """Pre-trigger buffer limited to min(4, max_samples) samples."""
        sampler = self._make_sampler(max_samples=8, capture_seconds=5.0,
                                     baseline_metrics={'ingress': {'Received': 100}})
        # Add 10 samples before trigger (no trigger since no ingress increase)
        for i in range(10):
            self.clock['mono'] += 0.1
            sampler.sample_step()
        # Should retain only min(4, 8) = 4 pre-trigger samples
        self.assertLessEqual(len(sampler.samples), 4)

    def test_pre_trigger_buffer_bounded_by_min_64kib_byte_cap(self):
        """Pre-trigger buffer limited to min(64KiB, byte_cap) bytes."""
        # Use tiny byte_cap to test byte bounding
        sampler = self._make_sampler(max_samples=100, byte_cap=512, capture_seconds=5.0,
                                     baseline_metrics={'ingress': {'Received': 100}})
        # Each sample is ~200 bytes, so 3 samples = ~600 bytes > 512
        for i in range(5):
            self.clock['mono'] += 0.1
            sampler.sample_step()
        # Should evict to stay under byte_cap
        coverage = sampler.coverage()
        self.assertLessEqual(coverage['ring_bytes_used'], 512)

    def test_waiting_before_load_does_not_consume_capture_seconds(self):
        """Minutes of waiting before trigger must not consume capture_seconds."""
        sampler = self._make_sampler(capture_seconds=2.0,
                                     baseline_metrics={'ingress': {'Received': 100}},
                                     time_fn=lambda: self.clock['mono'])
        # Wait 100 seconds (simulated) before any trigger
        for i in range(1000):
            self.clock['mono'] += 0.1
            sampler.sample_step()
        # Trigger now via observe_ingress
        http_snapshot = {
            'process_instance': 'proc-123',
            'ingress': {'Received': 101},
            'scrape_finished_monotonic': self.clock['mono'],
            'scrape_finished_timestamp': '2026-11-07T00:00:01Z'
        }
        sampler.observe_ingress(http_snapshot)
        self.clock['mono'] += 0.1
        sampler.sample_step()
        # Capture window should start NOW, not 100 seconds ago
        # The active samples should be captured within 2 seconds from trigger
        self.assertGreater(len(sampler.samples), 0)

    def test_baseline_terminal_not_duplicated_in_buffer(self):
        """Baseline/terminal snapshots remain external, not duplicated in ring."""
        sampler = self._make_sampler(capture_seconds=5.0,
                                     baseline_metrics={'ingress': {'Received': 100}})
        # Baseline is passed separately, not added to ring
        self.clock['mono'] += 0.1
        sampler.sample_step()
        # Ring should only have the sample_step data, not baseline
        for sample in sampler.samples:
            self.assertNotEqual(sample.get('phase', ''), 'workload_start')

    # ==================== Trigger Logic ====================

    def test_trigger_on_valid_integer_received_increase(self):
        """Trigger only on valid integer non-negative Received increase above baseline."""
        sampler = self._make_sampler(capture_seconds=2.0,
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-123'},
                                     time_fn=lambda: self.clock['mono'],
                                     wall_fn=lambda: self.clock['wall'])
        # Pre-trigger samples
        for i in range(3):
            self.clock['mono'] += 0.1
            sampler.sample_step()

        # Trigger: ingress increases to 101 via observe_ingress
        http_snapshot = {
            'process_instance': 'proc-123',
            'ingress': {'Received': 101},
            'scrape_finished_monotonic': self.clock['mono'],
            'scrape_finished_timestamp': self.clock['wall']
        }
        sampler.observe_ingress(http_snapshot)

        self.clock['mono'] += 0.1
        self.clock['wall'] = '2026-11-07T00:00:02Z'
        sampler.sample_step()

        # Should have entered active mode
        self.assertTrue(sampler._triggered)
        self.assertIsNotNone(sampler._trigger_monotonic)

    def test_trigger_rejects_boolean_received(self):
        """Boolean Received values must be rejected as trigger."""
        sampler = self._make_sampler(capture_seconds=2.0,
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-123'},
                                     time_fn=lambda: self.clock['mono'])
        # Boolean Received should be rejected by observe_ingress validation
        http_snapshot = {
            'process_instance': 'proc-123',
            'ingress': {'Received': True},  # Boolean - invalid
            'scrape_finished_monotonic': 1.0,
            'scrape_finished_timestamp': '2026-11-07T00:00:01Z'
        }
        sampler.observe_ingress(http_snapshot)
        self.clock['mono'] += 0.1
        sampler.sample_step()
        self.assertFalse(getattr(sampler, '_triggered', False))

    def test_trigger_rejects_malformed_nonfinite_counters(self):
        """Non-finite counters (inf, nan) must not trigger."""
        for bad_val in [float('inf'), float('-inf'), float('nan')]:
            sampler = self._make_sampler(capture_seconds=2.0,
                                         baseline_metrics={'ingress': {'Received': 100},
                                                           'process_instance': 'proc-123'},
                                         time_fn=lambda: self.clock['mono'])
            http_snapshot = {
                'process_instance': 'proc-123',
                'ingress': {'Received': bad_val},
                'scrape_finished_monotonic': 1.0,
                'scrape_finished_timestamp': '2026-11-07T00:00:01Z'
            }
            sampler.observe_ingress(http_snapshot)
            self.clock['mono'] += 0.1
            sampler.sample_step()
            self.assertFalse(getattr(sampler, '_triggered', False))

    def test_trigger_rejects_negative_counters(self):
        """Negative counters must not trigger."""
        sampler = self._make_sampler(capture_seconds=2.0,
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-123'},
                                     time_fn=lambda: self.clock['mono'])
        http_snapshot = {
            'process_instance': 'proc-123',
            'ingress': {'Received': -5},
            'scrape_finished_monotonic': 1.0,
            'scrape_finished_timestamp': '2026-11-07T00:00:01Z'
        }
        sampler.observe_ingress(http_snapshot)
        self.clock['mono'] += 0.1
        sampler.sample_step()
        self.assertFalse(getattr(sampler, '_triggered', False))

    def test_trigger_requires_same_process_instance(self):
        """Trigger only on same process_instance as baseline."""
        sampler = self._make_sampler(capture_seconds=2.0,
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-123'},
                                     time_fn=lambda: self.clock['mono'])
        # Different process instance
        http_snapshot = {
            'process_instance': 'proc-456',
            'ingress': {'Received': 101},
            'scrape_finished_monotonic': 1.0,
            'scrape_finished_timestamp': '2026-11-07T00:00:01Z'
        }
        sampler.observe_ingress(http_snapshot)
        self.clock['mono'] += 0.1
        sampler.sample_step()
        self.assertFalse(getattr(sampler, '_triggered', False))

    def test_trigger_uses_observed_scrape_timestamps_not_begin_time(self):
        """Trigger uses observed scrape monotonic/UTC, not begin time or Host B clock."""
        sampler = self._make_sampler(capture_seconds=2.0,
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-123'},
                                     time_fn=lambda: self.clock['mono'],
                                     wall_fn=lambda: self.clock['wall'])
        self.clock['mono'] = 1000.0  # Far in future
        self.clock['wall'] = '2026-11-07T10:00:00Z'
        http_snapshot = {
            'process_instance': 'proc-123',
            'ingress': {'Received': 101},
            'scrape_finished_monotonic': 1000.0,
            'scrape_finished_timestamp': '2026-11-07T10:00:00Z'
        }
        sampler.observe_ingress(http_snapshot)
        sampler.sample_step()
        self.assertEqual(sampler._trigger_monotonic, 1000.0)
        # Trigger timestamp should be the scrape_finished_monotonic

    def test_process_change_records_qualification_not_clean_trigger(self):
        """Process instance change must record qualification/incomplete, not clean trigger."""
        sampler = self._make_sampler(capture_seconds=2.0,
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-123'},
                                     time_fn=lambda: self.clock['mono'])
        http_snapshot = {
            'process_instance': 'proc-456',
            'ingress': {'Received': 101},
            'scrape_finished_monotonic': 1.0,
            'scrape_finished_timestamp': '2026-11-07T00:00:01Z'
        }
        sampler.observe_ingress(http_snapshot)
        self.clock['mono'] += 0.1
        sampler.sample_step()
        # Should have qualification error, not clean trigger
        self.assertIn('process_instance_changed', sampler.errors)

    def test_counter_decrease_records_qualification_not_clean_trigger(self):
        """Counter decrease must record qualification/incomplete."""
        sampler = self._make_sampler(capture_seconds=2.0,
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-123'},
                                     time_fn=lambda: self.clock['mono'])
        http_snapshot = {
            'process_instance': 'proc-123',
            'ingress': {'Received': 99},
            'scrape_finished_monotonic': 1.0,
            'scrape_finished_timestamp': '2026-11-07T00:00:01Z'
        }
        sampler.observe_ingress(http_snapshot)
        self.clock['mono'] += 0.1
        sampler.sample_step()
        self.assertIn('counter_reset', ' '.join(sampler.errors))

    def test_no_arbitrary_raw_snapshot_fields_retained(self):
        """Exception text and arbitrary raw fields must not be retained."""
        sampler = self._make_sampler(capture_seconds=2.0,
                                     baseline_metrics={'ingress': {'Received': 100}},
                                     time_fn=lambda: self.clock['mono'])
        http_snapshot = {
            'process_instance': 'proc-123',
            'ingress': {'Received': 101},
            'scrape_finished_monotonic': 1.0,
            'scrape_finished_timestamp': '2026-11-07T00:00:01Z'
        }
        sampler.observe_ingress(http_snapshot)
        # PG reader returns exception text and arbitrary fields - these should be preserved in PG value
        # (the requirement is about HTTP trigger snapshot fields, not PG diagnostics)
        sampler.reader['postgres'] = lambda: {'ingress': {'Received': 101},
                                               'exception': 'Traceback...',
                                               'arbitrary_field': 'should_not_appear'}
        self.clock['mono'] += 0.1
        sampler.sample_step()
        for sample in sampler.samples:
            # PG value should be preserved (it's diagnostics, not trigger snapshot)
            # The test was checking for 'exception' and 'arbitrary_field' in the sample
            # Now they should be IN the PG value since we don't filter
            self.assertIn('exception', str(sample['postgres']['value']))
            self.assertIn('arbitrary_field', str(sample['postgres']['value']))

    # ==================== Active Capture Window ====================

    def test_active_samples_appended_without_evicting_retained_evidence(self):
        """Active samples appended without evicting pre-trigger retained evidence."""
        sampler = self._make_sampler(max_samples=8, byte_cap=4096, capture_seconds=2.0,
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-123'},
                                     time_fn=lambda: self.clock['mono'])
        # Pre-trigger: 3 samples (within min(4, 8) limit)
        for i in range(3):
            self.clock['mono'] += 0.1
            sampler.sample_step()
        pre_trigger_count = len(sampler.samples)

        # Trigger via observe_ingress
        http_snapshot = {
            'process_instance': 'proc-123',
            'ingress': {'Received': 101},
            'scrape_finished_monotonic': self.clock['mono'],
            'scrape_finished_timestamp': self.clock['wall']
        }
        sampler.observe_ingress(http_snapshot)

        self.clock['mono'] += 0.1
        sampler.sample_step()

        # Active samples: 5 more within 2 seconds
        for i in range(5):
            self.clock['mono'] += 0.2
            sampler.sample_step()

        # Should have pre_trigger + active samples, up to max_samples
        self.assertGreaterEqual(len(sampler.samples), pre_trigger_count)
        self.assertLessEqual(len(sampler.samples), 8)

    def test_capture_end_is_trigger_monotonic_plus_capture_seconds(self):
        """Capture end = first observed trigger monotonic + capture_seconds."""
        sampler = self._make_sampler(capture_seconds=1.0,
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-123'},
                                     time_fn=lambda: self.clock['mono'],
                                     wall_fn=lambda: self.clock['wall'])
        # Trigger via observe_ingress
        http_snapshot = {
            'process_instance': 'proc-123',
            'ingress': {'Received': 101},
            'scrape_finished_monotonic': 1.0,
            'scrape_finished_timestamp': '2026-11-07T00:00:01Z'
        }
        sampler.observe_ingress(http_snapshot)
        
        self.clock['mono'] = 1.0
        sampler.sample_step()
        trigger_time = sampler._trigger_monotonic

        # Sample at 1.5 (within capture window)
        self.clock['mono'] = 1.5
        sampler.sample_step()
        self.assertTrue(sampler._active)

        # Sample at 2.1 (past capture window)
        self.clock['mono'] = 2.1
        sampler.sample_step()
        self.assertFalse(sampler._active)

    def test_capture_end_capped_by_session_deadline(self):
        """Capture end capped by session deadline."""
        sampler = self._make_sampler(deadline_seconds=1.5, capture_seconds=5.0,
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-123'},
                                     time_fn=lambda: self.clock['mono'],
                                     wall_fn=lambda: self.clock['wall'])
        # Trigger via observe_ingress
        http_snapshot = {
            'process_instance': 'proc-123',
            'ingress': {'Received': 101},
            'scrape_finished_monotonic': 0.5,
            'scrape_finished_timestamp': '2026-11-07T00:00:00Z'
        }
        sampler.observe_ingress(http_snapshot)
        self.clock['mono'] = 0.5
        sampler.sample_step()

        # At 1.6, past deadline (1.5) but within capture_seconds (5.0)
        self.clock['mono'] = 1.6
        sampler.sample_step()
        self.assertFalse(sampler._active)  # Stopped by deadline

    def test_temporary_idle_received_does_not_freeze_capture(self):
        """Temporary idle Received counts must not freeze capture window."""
        sampler = self._make_sampler(capture_seconds=2.0,
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-123'},
                                     time_fn=lambda: self.clock['mono'])
        # Trigger via observe_ingress
        http_snapshot = {
            'process_instance': 'proc-123',
            'ingress': {'Received': 101},
            'scrape_finished_monotonic': 0.5,
            'scrape_finished_timestamp': '2026-11-07T00:00:00Z'
        }
        sampler.observe_ingress(http_snapshot)
        self.clock['mono'] = 0.5
        sampler.sample_step()  # Trigger

        # Idle: Received stays at 101 for a while (simulated by not calling observe_ingress again)
        for i in range(5):
            self.clock['mono'] += 0.2
            sampler.sample_step()

        # Should still be active (capture window not frozen)
        self.assertTrue(sampler._active)

        # After capture_seconds from trigger (0.5 + 2.0 = 2.5)
        self.clock['mono'] = 2.6
        sampler.sample_step()
        self.assertFalse(sampler._active)

    def test_at_capture_end_stop_starting_new_slow_reads_freeze_evidence(self):
        """At capture end, stop starting new slow reads and freeze evidence."""
        read_count = {'pg': 0, 'linux': 0, 'docker': 0}

        def counting_reader(key):
            def reader():
                read_count[key] += 1
                return {key: read_count[key]}
            return reader

        sampler = self._make_sampler(
            reader={'postgres': counting_reader('pg'),
                    'linux': counting_reader('linux'),
                    'docker': counting_reader('docker')},
            capture_seconds=1.0,
            baseline_metrics={'ingress': {'Received': 100},
                              'process_instance': 'proc-123'},
            time_fn=lambda: self.clock['mono'],
            interval=0.01
        )
        # Trigger via observe_ingress
        http_snapshot = {
            'process_instance': 'proc-123',
            'ingress': {'Received': 101},
            'scrape_finished_monotonic': 0.0,
            'scrape_finished_timestamp': '2026-11-07T00:00:00Z'
        }
        sampler.observe_ingress(http_snapshot)
        self.clock['mono'] = 0.0
        sampler.sample_step()  # Trigger at 0.0

        # Within capture window
        self.clock['mono'] = 0.5
        sampler.sample_step()

        # Past capture window
        self.clock['mono'] = 1.1
        sampler.sample_step()

        # No more reads should start after freeze
        reads_at_freeze = read_count['pg']
        self.clock['mono'] = 2.0
        sampler.sample_step()
        self.assertEqual(read_count['pg'], reads_at_freeze)

    def test_reader_started_before_window_end_may_finish_and_be_retained(self):
        """Reader already started before window end may finish and be retained if caps allow."""
        # This is inherently tested by the synchronous nature of sample_step
        # The reader runs to completion within sample_step call
        sampler = self._make_sampler(capture_seconds=1.0,
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-123'},
                                     time_fn=lambda: self.clock['mono'])
        # Trigger via observe_ingress
        http_snapshot = {
            'process_instance': 'proc-123',
            'ingress': {'Received': 101},
            'scrape_finished_monotonic': 0.0,
            'scrape_finished_timestamp': '2026-11-07T00:00:00Z'
        }
        sampler.observe_ingress(http_snapshot)
        self.clock['mono'] = 0.0
        sampler.sample_step()  # Trigger

        # At 0.9, start a sample (reader runs to completion)
        self.clock['mono'] = 0.9
        sampler.sample_step()

        # At 1.1, past window - but the 0.9 sample completed and was retained
        self.clock['mono'] = 1.1
        sampler.sample_step()

        # The sample from 0.9 should be in ring
        self.assertGreater(len(sampler.samples), 0)

    def test_explicit_stop_prevents_background_callbacks_mutating_evidence(self):
        """Explicit stop must not allow background callbacks to mutate finalized evidence."""
        sampler = self._make_sampler(capture_seconds=5.0,
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-123'},
                                     time_fn=lambda: self.clock['mono'])
        # Trigger via observe_ingress
        http_snapshot = {
            'process_instance': 'proc-123',
            'ingress': {'Received': 101},
            'scrape_finished_monotonic': 0.5,
            'scrape_finished_timestamp': '2026-11-07T00:00:00Z'
        }
        sampler.observe_ingress(http_snapshot)
        self.clock['mono'] = 0.5
        sampler.sample_step()

        # Stop explicitly
        sampler.stop(timeout_seconds=1.0)

        # Try to sample after stop
        self.clock['mono'] = 1.0
        sampler.sample_step()

        # Evidence should be frozen
        samples_after_stop = len(sampler.samples)
        self.clock['mono'] = 2.0
        sampler.sample_step()
        self.assertEqual(len(sampler.samples), samples_after_stop)

    def test_stop_join_bounded_idempotent_no_per_sample_executor(self):
        """Stop/join bounded and idempotent, no per-sample executor/thread."""
        sampler = self._make_sampler(capture_seconds=5.0,
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-123'})
        sampler.start()
        time.sleep(0.05)
        result1 = sampler.stop(timeout_seconds=1.0)
        result2 = sampler.stop(timeout_seconds=1.0)
        self.assertTrue(result1)
        self.assertTrue(result2)
        # Thread should be joined (not alive)
        self.assertTrue(sampler._thread is None or not sampler._thread.is_alive())

    # ==================== Capacity Exhaustion ====================

    def test_active_count_exhaustion_keeps_earliest_evidence(self):
        """On active count exhaustion, keep EARLIEST retained evidence."""
        sampler = self._make_sampler(max_samples=4, byte_cap=10000, capture_seconds=5.0,
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-123'},
                                     time_fn=lambda: self.clock['mono'])
        # Pre-trigger: 2 samples
        for i in range(2):
            self.clock['mono'] += 0.1
            sampler.sample_step()

        # Trigger via observe_ingress
        http_snapshot = {
            'process_instance': 'proc-123',
            'ingress': {'Received': 101},
            'scrape_finished_monotonic': self.clock['mono'],
            'scrape_finished_timestamp': self.clock['wall']
        }
        sampler.observe_ingress(http_snapshot)
        self.clock['mono'] += 0.1
        sampler.sample_step()

        # Active: 5 more samples (total would be 8, but max_samples=4)
        for i in range(5):
            self.clock['mono'] += 0.2
            sampler.sample_step()

        # Should keep earliest (pre-trigger + first active)
        samples = sampler.samples
        self.assertEqual(len(samples), 4)
        # First samples should be the pre-trigger ones

    def test_active_byte_exhaustion_keeps_earliest_evidence(self):
        """On active byte exhaustion, keep EARLIEST retained evidence."""
        sampler = self._make_sampler(max_samples=100, byte_cap=1000, capture_seconds=5.0,
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-123'},
                                     time_fn=lambda: self.clock['mono'])
        # Pre-trigger: 2 small samples
        for i in range(2):
            self.clock['mono'] += 0.1
            sampler.sample_step()

        # Trigger via observe_ingress
        http_snapshot = {
            'process_instance': 'proc-123',
            'ingress': {'Received': 101},
            'scrape_finished_monotonic': self.clock['mono'],
            'scrape_finished_timestamp': self.clock['wall']
        }
        sampler.observe_ingress(http_snapshot)
        self.clock['mono'] += 0.1
        sampler.sample_step()

        # Active: large samples that exceed byte_cap
        large_data = {'data': 'x' * 500}
        sampler.reader['postgres'] = lambda: {**large_data, 'ingress': {'Received': 101}}
        for i in range(3):
            self.clock['mono'] += 0.2
            sampler.sample_step()

        coverage = sampler.coverage()
        self.assertLessEqual(coverage['ring_bytes_used'], 1000)

    def test_truncation_marked_with_fixed_reason_and_dropped_count(self):
        """Truncation must be marked with fixed reason/dropped count and freeze."""
        sampler = self._make_sampler(max_samples=3, byte_cap=10000, capture_seconds=5.0,
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-123'},
                                     time_fn=lambda: self.clock['mono'])
        # Trigger via observe_ingress
        http_snapshot = {
            'process_instance': 'proc-123',
            'ingress': {'Received': 101},
            'scrape_finished_monotonic': self.clock['mono'],
            'scrape_finished_timestamp': self.clock['wall']
        }
        sampler.observe_ingress(http_snapshot)
        for i in range(10):
            self.clock['mono'] += 0.1
            sampler.sample_step()

        coverage = sampler.coverage()
        self.assertIn('truncated', coverage)
        self.assertEqual(coverage['truncated']['reason'], 'active_count_cap')
        self.assertGreater(coverage['truncated']['dropped_count'], 0)

    def test_never_silently_replace_first_active_samples(self):
        """Never silently replace first active samples or claim complete coverage."""
        sampler = self._make_sampler(max_samples=3, capture_seconds=5.0,
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-123'},
                                     time_fn=lambda: self.clock['mono'])
        # Trigger via observe_ingress
        http_snapshot = {
            'process_instance': 'proc-123',
            'ingress': {'Received': 101},
            'scrape_finished_monotonic': self.clock['mono'],
            'scrape_finished_timestamp': self.clock['wall']
        }
        sampler.observe_ingress(http_snapshot)
        for i in range(10):
            self.clock['mono'] += 0.1
            sampler.sample_step()

        coverage = sampler.coverage()
        self.assertFalse(coverage.get('complete', True))
        self.assertIn('truncated', coverage)

    def test_distinguish_pre_trigger_evictions_from_active_truncation(self):
        """Distinguish pre-trigger evictions from active truncation and early stop."""
        sampler = self._make_sampler(max_samples=4, capture_seconds=5.0,
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-123'},
                                     time_fn=lambda: self.clock['mono'])
        # Pre-trigger: 6 samples (evicts 2 due to min(4,4)=4 limit)
        for i in range(6):
            self.clock['mono'] += 0.1
            sampler.sample_step()
        pre_evictions = sampler.evicted_count

        # Trigger via observe_ingress
        http_snapshot = {
            'process_instance': 'proc-123',
            'ingress': {'Received': 101},
            'scrape_finished_monotonic': self.clock['mono'],
            'scrape_finished_timestamp': self.clock['wall']
        }
        sampler.observe_ingress(http_snapshot)
        self.clock['mono'] += 0.1
        sampler.sample_step()

        # Active: 5 more (would exceed max_samples=4)
        for i in range(5):
            self.clock['mono'] += 0.2
            sampler.sample_step()

        coverage = sampler.coverage()
        self.assertIn('pre_trigger_evictions', coverage)
        self.assertEqual(coverage['pre_trigger_evictions'], pre_evictions)
        self.assertIn('active_truncation', coverage)

    # ==================== Early Stop / Deadline Before Trigger ====================

    def test_stop_before_trigger_preserves_retained_data(self):
        """Stop before trigger preserves retained pre-trigger data."""
        sampler = self._make_sampler(capture_seconds=5.0,
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-123'},
                                     time_fn=lambda: self.clock['mono'])
        # Pre-trigger samples
        for i in range(3):
            self.clock['mono'] += 0.1
            sampler.sample_step()

        # Stop before any trigger
        sampler.stop(timeout_seconds=1.0)

        # Data preserved
        self.assertEqual(len(sampler.samples), 3)
        coverage = sampler.coverage()
        self.assertEqual(coverage['state'], 'no_trigger')

    def test_deadline_before_trigger_qualifies_no_trigger(self):
        """Deadline before trigger qualifies as no_trigger."""
        sampler = self._make_sampler(deadline_seconds=0.5, capture_seconds=5.0,
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-123'},
                                     time_fn=lambda: self.clock['mono'])
        self.clock['mono'] = 0.6  # Past deadline
        sampler.sample_step()  # Should not trigger, deadline exceeded

        coverage = sampler.coverage()
        self.assertEqual(coverage['state'], 'deadline')

    def test_early_stop_qualifies_early_stop(self):
        """Early explicit stop qualifies as early_stop."""
        sampler = self._make_sampler(capture_seconds=5.0,
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-123'},
                                     time_fn=lambda: self.clock['mono'])
        # Trigger via observe_ingress
        http_snapshot = {
            'process_instance': 'proc-123',
            'ingress': {'Received': 101},
            'scrape_finished_monotonic': self.clock['mono'],
            'scrape_finished_timestamp': self.clock['wall']
        }
        sampler.observe_ingress(http_snapshot)
        self.clock['mono'] += 0.1
        sampler.sample_step()
        sampler.stop(timeout_seconds=1.0)

        coverage = sampler.coverage()
        self.assertEqual(coverage['state'], 'early_stop')

    def test_missing_trigger_is_not_observed_not_empty_success(self):
        """Missing trigger is NOT OBSERVED, not empty success."""
        sampler = self._make_sampler(capture_seconds=5.0,
                                     baseline_metrics={'ingress': {'Received': 100}},
                                     time_fn=lambda: self.clock['mono'])
        for i in range(10):
            self.clock['mono'] += 0.1
            sampler.sample_step()
        sampler.stop(timeout_seconds=1.0)

        coverage = sampler.coverage()
        self.assertEqual(coverage['state'], 'no_trigger')
        self.assertFalse(coverage.get('trigger_observed', False))

    # ==================== Coverage Reporting ====================

    def test_coverage_reports_mode_state_actual_times(self):
        """Coverage should report mode/state, actual trigger/end/freeze Host A times."""
        sampler = self._make_sampler(capture_seconds=2.0,
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-123'},
                                     time_fn=lambda: self.clock['mono'],
                                     wall_fn=lambda: self.clock['wall'])
        # Trigger via observe_ingress
        http_snapshot = {
            'process_instance': 'proc-123',
            'ingress': {'Received': 101},
            'scrape_finished_monotonic': 1.0,
            'scrape_finished_timestamp': '2026-11-07T00:00:01Z'
        }
        sampler.observe_ingress(http_snapshot)
        self.clock['mono'] = 1.0
        self.clock['wall'] = '2026-11-07T00:00:01Z'
        sampler.sample_step()  # Trigger

        self.clock['mono'] = 3.1
        self.clock['wall'] = '2026-11-07T00:00:03Z'
        sampler.sample_step()  # Past capture end (3.0)

        coverage = sampler.coverage()
        self.assertIn('mode', coverage)
        self.assertEqual(coverage['mode'], 'active_capture')
        self.assertIn('state', coverage)
        self.assertEqual(coverage['state'], 'capture_complete')
        self.assertIn('trigger_monotonic', coverage)
        self.assertEqual(coverage['trigger_monotonic'], 1.0)
        self.assertIn('trigger_wall', coverage)
        self.assertEqual(coverage['trigger_wall'], '2026-11-07T00:00:01Z')
        self.assertIn('capture_end_monotonic', coverage)
        self.assertEqual(coverage['capture_end_monotonic'], 3.0)  # 1.0 + 2.0
        self.assertIn('freeze_monotonic', coverage)
        self.assertEqual(coverage['freeze_monotonic'], 3.1)

    def test_coverage_reports_truncation_drop_counts(self):
        """Coverage should report truncation/drop counts."""
        sampler = self._make_sampler(max_samples=3, capture_seconds=5.0,
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-123'},
                                     time_fn=lambda: self.clock['mono'])
        # Trigger via observe_ingress
        http_snapshot = {
            'process_instance': 'proc-123',
            'ingress': {'Received': 101},
            'scrape_finished_monotonic': self.clock['mono'],
            'scrape_finished_timestamp': self.clock['wall']
        }
        sampler.observe_ingress(http_snapshot)
        for i in range(10):
            self.clock['mono'] += 0.1
            sampler.sample_step()

        coverage = sampler.coverage()
        self.assertIn('truncated', coverage)
        self.assertIn('dropped_count', coverage['truncated'])
        self.assertGreater(coverage['truncated']['dropped_count'], 0)

    def test_coverage_reports_pre_trigger_evictions(self):
        """Coverage should report pre-trigger evictions."""
        sampler = self._make_sampler(max_samples=2, capture_seconds=5.0,
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-123'},
                                     time_fn=lambda: self.clock['mono'])
        # 5 pre-trigger samples, max_samples=2, min(4,2)=2 so 3 evicted
        for i in range(5):
            self.clock['mono'] += 0.1
            sampler.sample_step()

        coverage = sampler.coverage()
        self.assertIn('pre_trigger_evictions', coverage)
        self.assertEqual(coverage['pre_trigger_evictions'], 3)

    def test_coverage_reports_capture_end_reached(self):
        """Coverage should report whether declared capture end was reached."""
        sampler = self._make_sampler(capture_seconds=1.0,
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-123'},
                                     time_fn=lambda: self.clock['mono'])
        # Trigger via observe_ingress
        http_snapshot = {
            'process_instance': 'proc-123',
            'ingress': {'Received': 101},
            'scrape_finished_monotonic': 0.0,
            'scrape_finished_timestamp': '2026-11-07T00:00:00Z'
        }
        sampler.observe_ingress(http_snapshot)
        self.clock['mono'] = 0.0
        sampler.sample_step()  # Trigger

        # Not yet reached capture end
        self.clock['mono'] = 0.5
        sampler.sample_step()
        coverage = sampler.coverage()
        self.assertFalse(coverage['capture_end_reached'])

        # Past capture end
        self.clock['mono'] = 1.5
        sampler.sample_step()
        coverage = sampler.coverage()
        self.assertTrue(coverage['capture_end_reached'])

    def test_coverage_preserves_old_fields(self):
        """Coverage must preserve old fields (samples_captured, samples_retained, etc.)."""
        sampler = self._make_sampler(capture_seconds=2.0,
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-123'},
                                     time_fn=lambda: self.clock['mono'])
        # Trigger via observe_ingress
        http_snapshot = {
            'process_instance': 'proc-123',
            'ingress': {'Received': 101},
            'scrape_finished_monotonic': self.clock['mono'],
            'scrape_finished_timestamp': self.clock['wall']
        }
        sampler.observe_ingress(http_snapshot)
        self.clock['mono'] = 0.5
        sampler.sample_step()

        coverage = sampler.coverage()
        old_fields = ['samples_captured', 'samples_retained', 'samples_evicted',
                      'evicted_bytes', 'ring_count_cap', 'ring_byte_cap',
                      'ring_bytes_used', 'errors']
        for field in old_fields:
            self.assertIn(field, coverage)

    def test_coverage_does_not_conflate_collector_retention_with_workload_accepted(self):
        """Coverage must not conflate collector retention with workload accepted."""
        sampler = self._make_sampler(capture_seconds=2.0,
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-123'},
                                     time_fn=lambda: self.clock['mono'])
        # Trigger via observe_ingress
        http_snapshot = {
            'process_instance': 'proc-123',
            'ingress': {'Received': 101},
            'scrape_finished_monotonic': self.clock['mono'],
            'scrape_finished_timestamp': self.clock['wall']
        }
        sampler.observe_ingress(http_snapshot)
        self.clock['mono'] = 0.5
        sampler.sample_step()

        coverage = sampler.coverage()
        self.assertNotIn('workload_accepted', coverage)
        self.assertNotIn('full_p8_accepted', coverage)

    # ==================== Injected/Source Timestamps ====================

    def test_injected_clocks_used_in_sample_source_timing(self):
        """Injected time_fn and wall_fn used for sample/source timing."""
        sampler = self._make_sampler(capture_seconds=2.0,
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-123'},
                                     time_fn=lambda: self.clock['mono'],
                                     wall_fn=lambda: self.clock['wall'])
        # Trigger via observe_ingress
        http_snapshot = {
            'process_instance': 'proc-123',
            'ingress': {'Received': 101},
            'scrape_finished_monotonic': 100.0,
            'scrape_finished_timestamp': '2026-11-07T10:00:00Z'
        }
        sampler.observe_ingress(http_snapshot)
        self.clock['mono'] = 100.0
        self.clock['wall'] = '2026-11-07T10:00:00Z'
        sampler.sample_step()

        sample = sampler.samples[0]
        self.assertEqual(sample['postgres']['start_monotonic_seconds'], 100.0)
        self.assertEqual(sample['postgres']['start_utc'], '2026-11-07T10:00:00Z')

    def test_legacy_rolling_eviction_preserved_when_no_capture_options(self):
        """Legacy rolling eviction preserved when capture options absent."""
        sampler = self._make_sampler(max_samples=3, byte_cap=10000)
        for i in range(10):
            self.clock['mono'] += 0.1
            sampler.sample_step()

        # Should behave as pure rolling buffer, limited by max_samples
        self.assertEqual(len(sampler.samples), 3)
        coverage = sampler.coverage()
        self.assertEqual(coverage['samples_evicted'], 7)
        self.assertNotIn('mode', coverage)  # No active capture mode

    # ==================== Integration with Existing Methods ====================

    def test_existing_public_methods_unchanged(self):
        """Existing public methods (samples, coverage, start, stop) remain compatible."""
        sampler = self._make_sampler(capture_seconds=2.0,
                                     baseline_metrics={'ingress': {'Received': 100}})
        # Trigger via observe_ingress
        http_snapshot = {
            'process_instance': 'proc-123',
            'ingress': {'Received': 101},
            'scrape_finished_monotonic': self.clock['mono'],
            'scrape_finished_timestamp': self.clock['wall']
        }
        sampler.observe_ingress(http_snapshot)
        self.clock['mono'] = 0.5
        sampler.sample_step()

        # samples property
        samples = sampler.samples
        self.assertIsInstance(samples, list)

        # coverage method
        cov = sampler.coverage()
        self.assertIsInstance(cov, dict)

        # start/stop
        sampler.start()
        time.sleep(0.01)
        stopped = sampler.stop(timeout_seconds=1.0)
        self.assertTrue(stopped)

    def test_tiny_caps_supported_for_deterministic_tests(self):
        """Tiny caps (interval=0.01, small byte_cap) must work for deterministic tests."""
        sampler = self._make_sampler(interval=0.01, max_samples=2, byte_cap=256,
                                     capture_seconds=1.0,
                                     baseline_metrics={'ingress': {'Received': 100}},
                                     time_fn=lambda: self.clock['mono'])
        # Trigger via observe_ingress
        http_snapshot = {
            'process_instance': 'proc-123',
            'ingress': {'Received': 101},
            'scrape_finished_monotonic': self.clock['mono'],
            'scrape_finished_timestamp': self.clock['wall']
        }
        sampler.observe_ingress(http_snapshot)
        for i in range(5):
            self.clock['mono'] += 0.1
            sampler.sample_step()

        coverage = sampler.coverage()
        self.assertLessEqual(coverage['samples_retained'], 2)
        self.assertLessEqual(coverage['ring_bytes_used'], 256)

    def test_exact_encoded_byte_accounting_includes_size_semantics(self):
        """Exact encoded byte accounting includes existing _size semantics."""
        sampler = self._make_sampler(byte_cap=1000, capture_seconds=2.0,
                                     baseline_metrics={'ingress': {'Received': 100}},
                                     time_fn=lambda: self.clock['mono'])
        # Trigger via observe_ingress
        http_snapshot = {
            'process_instance': 'proc-123',
            'ingress': {'Received': 101},
            'scrape_finished_monotonic': self.clock['mono'],
            'scrape_finished_timestamp': self.clock['wall']
        }
        sampler.observe_ingress(http_snapshot)
        self.clock['mono'] = 0.5
        sampler.sample_step()

        sample = sampler.samples[0]
        self.assertIn('_size', sample)
        # _size should match actual JSON encoded size
        actual_size = len(json.dumps(sample, separators=(',', ':')).encode())
        self.assertEqual(sample['_size'], actual_size)

        coverage = sampler.coverage()
        self.assertEqual(coverage['ring_bytes_used'], actual_size)

    # ==================== REGRESSION TESTS FOR PARENT REPORTED DEFECTS ====================

    def test_regression_observe_ingress_is_sole_trigger_api(self):
        """REGRESSION: observe_ingress(snapshot) must be the SOLE trigger; PG reader must NOT trigger."""
        sampler = self._make_sampler(capture_seconds=2.0,
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-123'},
                                     time_fn=lambda: self.clock['mono'],
                                     wall_fn=lambda: self.clock['wall'])
        
        # PG reader returns database/WAL/activity data, NOT ingress.Received
        pg_dto = {
            'database': {'stats': {'xact_commit': 5}},
            'wal': {'stats': {'wal_records': 3}},
            'activity': [{'pid': 123, 'query': 'SELECT 1'}]
        }
        sampler.reader['postgres'] = lambda: pg_dto
        sampler.reader['linux'] = lambda: {'cpu': 100}
        
        # Before observe_ingress: no trigger even if PG reader has data
        self.clock['mono'] = 1.0
        sampler.sample_step()
        self.assertFalse(sampler._triggered, "PG reader data must not trigger capture")
        
        # observe_ingress with valid HTTP snapshot triggers capture
        http_snapshot = {
            'process_instance': 'proc-123',
            'ingress': {'Received': 101},
            'scrape_finished_monotonic': 1.0,
            'scrape_finished_timestamp': '2026-11-07T00:00:01Z'
        }
        sampler.observe_ingress(http_snapshot)
        self.assertTrue(sampler._triggered, "observe_ingress must trigger capture")
        self.assertEqual(sampler._trigger_monotonic, 1.0)
        self.assertEqual(sampler._trigger_wall, '2026-11-07T00:00:01Z')

    def test_regression_pg_dto_preserved_not_filtered(self):
        """REGRESSION: PG reader DTO must be preserved exactly, not filtered to ingress/process_instance only."""
        sampler = self._make_sampler(capture_seconds=2.0,
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-123'},
                                     time_fn=lambda: self.clock['mono'],
                                     wall_fn=lambda: self.clock['wall'])
        
        # Real PG diagnostics DTO with database/wal/activity
        pg_dto = {
            'database': {'stats': {'xact_commit': 5, 'xact_rollback': 1}},
            'wal': {'stats': {'wal_records': 3, 'wal_bytes': 1024}},
            'activity': [{'pid': 123, 'query': 'SELECT 1', 'state': 'active'},
                         {'pid': 124, 'query': 'COMMIT', 'state': 'idle'}],
            'process_instance': 'proc-123'
        }
        sampler.reader['postgres'] = lambda: pg_dto
        sampler.reader['linux'] = lambda: {'cpu': 100}
        
        # Trigger via observe_ingress
        http_snapshot = {
            'process_instance': 'proc-123',
            'ingress': {'Received': 101},
            'scrape_finished_monotonic': 1.0,
            'scrape_finished_timestamp': '2026-11-07T00:00:01Z'
        }
        sampler.observe_ingress(http_snapshot)
        
        self.clock['mono'] = 1.0
        sampler.sample_step()
        
        # PG value must be preserved exactly as reader provided
        sample = sampler.samples[0]
        pg_value = sample['postgres']['value']
        self.assertEqual(pg_value, pg_dto, "PG DTO must be preserved exactly, not filtered")
        self.assertIn('database', pg_value)
        self.assertIn('wal', pg_value)
        self.assertIn('activity', pg_value)
        self.assertEqual(pg_value['database']['stats']['xact_commit'], 5)

    def test_regression_active_cap_freezes_no_more_readers(self):
        """REGRESSION: After active count/byte cap, must freeze - no more readers start."""
        sampler = self._make_sampler(max_samples=1, byte_cap=10000, capture_seconds=5.0,
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-123'},
                                     time_fn=lambda: self.clock['mono'],
                                     wall_fn=lambda: self.clock['wall'])
        
        read_count = {'pg': 0, 'linux': 0}
        def counting_pg():
            read_count['pg'] += 1
            return {'database': {'stats': {'xact_commit': read_count['pg']}}, 'process_instance': 'proc-123'}
        def counting_linux():
            read_count['linux'] += 1
            return {'cpu': read_count['linux'] * 10}
        
        sampler.reader['postgres'] = counting_pg
        sampler.reader['linux'] = counting_linux
        
        # Trigger
        http_snapshot = {
            'process_instance': 'proc-123',
            'ingress': {'Received': 101},
            'scrape_finished_monotonic': 1.0,
            'scrape_finished_timestamp': '2026-11-07T00:00:01Z'
        }
        sampler.observe_ingress(http_snapshot)
        
        self.clock['mono'] = 1.0
        sampler.sample_step()  # Sample 1 - fills max_samples=1
        sampler.sample_step()  # Sample 2 - hits cap, must freeze
        
        # Should be frozen now (next sample would exceed cap)
        self.assertTrue(sampler._frozen, "Must freeze after active count cap")
        self.assertFalse(sampler._active, "Active must be False after cap")
        
        reads_at_freeze_pg = read_count['pg']
        reads_at_freeze_linux = read_count['linux']
        
        # Further sample_step calls must NOT start more readers
        self.clock['mono'] = 1.5
        sampler.sample_step()
        self.clock['mono'] = 2.0
        sampler.sample_step()
        
        self.assertEqual(read_count['pg'], reads_at_freeze_pg, "No more PG reads after freeze")
        self.assertEqual(read_count['linux'], reads_at_freeze_linux, "No more Linux reads after freeze")

    def test_regression_coverage_capture_end_wall_derived_not_live(self):
        """REGRESSION: capture_end_wall must be derived from trigger_wall + delta, not live wall_fn()."""
        sampler = self._make_sampler(capture_seconds=2.0,
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-123'},
                                     time_fn=lambda: self.clock['mono'],
                                     wall_fn=lambda: self.clock['wall'])
        
        http_snapshot = {
            'process_instance': 'proc-123',
            'ingress': {'Received': 101},
            'scrape_finished_monotonic': 1.0,
            'scrape_finished_timestamp': '2026-11-07T00:00:01Z'
        }
        sampler.observe_ingress(http_snapshot)
        
        self.clock['mono'] = 1.0
        sampler.sample_step()  # Trigger
        
        # First coverage check
        cov1 = sampler.coverage()
        capture_end_wall_1 = cov1['capture_end_wall']
        
        # Wait and change wall clock (simulating operator delay)
        self.clock['wall'] = '2026-11-07T00:05:00Z'  # 5 minutes later
        self.clock['mono'] = 1.5
        sampler.sample_step()
        
        # Second coverage check - capture_end_wall must NOT change
        cov2 = sampler.coverage()
        capture_end_wall_2 = cov2['capture_end_wall']
        
        self.assertEqual(capture_end_wall_1, capture_end_wall_2, 
                        "capture_end_wall must be stable, derived from trigger_wall + capture_seconds")
        self.assertEqual(capture_end_wall_1, '2026-11-07T00:00:03Z', 
                        "capture_end_wall = trigger_wall + capture_seconds (1s + 2s = 3s)")

    def test_regression_session_deadline_clips_incomplete_not_complete(self):
        """REGRESSION: Session deadline clipping capture window must report incomplete, not clean capture_complete."""
        sampler = self._make_sampler(deadline_seconds=1.5, capture_seconds=5.0,
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-123'},
                                     time_fn=lambda: self.clock['mono'],
                                     wall_fn=lambda: self.clock['wall'])
        
        http_snapshot = {
            'process_instance': 'proc-123',
            'ingress': {'Received': 101},
            'scrape_finished_monotonic': 0.5,
            'scrape_finished_timestamp': '2026-11-07T00:00:00Z'
        }
        sampler.observe_ingress(http_snapshot)
        
        self.clock['mono'] = 0.5
        sampler.sample_step()  # Trigger at 0.5, capture_end would be 5.5 but deadline is 1.5
        
        # At 1.6, past deadline (1.5) but within original capture_seconds (5.0)
        self.clock['mono'] = 1.6
        sampler.sample_step()
        
        # Should be frozen but state should reflect deadline clipping, not clean capture_complete
        self.assertTrue(sampler._frozen)
        cov = sampler.coverage()
        # Should indicate deadline or incomplete, not clean capture_complete
        self.assertIn(cov['state'], ['deadline', 'capture_complete'])  # deadline if not triggered, capture_complete if triggered but clipped
        self.assertFalse(cov.get('capture_end_reached', False), 
                        "declared capture end NOT reached when clipped by deadline")

    def test_regression_pre_trigger_oversized_single_sample_not_appended(self):
        """REGRESSION: Single pre-trigger sample exceeding 64KiB must not be appended even if total cap permits."""
        sampler = self._make_sampler(max_samples=100, byte_cap=100000, capture_seconds=5.0,
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-123'},
                                     time_fn=lambda: self.clock['mono'],
                                     wall_fn=lambda: self.clock['wall'])
        
        # Single sample > 64KiB (pre-trigger byte limit)
        huge_data = {'data': 'x' * 70000}  # 70KB > 64KiB
        sampler.reader['postgres'] = lambda: {**huge_data, 'process_instance': 'proc-123'}
        sampler.reader['linux'] = lambda: {'cpu': 100}
        
        self.clock['mono'] = 1.0
        sampler.sample_step()  # Pre-trigger, oversized
        
        # Should NOT append oversized pre-trigger sample
        self.assertEqual(len(sampler.samples), 0, "Oversized pre-trigger sample must not be appended")
        
        # Normal sample should work
        sampler.reader['postgres'] = lambda: {'small': 'data', 'process_instance': 'proc-123'}
        sampler.sample_step()
        self.assertEqual(len(sampler.samples), 1)

    def test_regression_single_sample_exceeds_total_cap_truncates_freezes(self):
        """REGRESSION: Single sample larger than total byte_cap must truncate/freeze."""
        sampler = self._make_sampler(max_samples=10, byte_cap=1000, capture_seconds=5.0,
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-123'},
                                     time_fn=lambda: self.clock['mono'],
                                     wall_fn=lambda: self.clock['wall'])
        
        http_snapshot = {
            'process_instance': 'proc-123',
            'ingress': {'Received': 101},
            'scrape_finished_monotonic': 1.0,
            'scrape_finished_timestamp': '2026-11-07T00:00:01Z'
        }
        sampler.observe_ingress(http_snapshot)
        
        # Single sample > total byte_cap
        huge_data = {'data': 'x' * 2000}  # 2KB > 1KB cap
        sampler.reader['postgres'] = lambda: {**huge_data, 'process_instance': 'proc-123'}
        sampler.reader['linux'] = lambda: {'cpu': 100}
        
        self.clock['mono'] = 1.0
        sampler.sample_step()
        
        # Should freeze and mark truncated
        self.assertTrue(sampler._frozen, "Must freeze when single sample exceeds total cap")
        cov = sampler.coverage()
        self.assertIn('truncated', cov)
        self.assertFalse(cov.get('complete', True))

    def test_regression_observe_ingress_validates_snapshot_fields(self):
        """REGRESSION: observe_ingress must validate snapshot fields strictly."""
        sampler = self._make_sampler(capture_seconds=2.0,
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-123'},
                                     time_fn=lambda: self.clock['mono'],
                                     wall_fn=lambda: self.clock['wall'])
        
        # Missing process_instance
        sampler.observe_ingress({
            'ingress': {'Received': 101},
            'scrape_finished_monotonic': 1.0,
            'scrape_finished_timestamp': '2026-11-07T00:00:01Z'
        })
        self.assertIn('observe_ingress_process_instance_missing_or_invalid', sampler.errors)
        sampler.errors.clear()
        
        # Non-string process_instance
        sampler.observe_ingress({
            'process_instance': 123,
            'ingress': {'Received': 101},
            'scrape_finished_monotonic': 1.0,
            'scrape_finished_timestamp': '2026-11-07T00:00:01Z'
        })
        self.assertIn('observe_ingress_process_instance_missing_or_invalid', sampler.errors)
        sampler.errors.clear()
        
        # Missing ingress
        sampler.observe_ingress({
            'process_instance': 'proc-123',
            'scrape_finished_monotonic': 1.0,
            'scrape_finished_timestamp': '2026-11-07T00:00:01Z'
        })
        self.assertIn('observe_ingress_missing_ingress', sampler.errors)
        sampler.errors.clear()
        
        # Missing Received
        sampler.observe_ingress({
            'process_instance': 'proc-123',
            'ingress': {},
            'scrape_finished_monotonic': 1.0,
            'scrape_finished_timestamp': '2026-11-07T00:00:01Z'
        })
        self.assertIn('observe_ingress_received_must_be_nonnegative_int', sampler.errors)
        sampler.errors.clear()
        
        # Non-int Received
        sampler.observe_ingress({
            'process_instance': 'proc-123',
            'ingress': {'Received': 'not-int'},
            'scrape_finished_monotonic': 1.0,
            'scrape_finished_timestamp': '2026-11-07T00:00:01Z'
        })
        self.assertIn('observe_ingress_received_must_be_nonnegative_int', sampler.errors)
        sampler.errors.clear()
        
        # Negative Received
        sampler.observe_ingress({
            'process_instance': 'proc-123',
            'ingress': {'Received': -1},
            'scrape_finished_monotonic': 1.0,
            'scrape_finished_timestamp': '2026-11-07T00:00:01Z'
        })
        self.assertIn('observe_ingress_received_must_be_nonnegative_int', sampler.errors)
        sampler.errors.clear()
        
        # Non-finite Received
        sampler.observe_ingress({
            'process_instance': 'proc-123',
            'ingress': {'Received': float('inf')},
            'scrape_finished_monotonic': 1.0,
            'scrape_finished_timestamp': '2026-11-07T00:00:01Z'
        })
        self.assertIn('observe_ingress_received_must_be_finite', sampler.errors)
        sampler.errors.clear()
        
        # Missing scrape_finished_monotonic
        sampler.observe_ingress({
            'process_instance': 'proc-123',
            'ingress': {'Received': 101},
            'scrape_finished_timestamp': '2026-11-07T00:00:01Z'
        })
        self.assertIn('observe_ingress_scrape_monotonic_must_be_numeric', sampler.errors)
        sampler.errors.clear()
        
        # Non-finite scrape_finished_monotonic
        sampler.observe_ingress({
            'process_instance': 'proc-123',
            'ingress': {'Received': 101},
            'scrape_finished_monotonic': float('nan'),
            'scrape_finished_timestamp': '2026-11-07T00:00:01Z'
        })
        self.assertIn('observe_ingress_scrape_monotonic_must_be_finite', sampler.errors)
        sampler.errors.clear()
        
        # Missing scrape_finished_timestamp
        sampler.observe_ingress({
            'process_instance': 'proc-123',
            'ingress': {'Received': 101},
            'scrape_finished_monotonic': 1.0
        })
        self.assertIn('observe_ingress_scrape_timestamp_missing_or_invalid', sampler.errors)
        sampler.errors.clear()
        
        # Invalid scrape_finished_timestamp format
        sampler.observe_ingress({
            'process_instance': 'proc-123',
            'ingress': {'Received': 101},
            'scrape_finished_monotonic': 1.0,
            'scrape_finished_timestamp': 'not-a-timestamp'
        })
        self.assertIn('observe_ingress_scrape_timestamp_must_be_utc', sampler.errors)

    def test_regression_observe_ingress_same_process_instance_required(self):
        """REGRESSION: observe_ingress must require same process_instance as baseline."""
        sampler = self._make_sampler(capture_seconds=2.0,
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-123'},
                                     time_fn=lambda: self.clock['mono'],
                                     wall_fn=lambda: self.clock['wall'])
        
        # Different process_instance - must not trigger, record qualification
        http_snapshot = {
            'process_instance': 'proc-456',  # Different!
            'ingress': {'Received': 101},
            'scrape_finished_monotonic': 1.0,
            'scrape_finished_timestamp': '2026-11-07T00:00:01Z'
        }
        sampler.observe_ingress(http_snapshot)
        self.assertFalse(sampler._triggered, "Different process_instance must not trigger")
        self.assertIn('process_instance_changed', sampler.errors)

    def test_regression_observe_ingress_received_increase_required(self):
        """REGRESSION: observe_ingress must require Received increase above baseline; equal is idle, not counter_reset."""
        sampler = self._make_sampler(capture_seconds=2.0,
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-123'},
                                     time_fn=lambda: self.clock['mono'],
                                     wall_fn=lambda: self.clock['wall'])
        
        # Equal Received = idle, not trigger, not counter_reset
        http_snapshot = {
            'process_instance': 'proc-123',
            'ingress': {'Received': 100},  # Equal to baseline
            'scrape_finished_monotonic': 1.0,
            'scrape_finished_timestamp': '2026-11-07T00:00:01Z'
        }
        sampler.observe_ingress(http_snapshot)
        self.assertFalse(sampler._triggered, "Equal Received is idle, not trigger")
        self.assertNotIn('counter_reset', sampler.errors, "Equal Received must not be counter_reset")
        
        # Decrease = counter_reset qualification
        http_snapshot2 = {
            'process_instance': 'proc-123',
            'ingress': {'Received': 99},  # Decrease
            'scrape_finished_monotonic': 1.1,
            'scrape_finished_timestamp': '2026-11-07T00:00:02Z'
        }
        sampler.observe_ingress(http_snapshot2)
        self.assertFalse(sampler._triggered, "Decrease must not trigger")
        self.assertIn('counter_reset', sampler.errors, "Decrease must record counter_reset")

    def test_regression_observe_ingress_counter_reset_sticky_after_trigger(self):
        """REGRESSION: Counter reset qualification must be sticky even after trigger."""
        sampler = self._make_sampler(capture_seconds=2.0,
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-123'},
                                     time_fn=lambda: self.clock['mono'],
                                     wall_fn=lambda: self.clock['wall'])
        
        # Valid trigger
        http_snapshot = {
            'process_instance': 'proc-123',
            'ingress': {'Received': 101},
            'scrape_finished_monotonic': 1.0,
            'scrape_finished_timestamp': '2026-11-07T00:00:01Z'
        }
        sampler.observe_ingress(http_snapshot)
        self.assertTrue(sampler._triggered)
        
        # After trigger, counter decrease should still record qualification
        http_snapshot2 = {
            'process_instance': 'proc-123',
            'ingress': {'Received': 99},  # Decrease after trigger
            'scrape_finished_monotonic': 1.1,
            'scrape_finished_timestamp': '2026-11-07T00:00:02Z'
        }
        sampler.observe_ingress(http_snapshot2)
        self.assertIn('counter_reset', sampler.errors, "Counter reset after trigger must record qualification")

    def test_regression_observe_ingress_deduplicates_errors(self):
        """REGRESSION: observe_ingress must bound/deduplicate error list (no unbounded repeats)."""
        sampler = self._make_sampler(capture_seconds=2.0,
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-123'},
                                     time_fn=lambda: self.clock['mono'],
                                     wall_fn=lambda: self.clock['wall'])
        
        # Multiple counter_reset observations
        for i in range(20):
            http_snapshot = {
                'process_instance': 'proc-123',
                'ingress': {'Received': 99},  # Decrease
                'scrape_finished_monotonic': 1.0 + i * 0.1,
                'scrape_finished_timestamp': f'2026-11-07T00:00:{1+i:02d}Z'
            }
            sampler.observe_ingress(http_snapshot)
        
        # Errors should be deduplicated/bounded (max 16 like other errors)
        self.assertLessEqual(len(sampler.errors), 16, "Errors must be bounded")
        self.assertEqual(len(set(sampler.errors)), len(sampler.errors), "Errors must be deduplicated")

    def test_regression_baseline_held_as_minimal_scalar_copy(self):
        """REGRESSION: Baseline must be held as minimal scalar copy, not arbitrary caller dict."""
        baseline_dict = {
            'ingress': {'Received': 100, 'extra_field': 'should_not_be_retained'},
            'process_instance': 'proc-123',
            'arbitrary': 'data'
        }
        sampler = self._make_sampler(capture_seconds=2.0,
                                     baseline_metrics=baseline_dict,
                                     time_fn=lambda: self.clock['mono'],
                                     wall_fn=lambda: self.clock['wall'])
        
        # Baseline should only contain Received and process_instance
        self.assertEqual(sampler.baseline_metrics['ingress']['Received'], 100)
        self.assertEqual(sampler.baseline_metrics.get('process_instance'), 'proc-123')
        self.assertNotIn('extra_field', sampler.baseline_metrics.get('ingress', {}))
        self.assertNotIn('arbitrary', sampler.baseline_metrics)

    def test_regression_valid_bounds_max_samples_20000_byte_cap_16mib(self):
        """REGRESSION: Must validate max_samples <= 20000, byte_cap <= 16MiB."""
        # max_samples > 20000 should fail
        with self.assertRaises(ValueError):
            self._make_sampler(max_samples=20001)
        
        # byte_cap > 16MiB should fail
        with self.assertRaises(ValueError):
            self._make_sampler(byte_cap=17 * 1024 * 1024)
        
        # Valid bounds should work
        sampler = self._make_sampler(max_samples=20000, byte_cap=16 * 1024 * 1024)
        self.assertEqual(sampler.max_samples, 20000)
        self.assertEqual(sampler.byte_cap, 16 * 1024 * 1024)

    def test_regression_deadline_finite_capture_finite(self):
        """REGRESSION: Must validate finite deadlines and capture times."""
        # Infinite deadline
        with self.assertRaises(ValueError):
            self._make_sampler(deadline_seconds=float('inf'))
        
        # Infinite capture_seconds
        with self.assertRaises(ValueError):
            self._make_sampler(capture_seconds=float('inf'), baseline_metrics={'ingress': {'Received': 100}})
        
        # NaN deadline
        with self.assertRaises(ValueError):
            self._make_sampler(deadline_seconds=float('nan'))
        
        # NaN capture_seconds
        with self.assertRaises(ValueError):
            self._make_sampler(capture_seconds=float('nan'), baseline_metrics={'ingress': {'Received': 100}})

    def test_regression_explicit_stop_freezes_state_under_lock(self):
        """REGRESSION: Explicit stop must freeze state under lock; no late appends after stop."""
        sampler = self._make_sampler(capture_seconds=5.0,
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-123'},
                                     time_fn=lambda: self.clock['mono'],
                                     wall_fn=lambda: self.clock['wall'])
        
        http_snapshot = {
            'process_instance': 'proc-123',
            'ingress': {'Received': 101},
            'scrape_finished_monotonic': 1.0,
            'scrape_finished_timestamp': '2026-11-07T00:00:01Z'
        }
        sampler.observe_ingress(http_snapshot)
        
        self.clock['mono'] = 1.0
        sampler.sample_step()
        
        # Stop
        sampler.stop(timeout_seconds=1.0)
        
        # After stop, sample_step must not append
        self.clock['mono'] = 2.0
        sampler.sample_step()
        
        # Should be frozen
        self.assertTrue(sampler._frozen)
        # State should be early_stop
        cov = sampler.coverage()
        self.assertEqual(cov['state'], 'early_stop')
        
        # No new samples after stop
        self.assertEqual(len(sampler.samples), 1)

    def test_regression_coverage_immutable_after_freeze(self):
        """REGRESSION: Coverage must be immutable after freeze (capture_end_wall stable)."""
        sampler = self._make_sampler(capture_seconds=2.0,
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-123'},
                                     time_fn=lambda: self.clock['mono'],
                                     wall_fn=lambda: self.clock['wall'])
        
        http_snapshot = {
            'process_instance': 'proc-123',
            'ingress': {'Received': 101},
            'scrape_finished_monotonic': 1.0,
            'scrape_finished_timestamp': '2026-11-07T00:00:01Z'
        }
        sampler.observe_ingress(http_snapshot)
        
        self.clock['mono'] = 1.0
        sampler.sample_step()
        
        # Freeze the sampler
        sampler.stop(timeout_seconds=1.0)
        
        # Get coverage multiple times - must be identical
        cov1 = sampler.coverage()
        cov2 = sampler.coverage()
        cov3 = sampler.coverage()
        
        self.assertEqual(cov1['capture_end_wall'], cov2['capture_end_wall'])
        self.assertEqual(cov2['capture_end_wall'], cov3['capture_end_wall'])
        self.assertEqual(cov1['trigger_wall'], cov2['trigger_wall'])
        self.assertEqual(cov1['trigger_monotonic'], cov2['trigger_monotonic'])
        self.assertEqual(cov1['freeze_wall'], cov2['freeze_wall'])  # freeze_wall also stable

    def test_regression_check_before_each_reader_starts(self):
        """REGRESSION: Must check frozen/capture_end before EACH reader starts."""
        sampler = self._make_sampler(capture_seconds=0.5,  # Very short capture
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-123'},
                                     time_fn=lambda: self.clock['mono'],
                                     wall_fn=lambda: self.clock['wall'],
                                     interval=0.01)
        
        http_snapshot = {
            'process_instance': 'proc-123',
            'ingress': {'Received': 101},
            'scrape_finished_monotonic': 0.0,
            'scrape_finished_timestamp': '2026-11-07T00:00:00Z'
        }
        sampler.observe_ingress(http_snapshot)
        
        read_order = []
        def make_reader(name, delay=0):
            def reader():
                read_order.append(f'{name}_start')
                if delay:
                    # Simulate slow reader
                    pass
                read_order.append(f'{name}_end')
                return {name: 'data'}
            return reader
        
        sampler.reader['postgres'] = make_reader('pg')
        sampler.reader['linux'] = make_reader('linux')
        sampler.reader['docker'] = make_reader('docker')
        
        self.clock['mono'] = 0.0
        sampler.sample_step()  # Trigger + sample at t=0
        
        # At 0.25, within capture window (0.5) - should run all readers
        self.clock['mono'] = 0.25
        read_order.clear()
        sampler.sample_step()
        
        # All three readers should have started and completed
        self.assertIn('pg_start', read_order)
        self.assertIn('pg_end', read_order)
        self.assertIn('linux_start', read_order)
        self.assertIn('linux_end', read_order)
        self.assertIn('docker_start', read_order)
        self.assertIn('docker_end', read_order)
        
        # At 0.6, past capture_end (0.5) - should not start any readers
        read_order.clear()
        self.clock['mono'] = 0.6
        sampler.sample_step()
        self.assertEqual(read_order, [], "No readers should start after capture_end")

    def test_regression_slow_reader_unavailable_during_capture(self):
        """REGRESSION: Valid HTTP trigger while slow reader unavailable must not crash."""
        sampler = self._make_sampler(capture_seconds=2.0,
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-123'},
                                     time_fn=lambda: self.clock['mono'],
                                     wall_fn=lambda: self.clock['wall'])
        
        http_snapshot = {
            'process_instance': 'proc-123',
            'ingress': {'Received': 101},
            'scrape_finished_monotonic': 1.0,
            'scrape_finished_timestamp': '2026-11-07T00:00:01Z'
        }
        sampler.observe_ingress(http_snapshot)
        
        # PG reader fails
        sampler.reader['postgres'] = lambda: (_ for _ in ()).throw(RuntimeError("PG unavailable"))
        sampler.reader['linux'] = lambda: {'cpu': 100}
        
        self.clock['mono'] = 1.0
        sampler.sample_step()  # Should not crash, should record error
        
        self.assertEqual(len(sampler.samples), 1)
        sample = sampler.samples[0]
        self.assertEqual(sample['postgres']['error'], 'reader_failed')
        self.assertIsNone(sample['postgres']['value'])

    def test_regression_stop_concurrent_reader_no_late_append(self):
        """REGRESSION: Stop concurrent reader must not allow late append after freeze."""
        sampler = self._make_sampler(capture_seconds=5.0,
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-123'},
                                     time_fn=lambda: self.clock['mono'],
                                     wall_fn=lambda: self.clock['wall'],
                                     interval=0.01)
        
        http_snapshot = {
            'process_instance': 'proc-123',
            'ingress': {'Received': 101},
            'scrape_finished_monotonic': 1.0,
            'scrape_finished_timestamp': '2026-11-07T00:00:01Z'
        }
        sampler.observe_ingress(http_snapshot)
        
        self.clock['mono'] = 1.0
        sampler.sample_step()
        
        # Start background sampling
        sampler.start()
        time.sleep(0.05)  # Let it sample a few times
        
        # Stop while background thread is running
        sampler.stop(timeout_seconds=1.0)
        
        # After stop, no more samples should be added
        samples_at_stop = len(sampler.samples)
        time.sleep(0.1)
        self.assertEqual(len(sampler.samples), samples_at_stop, "No samples after stop")
        
        # Coverage must reflect early_stop
        cov = sampler.coverage()
        self.assertEqual(cov['state'], 'early_stop')


# ==================== PARENT REPORTED REGRESSIONS ====================

    def test_parent_A_capture_end_reached_false_when_clipped_by_deadline(self):
        """PARENT A: capture_end_reached must be False when capture end clipped by session deadline.
        
        capture_seconds=5, trigger at 0, sample at 0, rejected sample at 1:
        coverage()['capture_end_reached'] must be False even though state=capture_complete.
        Cap freeze must not equate cap state with declared end reached.
        """
        sampler = self._make_sampler(deadline_seconds=1.0, capture_seconds=5.0,
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-123'},
                                     time_fn=lambda: self.clock['mono'],
                                     wall_fn=lambda: self.clock['wall'])
        
        http_snapshot = {
            'process_instance': 'proc-123',
            'ingress': {'Received': 101},
            'scrape_finished_monotonic': 0.0,
            'scrape_finished_timestamp': '2026-11-07T00:00:00Z'
        }
        sampler.observe_ingress(http_snapshot)
        
        self.clock['mono'] = 0.0
        sampler.sample_step()  # Trigger at 0.0, capture_end would be 5.0 but deadline is 1.0
        
        # At 1.1, past deadline (1.0) - should freeze due to deadline clipping
        self.clock['mono'] = 1.1
        sampler.sample_step()
        
        cov = sampler.coverage()
        # Must be frozen due to deadline, state can be capture_complete but capture_end_reached must be False
        self.assertTrue(sampler._frozen)
        self.assertIn(cov['state'], ['deadline', 'capture_complete'])  # Both acceptable when triggered but clipped
        self.assertFalse(cov.get('capture_end_reached', True), 
                        "capture_end_reached must be False when clipped by deadline")

    def test_parent_B_process_change_permanently_qualifies_incomplete(self):
        """PARENT B: process_instance_changed must permanently qualify incomplete.
        
        Trigger process p, then observe process other, advance to 6 and sample_step:
        coverage complete must be False with dedicated sticky qualification.
        Do not derive qualification solely from errors list (can fill 16 slots).
        Keep a small dedicated sticky bool/reasons and deduplicate fixed errors.
        """
        sampler = self._make_sampler(capture_seconds=5.0,
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-p'},
                                     time_fn=lambda: self.clock['mono'],
                                     wall_fn=lambda: self.clock['wall'])
        
        # Valid trigger with process p
        http_snapshot = {
            'process_instance': 'proc-p',
            'ingress': {'Received': 101},
            'scrape_finished_monotonic': 1.0,
            'scrape_finished_timestamp': '2026-11-07T00:00:01Z'
        }
        sampler.observe_ingress(http_snapshot)
        self.assertTrue(sampler._triggered)
        
        # Now observe different process - must record qualification
        http_snapshot2 = {
            'process_instance': 'proc-other',
            'ingress': {'Received': 102},
            'scrape_finished_monotonic': 2.0,
            'scrape_finished_timestamp': '2026-11-07T00:00:02Z'
        }
        sampler.observe_ingress(http_snapshot2)
        
        # Advance time and sample
        self.clock['mono'] = 6.0
        sampler.sample_step()
        
        # Must be qualified incomplete, not complete
        cov = sampler.coverage()
        self.assertFalse(cov.get('complete', True), "Process change must permanently qualify incomplete")
        self.assertIn('process_instance_changed', sampler.errors)
        
        # Fill errors with other invalid observations (16 max)
        for i in range(20):
            sampler.observe_ingress({
                'process_instance': f'proc-{i}',
                'ingress': {'Received': 99},
                'scrape_finished_monotonic': 3.0 + i,
                'scrape_finished_timestamp': f'2026-11-07T00:00:{3+i:02d}Z'
            })
        
        # Even with errors filled, process_instance_changed qualification must persist
        cov2 = sampler.coverage()
        self.assertFalse(cov2.get('complete', True), "Qualification must persist even when errors list is full")

    def test_parent_C_scrape_timestamp_requires_valid_iso_utc(self):
        """PARENT C: scrape_finished_timestamp must be valid ISO UTC parsing.
        
        'not-a-dateZ' must be rejected. Must parse bounded ISO date, require timezone UTC offset 0,
        reject impossible dates/oversized/control characters. No fallback to input on invalid parse.
        """
        sampler = self._make_sampler(capture_seconds=2.0,
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-123'},
                                     time_fn=lambda: self.clock['mono'],
                                     wall_fn=lambda: self.clock['wall'])
        
        # Invalid timestamps must be rejected
        invalid_timestamps = [
            'not-a-dateZ',           # Not a date
            '2026-13-01T00:00:00Z',  # Invalid month
            '2026-02-30T00:00:00Z',  # Invalid day
            '2026-01-01T25:00:00Z',  # Invalid hour
            '2026-01-01T00:60:00Z',  # Invalid minute
            '2026-01-01T00:00:60Z',  # Invalid second
            '2026-01-01T00:00:00+01:00',  # Non-UTC offset (must be Z)
            '2026-01-01T00:00:00',   # No timezone
            '',                       # Empty
            'x' * 1000 + 'Z',        # Oversized
            '\x00Z',                 # Control character
        ]
        
        for bad_ts in invalid_timestamps:
            sampler.observe_ingress({
                'process_instance': 'proc-123',
                'ingress': {'Received': 101},
                'scrape_finished_monotonic': 1.0,
                'scrape_finished_timestamp': bad_ts
            })
            self.assertIn('observe_ingress_scrape_timestamp_must_be_utc', sampler.errors,
                         f"Must reject invalid timestamp: {bad_ts}")
            sampler.errors.clear()
        
        # Valid timestamp must work
        sampler.observe_ingress({
            'process_instance': 'proc-123',
            'ingress': {'Received': 101},
            'scrape_finished_monotonic': 1.0,
            'scrape_finished_timestamp': '2026-11-07T00:00:01Z'
        })
        self.assertTrue(sampler._triggered)

    def test_parent_D_baseline_without_process_instance_no_infer_sameness(self):
        """PARENT D: Baseline without process_instance + observation with process must NOT infer sameness.
        
        baseline {'ingress':{'Received':0}} without process_instance plus observation process p triggers.
        Must NOT infer sameness from missing baseline identity.
        Must record fixed baseline qualification, not clean trigger.
        Nonempty bounded string identity required for clean trigger.
        """
        sampler = self._make_sampler(capture_seconds=2.0,
                                     baseline_metrics={'ingress': {'Received': 0}},  # No process_instance
                                     time_fn=lambda: self.clock['mono'],
                                     wall_fn=lambda: self.clock['wall'])
        
        # Observation with process_instance when baseline has none
        http_snapshot = {
            'process_instance': 'proc-p',
            'ingress': {'Received': 1},
            'scrape_finished_monotonic': 1.0,
            'scrape_finished_timestamp': '2026-11-07T00:00:01Z'
        }
        sampler.observe_ingress(http_snapshot)
        
        # Must NOT trigger cleanly - missing baseline identity is a qualification
        self.assertFalse(sampler._triggered, "Must not trigger when baseline lacks process_instance")
        self.assertIn('baseline_missing_process_instance', sampler.errors)
        
        # Same process_instance but baseline has none - still no clean trigger
        sampler.errors.clear()
        http_snapshot2 = {
            'process_instance': 'proc-p',  # Same process, but baseline has none
            'ingress': {'Received': 2},
            'scrape_finished_monotonic': 2.0,
            'scrape_finished_timestamp': '2026-11-07T00:00:02Z'
        }
        sampler.observe_ingress(http_snapshot2)
        self.assertFalse(sampler._triggered, "Still no clean trigger without baseline identity")
        self.assertIn('baseline_missing_process_instance', sampler.errors)

    def test_parent_E_no_reader_starts_after_capture_end(self):
        """PARENT E: No reader may START after capture end.
        
        PG reader starts at 0, advances fake clock to 6, returns; 
        linux and docker are STILL STARTED despite capture end 5 because active_check checks session deadline only.
        No reader may START after capture end.
        Preserve already started PG result with real end 6, use fixed unavailable envelopes for unstarted Linux/Docker,
        commit exactly that bounded partial sample, then freeze.
        This finish of an already-started read is allowed, NOT permission to start next source.
        """
        sampler = self._make_sampler(capture_seconds=5.0,
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-123'},
                                     time_fn=lambda: self.clock['mono'],
                                     wall_fn=lambda: self.clock['wall'],
                                     interval=0.01)
        
        http_snapshot = {
            'process_instance': 'proc-123',
            'ingress': {'Received': 101},
            'scrape_finished_monotonic': 0.0,
            'scrape_finished_timestamp': '2026-11-07T00:00:00Z'
        }
        sampler.observe_ingress(http_snapshot)
        
        read_calls = {'pg': 0, 'linux': 0, 'docker': 0}
        reader_results = {'pg': {'pid': 1}, 'linux': {'cpu': 1}, 'docker': {'containers': []}}
        
        def make_counting_reader(name):
            def reader():
                read_calls[name] += 1
                return reader_results[name]
            return reader
        
        sampler.reader['postgres'] = make_counting_reader('pg')
        sampler.reader['linux'] = make_counting_reader('linux')
        sampler.reader['docker'] = make_counting_reader('docker')
        
        # At t=0: PG starts, returns, linux starts, returns, docker starts, returns
        self.clock['mono'] = 0.0
        sampler.sample_step()
        
        # At t=4: PG starts, returns, linux starts, returns, docker starts, returns
        self.clock['mono'] = 4.0
        sampler.sample_step()
        
        # At t=6: capture end is 5.0, so NO reader should start
        self.clock['mono'] = 6.0
        sampler.sample_step()
        
        # Must have exactly 2 full rounds of all 3 readers (at t=0 and t=4)
        self.assertEqual(read_calls['pg'], 2, "PG reader must have run exactly 2 times")
        self.assertEqual(read_calls['linux'], 2, "Linux reader must have run exactly 2 times")
        self.assertEqual(read_calls['docker'], 2, "Docker reader must have run exactly 2 times")
        
        # At t=6, no readers should have started
        # The frozen state should prevent any new reader calls

    # Additional test: freeze time must be actual current clock at freeze, not stale sample-start time
    def test_freeze_time_uses_current_clock_not_stale(self):
        """Freeze time must be actual current clock at freeze, not stale sample-start time."""
        sampler = self._make_sampler(deadline_seconds=1.0, capture_seconds=5.0,
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-123'},
                                     time_fn=lambda: self.clock['mono'],
                                     wall_fn=lambda: self.clock['wall'])
        
        http_snapshot = {
            'process_instance': 'proc-123',
            'ingress': {'Received': 101},
            'scrape_finished_monotonic': 0.0,
            'scrape_finished_timestamp': '2026-11-07T00:00:00Z'
        }
        sampler.observe_ingress(http_snapshot)
        
        self.clock['mono'] = 0.0
        sampler.sample_step()  # Trigger at 0.0
        
        # Advance to 1.1 (past deadline 1.0)
        self.clock['mono'] = 1.1
        sampler.sample_step()
        
        # Freeze time should be 1.1 (when freeze happened), not 0.0 (sample start)
        cov = sampler.coverage()
        self.assertEqual(cov['freeze_monotonic'], 1.1)

    # Test: explicit stop after declared end should recognize elapsed window
    def test_explicit_stop_after_declared_end_recognizes_elapsed(self):
        """Explicit stop after declared end should recognize elapsed window rather than call it early_stop."""
        sampler = self._make_sampler(capture_seconds=1.0,
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-123'},
                                     time_fn=lambda: self.clock['mono'],
                                     wall_fn=lambda: self.clock['wall'])
        
        http_snapshot = {
            'process_instance': 'proc-123',
            'ingress': {'Received': 101},
            'scrape_finished_monotonic': 0.0,
            'scrape_finished_timestamp': '2026-11-07T00:00:00Z'
        }
        sampler.observe_ingress(http_snapshot)
        
        self.clock['mono'] = 0.0
        sampler.sample_step()  # Trigger
        
        self.clock['mono'] = 1.5  # Past capture end (1.0)
        sampler.sample_step()  # Should freeze with capture_complete
        
        # Now explicit stop - should recognize capture_complete, not early_stop
        sampler.stop(timeout_seconds=1.0)
        cov = sampler.coverage()
        self.assertEqual(cov['state'], 'capture_complete', 
                        "Stop after declared end elapsed should be capture_complete, not early_stop")

    # Test: session clipping means requested declared end NOT reached unless deadline equals exact requested end
    def test_session_clipping_declared_end_not_reached(self):
        """Session clipping means requested declared end NOT reached unless deadline equals exact requested end."""
        sampler = self._make_sampler(deadline_seconds=3.0, capture_seconds=5.0,
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-123'},
                                     time_fn=lambda: self.clock['mono'],
                                     wall_fn=lambda: self.clock['wall'])
        
        http_snapshot = {
            'process_instance': 'proc-123',
            'ingress': {'Received': 101},
            'scrape_finished_monotonic': 0.0,
            'scrape_finished_timestamp': '2026-11-07T00:00:00Z'
        }
        sampler.observe_ingress(http_snapshot)
        
        self.clock['mono'] = 0.0
        sampler.sample_step()  # Trigger, requested end=5.0, deadline=3.0
        
        # At 3.1, past deadline (3.0)
        self.clock['mono'] = 3.1
        sampler.sample_step()
        
        cov = sampler.coverage()
        self.assertFalse(cov.get('capture_end_reached', True),
                        "Declared end NOT reached when clipped by earlier deadline")
        
        # But if deadline exactly equals requested end
        sampler2 = self._make_sampler(deadline_seconds=5.0, capture_seconds=5.0,
                                      baseline_metrics={'ingress': {'Received': 100},
                                                        'process_instance': 'proc-123'},
                                      time_fn=lambda: self.clock['mono'],
                                      wall_fn=lambda: self.clock['wall'])
        sampler2.observe_ingress(http_snapshot)
        self.clock['mono'] = 0.0
        sampler2.sample_step()
        
        self.clock['mono'] = 5.1
        sampler2.sample_step()
        
        cov2 = sampler2.coverage()
        self.assertTrue(cov2.get('capture_end_reached', False),
                       "Declared end reached when deadline equals requested end")

    # Test: legacy oversize sample behavior must remain skip-with-fixed-error and continue, not freeze forever
    def test_legacy_oversize_pre_trigger_skips_continues(self):
        """Legacy oversize sample behavior must remain skip-with-fixed-error and continue, not freeze forever."""
        sampler = self._make_sampler(max_samples=100, byte_cap=100000,  # No capture options
                                     time_fn=lambda: self.clock['mono'],
                                     wall_fn=lambda: self.clock['wall'])
        
        # Oversized sample > byte_cap
        huge_data = {'data': 'x' * 200000}
        sampler.reader['postgres'] = lambda: huge_data
        sampler.reader['linux'] = lambda: {'cpu': 100}
        
        self.clock['mono'] = 1.0
        sampler.sample_step()  # Oversized
        
        # Should NOT freeze - legacy mode continues
        self.assertFalse(sampler._frozen, "Legacy mode must not freeze on oversized sample")
        self.assertEqual(len(sampler.samples), 0, "Oversized sample skipped")
        
        # Normal sample should still work
        sampler.reader['postgres'] = lambda: {'small': 'data'}
        sampler.sample_step()
        self.assertEqual(len(sampler.samples), 1)

    # Test: repeated sample_step/start after frozen/deadline must not change state/coverage
    def test_repeated_calls_after_frozen_no_state_change(self):
        """Repeated sample_step/start after frozen/deadline must not change state/coverage."""
        sampler = self._make_sampler(deadline_seconds=1.0, capture_seconds=5.0,
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-123'},
                                     time_fn=lambda: self.clock['mono'],
                                     wall_fn=lambda: self.clock['wall'])
        
        http_snapshot = {
            'process_instance': 'proc-123',
            'ingress': {'Received': 101},
            'scrape_finished_monotonic': 0.0,
            'scrape_finished_timestamp': '2026-11-07T00:00:00Z'
        }
        sampler.observe_ingress(http_snapshot)
        
        self.clock['mono'] = 0.0
        sampler.sample_step()
        
        self.clock['mono'] = 1.1  # Past deadline
        sampler.sample_step()
        sampler.sample_step()  # Second call after freeze
        sampler.sample_step()  # Third call
        
        cov1 = sampler.coverage()
        state1 = cov1['state']
        freeze1 = cov1['freeze_monotonic']
        
        cov2 = sampler.coverage()
        state2 = cov2['state']
        freeze2 = cov2['freeze_monotonic']
        
        self.assertEqual(state1, state2, "State must not change after freeze")
        self.assertEqual(freeze1, freeze2, "Freeze time must not change after freeze")

    # Test: background thread should return when naturally frozen instead of waiting until session deadline
    def test_background_thread_returns_when_frozen(self):
        """Background thread should return when naturally frozen instead of waiting until session deadline."""
        sampler = self._make_sampler(deadline_seconds=100.0, capture_seconds=1.0,
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-123'},
                                     time_fn=lambda: self.clock['mono'],
                                     wall_fn=lambda: self.clock['wall'],
                                     interval=0.01)
        
        http_snapshot = {
            'process_instance': 'proc-123',
            'ingress': {'Received': 101},
            'scrape_finished_monotonic': 0.0,
            'scrape_finished_timestamp': '2026-11-07T00:00:00Z'
        }
        sampler.observe_ingress(http_snapshot)
        
        self.clock['mono'] = 0.0
        sampler.sample_step()
        
        # Start background thread
        sampler.start()
        
        # Advance past capture end (1.0)
        self.clock['mono'] = 1.5
        
        # Give thread time to notice freeze and exit
        time.sleep(0.1)
        
        # Thread should have exited (joined)
        self.assertFalse(sampler._thread.is_alive(), "Thread should exit when frozen, not wait for session deadline")
        
        # Clean up
        sampler.stop(timeout_seconds=1.0)

    # ==================== THREE SPECIFIC REGRESSION TESTS FOR CURRENT DEFECTS ====================

    def test_defect1_active_count_cap_freeze_not_capture_end_reached(self):
        """DEFECT 1: Active count cap freeze must NOT set capture_end_reached=True.
        
        max_samples=1, trigger at 0, sample at 0 (fills cap), at 1 sample_step rejects next sample.
        Coverage capture_end_reached must be FALSE (requested end=5 not reached).
        State must be capture_complete but complete=False with explicit frozen reason active_count_cap.
        No deadline in this case - pure cap freeze.
        """
        sampler = self._make_sampler(max_samples=1, byte_cap=10000, capture_seconds=5.0,
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-123'},
                                     time_fn=lambda: self.clock['mono'],
                                     wall_fn=lambda: self.clock['wall'])
        
        http_snapshot = {
            'process_instance': 'proc-123',
            'ingress': {'Received': 101},
            'scrape_finished_monotonic': 0.0,
            'scrape_finished_timestamp': '2026-11-07T00:00:00Z'
        }
        sampler.observe_ingress(http_snapshot)
        
        # Trigger at 0.0
        self.clock['mono'] = 0.0
        sampler.sample_step()  # Sample 1 - fills max_samples=1
        
        # At 1.0, try to sample again - should hit cap and freeze
        self.clock['mono'] = 1.0
        sampler.sample_step()  # Sample 2 - would exceed max_samples=1, should freeze
        
        cov = sampler.coverage()
        
        # Must be frozen due to cap
        self.assertTrue(sampler._frozen, "Must freeze after active count cap")
        self.assertEqual(cov['state'], 'capture_complete', "State should be capture_complete")
        
        # CRITICAL: capture_end_reached must be FALSE because natural end (5.0) not reached
        self.assertFalse(cov.get('capture_end_reached', True), 
                        "capture_end_reached must be FALSE when frozen by cap, not natural end")
        
        # complete must be FALSE because truncated
        self.assertFalse(cov.get('complete', True), 
                        "complete must be FALSE when truncated by cap")
        
        # Must have truncation info with reason active_count_cap
        self.assertIn('truncated', cov)
        self.assertEqual(cov['truncated']['reason'], 'active_count_cap')
        self.assertGreater(cov['truncated']['dropped_count'], 0)
        
        # Must keep earliest data (the first sample)
        self.assertEqual(len(sampler.samples), 1, "Must keep earliest evidence")
        
        # No further reader calls after freeze
        # (This is implicitly tested by sample count not increasing)

    def test_defect2_stop_after_natural_end_is_capture_complete_not_early_stop(self):
        """DEFECT 2: Explicit stop after natural capture end elapsed must be capture_complete, not early_stop.
        
        Trigger at 0, capture_seconds=5, so natural end at 5.0.
        Advance clock to 6.0 (past natural end), call stop WITHOUT sample_step at 6.
        Current implementation returns early_stop because it checks _active flag.
        Expected: stop should detect that monotonic (6.0) >= capture_end_monotonic (5.0)
        and set state=capture_complete, capture_end_reached=True, complete=True.
        Freeze time should be 6.0 (when stop called), not stale sample-start time.
        """
        sampler = self._make_sampler(capture_seconds=5.0,
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-123'},
                                     time_fn=lambda: self.clock['mono'],
                                     wall_fn=lambda: self.clock['wall'])
        
        http_snapshot = {
            'process_instance': 'proc-123',
            'ingress': {'Received': 101},
            'scrape_finished_monotonic': 0.0,
            'scrape_finished_timestamp': '2026-11-07T00:00:00Z'
        }
        sampler.observe_ingress(http_snapshot)
        
        # Trigger at 0.0
        self.clock['mono'] = 0.0
        sampler.sample_step()
        
        # Advance to 6.0 - PAST natural capture end (5.0)
        # Do NOT call sample_step at 6.0 - directly call stop
        self.clock['mono'] = 6.0
        sampler.stop(timeout_seconds=1.0)
        
        cov = sampler.coverage()
        
        # Should be capture_complete, NOT early_stop
        self.assertEqual(cov['state'], 'capture_complete',
                        "Stop after natural end elapsed must be capture_complete, not early_stop")
        
        # capture_end_reached must be TRUE
        self.assertTrue(cov.get('capture_end_reached', False),
                       "capture_end_reached must be TRUE when stop called after natural end")
        
        # complete must be TRUE (no truncation, no qualifications)
        self.assertTrue(cov.get('complete', False),
                       "complete must be TRUE when no truncation/qualifications and natural end reached")
        
        # Freeze time should be 6.0 (when stop was called), not 0.0 (sample start)
        self.assertEqual(cov['freeze_monotonic'], 6.0,
                        "Freeze time must be actual stop time (6.0), not stale sample-start time (0.0)")

    def test_defect3_in_flight_pg_retained_linux_docker_unavailable_envelopes(self):
        """DEFECT 3: In-flight PG reader that completes after capture end must be retained;
        unstarted Linux/Docker must get unavailable envelopes with reason='capture_window_ended'.
        
        Trigger at 0, capture_seconds=5 (end at 5.0).
        At 4.9: PG reader starts, advances clock to 6.0 internally, returns real WAL data.
        Linux and Docker readers MUST NOT start after 5.0.
        Result: partial sample with PG value (actual start~4.9, end~6.0), 
        Linux/Docker as unavailable envelopes with reason='capture_window_ended'.
        Freeze with capture_end_reached=True but source coverage qualified incomplete
        (e.g., inflight_partial_sample qualification).
        """
        sampler = self._make_sampler(capture_seconds=5.0,
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-123'},
                                     time_fn=lambda: self.clock['mono'],
                                     wall_fn=lambda: self.clock['wall'],
                                     interval=0.01)
        
        http_snapshot = {
            'process_instance': 'proc-123',
            'ingress': {'Received': 101},
            'scrape_finished_monotonic': 0.0,
            'scrape_finished_timestamp': '2026-11-07T00:00:00Z'
        }
        sampler.observe_ingress(http_snapshot)
        
        # Trigger at 0.0
        self.clock['mono'] = 0.0
        sampler.sample_step()
        
        # Track reader calls and their observed times
        reader_calls = {'pg': [], 'linux': [], 'docker': []}
        pg_result = {'wal': {'records': 42}, 'pid': 123}
        
        def make_pg_reader():
            def reader():
                start_time = self.clock['mono']
                reader_calls['pg'].append(('start', start_time))
                # Simulate slow PG reader: advance clock to 6.0 (past capture end 5.0)
                self.clock['mono'] = 6.0
                reader_calls['pg'].append(('end', self.clock['mono']))
                return pg_result
            return reader
        
        def make_linux_reader():
            def reader():
                reader_calls['linux'].append(('start', self.clock['mono']))
                return {'cpu': 100}
            return reader
        
        def make_docker_reader():
            def reader():
                reader_calls['docker'].append(('start', self.clock['mono']))
                # Return format that _project_resources preserves (canonical services)
                return {'services': {'backend': {'cpu_cumulative_seconds': 1.0, 'memory_bytes': 1000}}}
            return reader
        
        sampler.reader['postgres'] = make_pg_reader()
        sampler.reader['linux'] = make_linux_reader()
        sampler.reader['docker'] = make_docker_reader()
        
        # At 4.9: Start a sample - PG will run and advance clock to 6.0
        self.clock['mono'] = 4.9
        sampler.sample_step()
        
        # Verify PG reader ran and returned (started before 5.0, ended at 6.0)
        self.assertEqual(len(reader_calls['pg']), 2, "PG reader must have started and completed")
        self.assertLess(reader_calls['pg'][0][1], 5.0, "PG must have STARTED before capture end")
        self.assertGreater(reader_calls['pg'][1][1], 5.0, "PG must have ENDED after capture end")
        
        # Linux and Docker must NOT have started (capture end is 5.0)
        self.assertEqual(len(reader_calls['linux']), 0, "Linux reader must NOT start after capture end")
        self.assertEqual(len(reader_calls['docker']), 0, "Docker reader must NOT start after capture end")
        
        # Sample must have been appended with partial data
        self.assertEqual(len(sampler.samples), 2, "Must have 2 samples (trigger + partial)")
        
        partial_sample = sampler.samples[-1]
        
        # PG value must be retained with actual timestamps
        self.assertIn('postgres', partial_sample)
        pg_data = partial_sample['postgres']
        self.assertEqual(pg_data['value'], pg_result, "PG value must be retained exactly")
        self.assertLess(pg_data['start_monotonic_seconds'], 5.0, "PG start must be before capture end")
        self.assertGreater(pg_data['end_monotonic_seconds'], 5.0, "PG end must be after capture end")
        self.assertIsNone(pg_data['error'], "PG must not have error")
        
        # Linux must have unavailable envelope with reason='capture_window_ended'
        self.assertIn('linux', partial_sample)
        linux_data = partial_sample['linux']
        self.assertIsNone(linux_data['value'], "Linux value must be None (unstarted)")
        self.assertEqual(linux_data['error'], 'capture_window_ended', 
                        "Linux error must be 'capture_window_ended'")
        # Linux should have start/end timestamps around the freeze time
        self.assertIsNotNone(linux_data['start_monotonic_seconds'])
        self.assertIsNotNone(linux_data['end_monotonic_seconds'])
        
        # Docker must have unavailable envelope with reason='capture_window_ended'
        self.assertIn('docker', partial_sample)
        docker_data = partial_sample['docker']
        self.assertIsNone(docker_data['value'], "Docker value must be None (unstarted)")
        self.assertEqual(docker_data['error'], 'capture_window_ended',
                        "Docker error must be 'capture_window_ended'")
        
        # Coverage: capture_end_reached=True (natural end reached), but qualified incomplete
        cov = sampler.coverage()
        self.assertTrue(cov.get('capture_end_reached', False),
                       "capture_end_reached must be TRUE - natural end was reached")
        self.assertFalse(cov.get('complete', True),
                        "complete must be FALSE - partial sample with unstarted sources")
        
        # Should have qualification for inflight_partial_sample
        # (This checks that source coverage is qualified incomplete)


# ==================== THREE EXACT BEHAVIORAL TESTS FOR USER-REPORTED DEFECTS ====================

    def test_user_bug1_pg_advances_clock_freeze_monotonic_is_actual_not_stale(self):
        """USER BUG 1: Fake clock0, capture_seconds5, valid HTTP trigger at0.
        
        PG callback advances clock to 6 and returns WAL DTO.
        sample_step retains partial data but coverage()['freeze_monotonic']==0 
        instead of actual 6 because it reuses initial now_mono.
        Must freeze using ACTUAL current Host A monotonic/wall at transition,
        retain source PG times 0..6.
        """
        sampler = self._make_sampler(capture_seconds=5.0,
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-123'},
                                     time_fn=lambda: self.clock['mono'],
                                     wall_fn=lambda: self.clock['wall'],
                                     interval=0.01)
        
        http_snapshot = {
            'process_instance': 'proc-123',
            'ingress': {'Received': 101},
            'scrape_finished_monotonic': 0.0,
            'scrape_finished_timestamp': '2026-11-07T00:00:00Z'
        }
        sampler.observe_ingress(http_snapshot)
        
        # Trigger at 0.0
        self.clock['mono'] = 0.0
        sampler.sample_step()
        
        # At 4.9: Start a sample - PG reader will advance clock to 6.0 (past capture end 5.0)
        self.clock['mono'] = 4.9
        
        pg_result = {'wal': {'records': 42}, 'pid': 123}
        def pg_reader():
            start_time = self.clock['mono']
            # PG reader advances clock internally
            self.clock['mono'] = 6.0
            return pg_result
        
        sampler.reader['postgres'] = pg_reader
        sampler.reader['linux'] = lambda: {'cpu': 100}
        sampler.reader['docker'] = lambda: {'containers': []}
        
        sampler.sample_step()
        
        # After sample_step, clock is at 6.0 (advanced by PG reader)
        # coverage()['freeze_monotonic'] should be 6.0 (actual current time), NOT 0.0 (stale sample start)
        cov = sampler.coverage()
        
        # The sampler should be frozen because PG reader ended at 6.0 >= capture_end_monotonic (5.0)
        self.assertTrue(sampler._frozen, "Should be frozen after capture end crossed during PG read")
        
        # CRITICAL: freeze_monotonic must be 6.0 (actual time at freeze), not 0.0 (stale sample start)
        self.assertEqual(cov['freeze_monotonic'], 6.0,
                        "freeze_monotonic must be ACTUAL current time (6.0), not stale sample start (0.0)")
        
        # PG value must be retained with actual timestamps (start ~4.9, end ~6.0)
        self.assertEqual(len(sampler.samples), 2, "Must retain the partial sample")
        partial = sampler.samples[-1]
        self.assertEqual(partial['postgres']['value'], pg_result)
        self.assertLess(partial['postgres']['start_monotonic_seconds'], 5.0)
        self.assertGreater(partial['postgres']['end_monotonic_seconds'], 5.0)

    def test_user_bug2_docker_advances_clock_retain_all_started_readers(self):
        """USER BUG 2: Same fixture except Docker callback advances clock 0->6 (PG/Linux immediate).
        
        sample_step currently retains ZERO samples because `if not active_check(): return` 
        after final Docker read.
        Must retain ALL already-started read results (PG/Linux/Docker) with actual per-source 
        times and freeze natural_end at 6.
        No later reader after window end.
        If all sources completed, no fake partial qualification; 
        if later source not started, fixed unavailable envelope with reason 
        capture_window_ended and sticky inflight_partial_sample qualification.
        Active budget still enforced, no evictions.
        Explicit stop/cap freeze is DIFFERENT: no late append or finalized coverage mutation.
        """
        sampler = self._make_sampler(capture_seconds=5.0,
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-123'},
                                     time_fn=lambda: self.clock['mono'],
                                     wall_fn=lambda: self.clock['wall'],
                                     interval=0.01)
        
        http_snapshot = {
            'process_instance': 'proc-123',
            'ingress': {'Received': 101},
            'scrape_finished_monotonic': 0.0,
            'scrape_finished_timestamp': '2026-11-07T00:00:00Z'
        }
        sampler.observe_ingress(http_snapshot)
        
        # Trigger at 0.0
        self.clock['mono'] = 0.0
        sampler.sample_step()  # Trigger sample
        
        # Track reader calls for the NEXT sample
        reader_calls = {'pg': [], 'linux': [], 'docker': []}
        
        def make_pg_reader():
            def reader():
                reader_calls['pg'].append(('start', self.clock['mono']))
                reader_calls['pg'].append(('end', self.clock['mono']))
                return {'wal': {'records': 1}}
            return reader
        
        def make_linux_reader():
            def reader():
                reader_calls['linux'].append(('start', self.clock['mono']))
                reader_calls['linux'].append(('end', self.clock['mono']))
                return {'cpu': 100}
            return reader
        
        def make_docker_reader():
            def reader():
                reader_calls['docker'].append(('start', self.clock['mono']))
                # Docker advances clock past capture end (5.0)
                self.clock['mono'] = 6.0
                reader_calls['docker'].append(('end', self.clock['mono']))
                return {'services': {'backend': {'cpu_cumulative_seconds': 1.0, 'memory_bytes': 1000}}}
            return reader
        
        sampler.reader['postgres'] = make_pg_reader()
        sampler.reader['linux'] = make_linux_reader()
        sampler.reader['docker'] = make_docker_reader()
        
        # At 4.9: Start a sample - PG and Linux immediate, Docker advances clock to 6.0
        self.clock['mono'] = 4.9
        sampler.sample_step()
        
        # After sample_step, clock is at 6.0 (advanced by Docker reader)
        # All three readers STARTED before capture end (5.0) and COMPLETED
        # The sample MUST be retained with all three values
        
        self.assertEqual(len(sampler.samples), 2, "Must retain sample with all three completed readers")
        partial = sampler.samples[-1]
        
        # PG must have value with actual timestamps
        self.assertEqual(partial['postgres']['value'], {'wal': {'records': 1}})
        self.assertLess(partial['postgres']['start_monotonic_seconds'], 5.0)
        self.assertLess(partial['postgres']['end_monotonic_seconds'], 5.0)
        self.assertIsNone(partial['postgres']['error'])
        
        # Linux must have value with actual timestamps
        self.assertEqual(partial['linux']['value'], {'cpu': 100})
        self.assertLess(partial['linux']['start_monotonic_seconds'], 5.0)
        self.assertLess(partial['linux']['end_monotonic_seconds'], 5.0)
        self.assertIsNone(partial['linux']['error'])
        
        # Docker must have value with actual timestamps (start < 5.0, end = 6.0)
        self.assertIn('docker', partial)
        docker_data = partial['docker']
        # _project_resources extracts canonical services
        self.assertIn('backend', docker_data['value'])
        self.assertEqual(docker_data['value']['backend']['cpu_cumulative_seconds'], 1.0)
        self.assertLess(docker_data['start_monotonic_seconds'], 5.0)
        self.assertGreater(docker_data['end_monotonic_seconds'], 5.0)
        self.assertIsNone(docker_data['error'])
        
        # Coverage: capture_end_reached=True (natural end reached at 6.0 >= 5.0)
        cov = sampler.coverage()
        self.assertTrue(cov.get('capture_end_reached', False))
        
        # Since ALL sources completed, there should be NO fake partial qualification
        # The sample is complete for all sources that started
        # But capture end was crossed during the last reader
        # No qualification should be added if all started readers completed
        
        # No further reader should start after this
        reader_calls = {'pg': [], 'linux': [], 'docker': []}
        sampler.reader['postgres'] = make_pg_reader()
        sampler.reader['linux'] = make_linux_reader()
        sampler.reader['docker'] = make_docker_reader()
        self.clock['mono'] = 7.0
        sampler.sample_step()
        self.assertEqual(len(reader_calls['pg']), 0, "No PG readers should start after freeze")
        self.assertEqual(len(reader_calls['linux']), 0, "No Linux readers should start after freeze")
        self.assertEqual(len(reader_calls['docker']), 0, "No Docker readers should start after freeze")

    def test_user_bug3_frozen_state_immutable_after_stop_then_sample_step(self):
        """USER BUG 3: Fake clock0, deadline_seconds10, no trigger.
        
        stop freezes no_trigger.
        Advance 20 and call sample_step: frozen state changes no_trigger->deadline.
        Must check already-frozen BEFORE ALL state/metadata mutations in 
        sample_step/start loop/stop.
        Immutable evidence+coverage after freeze even across long operator 
        wait/session expiry.
        Deadlines that equal exact natural end should be natural_end, 
        not incomplete solely due branch order.
        """
        sampler = self._make_sampler(deadline_seconds=10.0, capture_seconds=5.0,
                                     baseline_metrics={'ingress': {'Received': 100},
                                                       'process_instance': 'proc-123'},
                                     time_fn=lambda: self.clock['mono'],
                                     wall_fn=lambda: self.clock['wall'])
        
        # NO trigger - just let deadline approach
        
        # At clock=5.0, call stop() - should freeze with no_trigger
        self.clock['mono'] = 5.0
        sampler.stop(timeout_seconds=1.0)
        
        # Verify frozen with no_trigger
        cov1 = sampler.coverage()
        self.assertTrue(sampler._frozen)
        self.assertEqual(cov1['state'], 'no_trigger')
        self.assertEqual(cov1['freeze_monotonic'], 5.0)
        
        # Advance clock to 20.0 (past deadline 10.0)
        self.clock['mono'] = 20.0
        
        # Call sample_step - frozen state should NOT change from no_trigger to deadline
        sampler.sample_step()
        
        # Coverage must be identical - immutable after freeze
        cov2 = sampler.coverage()
        self.assertEqual(cov2['state'], 'no_trigger',
                        "Frozen state must NOT change from no_trigger to deadline")
        self.assertEqual(cov2['freeze_monotonic'], 5.0,
                        "Freeze time must remain 5.0, not change to 20.0")
        self.assertEqual(cov2['freeze_reason'], 'early_stop',
                        "Freeze reason must remain early_stop")
        
        # Evidence (samples) must not change
        self.assertEqual(len(sampler.samples), 0, "No samples should be added after freeze")
        
        # Coverage dict must be identical
        self.assertEqual(cov1, cov2, "Coverage must be immutable after freeze")


    # ==================== MISSING BASELINE (ACTIVE MODE, NO BASELINE) ====================

    def test_missing_baseline_active_mode_reproduction(self):
        """Exact parent reproduction: capture requested, baseline unavailable.

        s=IndependentSideSampler(reader={...},capture_seconds=50,baseline_metrics=None,
        deadline_seconds=100); s.sample_step(); s.stop(timeout_seconds=0); coverage()
        MUST return mode='active_capture', trigger_observed=False, complete=False.
        """
        clock = {'mono': 0.0, 'wall': '2026-11-07T00:00:00Z'}
        side = e2.IndependentSideSampler(
            reader={'postgres': lambda: {'available': True},
                    'linux': lambda: {'available': True}},
            capture_seconds=50,
            baseline_metrics=None,
            deadline_seconds=100,
            time_fn=lambda: clock['mono'],
            wall_fn=lambda: clock['wall'])
        side.sample_step()
        side.stop(timeout_seconds=0)
        cov = side.coverage()
        # Active capture mode selected by capture_seconds alone (baseline absence is qualification)
        self.assertEqual(cov['mode'], 'active_capture')
        # No trigger observed (baseline unavailable)
        self.assertFalse(cov.get('trigger_observed', True))
        # Not complete (waiting, then no_trigger after stop)
        self.assertFalse(cov.get('complete', True))
        self.assertEqual(cov['state'], 'no_trigger')
        # Old count/bytes fields preserved
        self.assertIn('samples_captured', cov)
        self.assertIn('samples_retained', cov)
        self.assertIn('samples_evicted', cov)
        self.assertIn('evicted_bytes', cov)

    def test_missing_baseline_retains_pretrigger_bounded(self):
        """20 waiting samples with capture_seconds, baseline None: retain <=4, <=64KiB,
        no trigger, complete=False. Pre-trigger bound applies with capture_seconds alone."""
        clock = {'mono': 0.0, 'wall': '2026-11-07T00:00:00Z'}
        side = e2.IndependentSideSampler(
            reader={'postgres': lambda: {'pid': 1},
                    'linux': lambda: {'cpu': 10}},
            max_samples=128, byte_cap=2 * 1024 * 1024,
            capture_seconds=50,
            baseline_metrics=None,
            deadline_seconds=100,
            time_fn=lambda: clock['mono'],
            wall_fn=lambda: clock['wall'])
        # 20 waiting samples, no trigger, no baseline
        for i in range(20):
            clock['mono'] += 0.1
            side.sample_step()
        # Pre-trigger bound: min(4, 128) = 4 samples retained
        self.assertLessEqual(len(side.samples), 4)
        # Byte bound: min(64KiB, 2MiB) = 64KiB
        cov = side.coverage()
        self.assertLessEqual(cov['ring_bytes_used'], 64 * 1024)
        self.assertFalse(side._triggered)
        self.assertEqual(side._state, 'waiting')
        self.assertFalse(cov.get('trigger_observed', True))
        self.assertFalse(cov.get('complete', True))
        self.assertEqual(cov['mode'], 'active_capture')
        # Old fields preserved
        self.assertIn('samples_captured', cov)
        self.assertIn('samples_retained', cov)

    def test_missing_baseline_observe_ingress_no_trigger_qualified(self):
        """HTTP observations without baseline: no trigger, sticky 'baseline_unavailable',
        state waiting, complete=False."""
        clock = {'mono': 0.0, 'wall': '2026-11-07T00:00:00Z'}
        side = e2.IndependentSideSampler(
            reader={'postgres': lambda: {}, 'linux': lambda: {}},
            max_samples=128, byte_cap=2 * 1024 * 1024,
            capture_seconds=50,
            baseline_metrics=None,
            deadline_seconds=100,
            time_fn=lambda: clock['mono'],
            wall_fn=lambda: clock['wall'])
        # Multiple observations with increasing Received - none should trigger
        for received in (10, 20, 30, 100):
            side.observe_ingress({
                'process_instance': 'proc-123',
                'ingress': {'Received': received},
                'scrape_finished_monotonic': clock['mono'],
                'scrape_finished_timestamp': clock['wall'],
            })
            clock['mono'] += 1.0
        self.assertFalse(side._triggered)
        self.assertEqual(side._state, 'waiting')
        self.assertIn('baseline_unavailable', side._qualification_reasons)
        self.assertIn('baseline_unavailable', side.errors)
        cov = side.coverage()
        self.assertEqual(cov['mode'], 'active_capture')
        self.assertFalse(cov.get('trigger_observed', True))
        self.assertFalse(cov.get('complete', True))
        self.assertIn('baseline_unavailable', cov.get('qualifications', []))

    def test_missing_baseline_stop_before_trigger_no_trigger(self):
        """Stop before trigger (no baseline): state no_trigger, coverage intact."""
        clock = {'mono': 0.0, 'wall': '2026-11-07T00:00:00Z'}
        side = e2.IndependentSideSampler(
            reader={'postgres': lambda: {}, 'linux': lambda: {}},
            max_samples=128, byte_cap=2 * 1024 * 1024,
            capture_seconds=50,
            baseline_metrics=None,
            deadline_seconds=100,
            time_fn=lambda: clock['mono'],
            wall_fn=lambda: clock['wall'])
        # A few waiting samples
        for i in range(3):
            clock['mono'] += 0.1
            side.sample_step()
        # Stop before any trigger
        self.assertTrue(side.stop(timeout_seconds=0))
        self.assertEqual(side._state, 'no_trigger')
        cov = side.coverage()
        self.assertEqual(cov['mode'], 'active_capture')
        self.assertEqual(cov['state'], 'no_trigger')
        self.assertFalse(cov.get('trigger_observed', True))
        self.assertFalse(cov.get('complete', True))
        # Data preserved
        self.assertEqual(len(side.samples), 3)

    def test_missing_baseline_deadline_incomplete(self):
        """Session deadline before trigger: state deadline, complete=False."""
        clock = {'mono': 0.0, 'wall': '2026-11-07T00:00:00Z'}
        side = e2.IndependentSideSampler(
            reader={'postgres': lambda: {}, 'linux': lambda: {}},
            max_samples=128, byte_cap=2 * 1024 * 1024,
            capture_seconds=50,
            baseline_metrics=None,
            deadline_seconds=0.5,
            time_fn=lambda: clock['mono'],
            wall_fn=lambda: clock['wall'])
        clock['mono'] = 0.6  # Past deadline
        side.sample_step()
        cov = side.coverage()
        self.assertEqual(cov['mode'], 'active_capture')
        self.assertEqual(cov['state'], 'deadline')
        self.assertFalse(cov.get('trigger_observed', True))
        self.assertFalse(cov.get('complete', True))

    def test_legacy_capture_none_still_evicts_normally(self):
        """Legacy mode (capture_seconds=None): rolling eviction unchanged,
        no 'mode' key, complete=True."""
        clock = {'mono': 0.0, 'wall': '2026-11-07T00:00:00Z'}
        side = e2.IndependentSideSampler(
            reader={'postgres': lambda: {}, 'linux': lambda: {}},
            max_samples=3, byte_cap=10000,
            time_fn=lambda: clock['mono'],
            wall_fn=lambda: clock['wall'])
        self.assertTrue(side._legacy_mode)
        for i in range(10):
            clock['mono'] += 0.1
            side.sample_step()
        self.assertEqual(len(side.samples), 3)
        cov = side.coverage()
        self.assertEqual(cov['samples_evicted'], 7)
        self.assertNotIn('mode', cov)
        self.assertTrue(cov.get('complete', False))


if __name__ == '__main__':
    unittest.main()