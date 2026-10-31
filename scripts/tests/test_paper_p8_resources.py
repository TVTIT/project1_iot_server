"""Unit tests for B-only bounded process resource sampling helper (scripts/paper_p8_resources.py).

Tests:
- Realistic reader process keys (pid, starttime) and safe JSON export
- PID reuse detection as process_loss without negative or bogus CPU
- Pre-existing new child detection as process_appeared without lifetime ticks overcount
- Baseline, counter reset, process loss, process appearance, and unavailable handling
- Bounded snapshots cap, deadline enforcement (even when interval > deadline)
- Stop() unblocked by slow reader: lock not held during reader I/O or join; finite join timeout
- Strict timing validation rejecting NaN, Inf, non-finite values
- CSV error propagation on malformed row and injected write failure without swallowing
- Exclusive private CSV export without fake zeros
- Safe FD counting scoped to root process only
- Report scopes qualification (fd root_process_only, rss sum_sampled_RSS_may_double_count_shared_pages_not_peak)
- Strict JSON serialization with allow_nan=False and no secrets/PIDs
- Standalone subprocess import with PYTHONPATH removed
- Short real own-process smoke test (< 2 seconds) without external polling loops
"""
from unittest.mock import patch
import csv
import json
import math
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import unittest

# Anchor repository root to sys.path so test can run standalone without PYTHONPATH
REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.paper_p8_resources import (
    DEFAULT_DEADLINE_SECONDS,
    DEFAULT_MAX_SNAPSHOTS,
    DEFAULT_SAMPLE_INTERVAL_SECONDS,
    GENERATOR_RESOURCES_CSV_COLUMNS,
    BoundedProcessSampler,
    GeneratorResourceSampler,
    _get_default_process_tree_reader,
    _safe_fd_count,
    write_generator_resources_csv,
    write_resources_generator_csv,
)


class TestPaperP8Resources(unittest.TestCase):
    """Test suite for GeneratorResourceSampler and resources-generator CSV writer."""

    def test_reader_realistic_process_keys_and_safe_exports(self):
        """Verify sampling with composite (pid, starttime) keys and PID-free safe exports."""
        state = {
            'call_count': 0,
            'current_time': 100.0,
        }

        def mock_time():
            return state['current_time']

        def mock_reader(pid, proc_root=Path('/proc')):
            state['call_count'] += 1
            if state['call_count'] == 1:
                return {
                    'available': True,
                    'rss_bytes': 30 * 1024 * 1024,
                    'cpu_ticks': 100,
                    'process_count': 3,
                    'processes': {
                        (1000, 5000): {'ticks': 50, 'rss_bytes': 10 * 1024 * 1024},
                        (1001, 5010): {'ticks': 30, 'rss_bytes': 10 * 1024 * 1024},
                        (1002, 5020): {'ticks': 20, 'rss_bytes': 10 * 1024 * 1024},
                    },
                }
            return {
                'available': True,
                'rss_bytes': 35 * 1024 * 1024,
                'cpu_ticks': 250,
                'process_count': 3,
                'processes': {
                    (1000, 5000): {'ticks': 120, 'rss_bytes': 12 * 1024 * 1024},
                    (1001, 5010): {'ticks': 70, 'rss_bytes': 11 * 1024 * 1024},
                    (1002, 5020): {'ticks': 60, 'rss_bytes': 12 * 1024 * 1024},
                },
            }

        sampler = GeneratorResourceSampler(
            root_pid=1000,
            interval=0.5,
            reader=mock_reader,
            time_fn=mock_time,
            host_cpus=2,
            clk_tck=100,
        )

        sampler.start()
        report0 = sampler.report()
        self.assertEqual(len(report0['samples']), 1)
        s0 = report0['samples'][0]
        self.assertTrue(s0['available'])
        self.assertIsNone(s0['cpu_percent'])
        self.assertIsNone(s0['delta_ticks'])
        self.assertEqual(s0['reason'], 'first_sample_baseline_unavailable')
        self.assertEqual(s0['rss_bytes'], 30 * 1024 * 1024)

        state['current_time'] = 100.5
        with sampler._lock:
            proc, fd_count = sampler._do_reader_io()
            sampler._commit_snapshot_locked(proc, fd_count)

        sampler.stop()
        report = sampler.report()
        self.assertEqual(len(report['samples']), 2)
        s1 = report['samples'][1]
        self.assertTrue(s1['available'])
        self.assertEqual(s1['delta_ticks'], 150)
        self.assertEqual(s1['interval_seconds'], 0.5)
        self.assertAlmostEqual(s1['cpu_percent'], 150.0, places=4)
        self.assertIsNone(s1['reason'])
        self.assertEqual(s1['rss_bytes'], 35 * 1024 * 1024)

        summary = report['summary']
        self.assertEqual(summary['total_samples'], 2)
        self.assertEqual(summary['available_samples'], 2)
        self.assertEqual(summary['cpu_available_samples'], 1)
        self.assertEqual(summary['max_sample_rss_bytes'], 35 * 1024 * 1024)
        self.assertEqual(summary['missing_reasons'], ['first_sample_baseline_unavailable'])

        # Verify safe JSON export without NaN/Inf, PIDs, cmdlines, or secrets
        report_json = json.dumps(report, allow_nan=False)
        self.assertNotIn('1000', report_json)
        self.assertNotIn('1001', report_json)
        self.assertNotIn('1002', report_json)
        self.assertNotIn('5000', report_json)
        self.assertNotIn('processes', report_json)

    def test_pid_reuse_marks_cpu_unavailable_as_process_loss(self):
        """Verify PID reuse with different starttime triggers process_loss without negative/bogus CPU."""
        state = {'current_time': 200.0, 'step': 0}

        def mock_time():
            return state['current_time']

        def mock_reader(pid, proc_root=Path('/proc')):
            state['step'] += 1
            if state['step'] == 1:
                return {
                    'available': True,
                    'rss_bytes': 10 * 1024 * 1024,
                    'cpu_ticks': 50,
                    'process_count': 1,
                    'processes': {
                        (2000, 1000): {'ticks': 50, 'rss_bytes': 10 * 1024 * 1024},
                    },
                }
            return {
                'available': True,
                'rss_bytes': 10 * 1024 * 1024,
                'cpu_ticks': 5,
                'process_count': 1,
                'processes': {
                    (2000, 2000): {'ticks': 5, 'rss_bytes': 10 * 1024 * 1024},
                },
            }

        sampler = GeneratorResourceSampler(
            root_pid=2000,
            interval=0.5,
            reader=mock_reader,
            time_fn=mock_time,
            host_cpus=1,
            clk_tck=100,
        )

        sampler.start()
        state['current_time'] = 200.5
        with sampler._lock:
            proc, fd_count = sampler._do_reader_io()
            sampler._commit_snapshot_locked(proc, fd_count)
        sampler.stop()

        report = sampler.report()
        s1 = report['samples'][1]
        self.assertTrue(s1['available'])
        self.assertIsNone(s1['cpu_percent'], "PID reuse must not result in bogus or negative CPU")
        self.assertIsNone(s1['delta_ticks'])
        self.assertEqual(s1['reason'], 'process_loss')

    def test_preexisting_new_child_marks_cpu_unavailable_process_appeared(self):
        """Verify new child observed does not dump lifetime ticks into interval; rebaselines cleanly."""
        state = {'current_time': 250.0, 'step': 0}

        def mock_time():
            return state['current_time']

        def mock_reader(pid, proc_root=Path('/proc')):
            state['step'] += 1
            if state['step'] == 1:
                # Sample 0: root only, 50 ticks
                return {
                    'available': True,
                    'rss_bytes': 10 * 1024 * 1024,
                    'cpu_ticks': 50,
                    'process_count': 1,
                    'processes': {
                        (1000, 100): {'ticks': 50, 'rss_bytes': 10 * 1024 * 1024},
                    },
                }
            elif state['step'] == 2:
                # Sample 1: root has 70 ticks (+20), and child appears with 5000 lifetime ticks!
                return {
                    'available': True,
                    'rss_bytes': 20 * 1024 * 1024,
                    'cpu_ticks': 5070,
                    'process_count': 2,
                    'processes': {
                        (1000, 100): {'ticks': 70, 'rss_bytes': 10 * 1024 * 1024},
                        (1001, 105): {'ticks': 5000, 'rss_bytes': 10 * 1024 * 1024},
                    },
                }
            else:
                # Sample 2: root has 90 ticks (+20), child has 5030 ticks (+30). Total delta = 50.
                return {
                    'available': True,
                    'rss_bytes': 20 * 1024 * 1024,
                    'cpu_ticks': 5120,
                    'process_count': 2,
                    'processes': {
                        (1000, 100): {'ticks': 90, 'rss_bytes': 10 * 1024 * 1024},
                        (1001, 105): {'ticks': 5030, 'rss_bytes': 10 * 1024 * 1024},
                    },
                }

        sampler = GeneratorResourceSampler(
            root_pid=1000,
            interval=0.5,
            reader=mock_reader,
            time_fn=mock_time,
            host_cpus=1,
            clk_tck=100,
        )

        sampler.start()
        # Step 2: child appears
        state['current_time'] = 250.5
        with sampler._lock:
            proc, fd_count = sampler._do_reader_io()
            sampler._commit_snapshot_locked(proc, fd_count)

        # Step 3: normal delta after rebaseline
        state['current_time'] = 251.0
        with sampler._lock:
            proc, fd_count = sampler._do_reader_io()
            sampler._commit_snapshot_locked(proc, fd_count)

        sampler.stop()
        report = sampler.report()
        samples = report['samples']
        self.assertEqual(len(samples), 3)

        # Sample 1 must mark process_appeared and NOT attribute 5000 ticks to the interval
        self.assertIsNone(samples[1]['cpu_percent'])
        self.assertIsNone(samples[1]['delta_ticks'])
        self.assertEqual(samples[1]['reason'], 'process_appeared')

        # Sample 2 must accurately compute delta = 50 ticks (20 from root + 30 from child)
        self.assertEqual(samples[2]['delta_ticks'], 50)
        self.assertAlmostEqual(samples[2]['cpu_percent'], 100.0, places=4)
        self.assertIsNone(samples[2]['reason'])

    def test_baseline_and_counter_reset(self):
        """Verify counter reset (negative delta) marks CPU unavailable with exact reason."""
        state = {'current_time': 300.0, 'step': 0}

        def mock_reader(pid, proc_root=Path('/proc')):
            state['step'] += 1
            if state['step'] == 1:
                return {
                    'available': True,
                    'rss_bytes': 1024,
                    'cpu_ticks': 500,
                    'process_count': 1,
                    'processes': {(3000, 100): {'ticks': 500, 'rss_bytes': 1024}},
                }
            return {
                'available': True,
                'rss_bytes': 1024,
                'cpu_ticks': 200,
                'process_count': 1,
                'processes': {(3000, 100): {'ticks': 200, 'rss_bytes': 1024}},
            }

        sampler = GeneratorResourceSampler(
            root_pid=3000,
            interval=0.5,
            reader=mock_reader,
            time_fn=lambda: state['current_time'],
        )

        sampler.start()
        state['current_time'] = 300.5
        with sampler._lock:
            proc, fd_count = sampler._do_reader_io()
            sampler._commit_snapshot_locked(proc, fd_count)
        sampler.stop()

        report = sampler.report()
        s1 = report['samples'][1]
        self.assertIsNone(s1['cpu_percent'])
        self.assertIsNone(s1['delta_ticks'])
        self.assertEqual(s1['reason'], 'counter_reset_negative_delta')

    def test_unavailable_when_proc_disappears_and_recovery_no_origin(self):
        """Verify handling when /proc disappears or fails, and recovery requires new baseline."""
        state = {'current_time': 400.0, 'step': 0}

        def mock_reader(pid, proc_root=Path('/proc')):
            state['step'] += 1
            if state['step'] == 1:
                return {
                    'available': True,
                    'rss_bytes': 1000,
                    'cpu_ticks': 10,
                    'process_count': 1,
                    'processes': {(4000, 10): {'ticks': 10, 'rss_bytes': 1000}},
                }
            elif state['step'] == 2:
                return {'available': None, 'reason': 'generator_process_unavailable'}
            elif state['step'] == 3:
                return {
                    'available': True,
                    'rss_bytes': 1200,
                    'cpu_ticks': 30,
                    'process_count': 1,
                    'processes': {(4000, 10): {'ticks': 30, 'rss_bytes': 1200}},
                }
            else:
                return {
                    'available': True,
                    'rss_bytes': 1200,
                    'cpu_ticks': 50,
                    'process_count': 1,
                    'processes': {(4000, 10): {'ticks': 50, 'rss_bytes': 1200}},
                }

        sampler = GeneratorResourceSampler(
            root_pid=4000,
            interval=0.5,
            reader=mock_reader,
            time_fn=lambda: state['current_time'],
        )

        sampler.start()
        state['current_time'] = 400.5
        with sampler._lock:
            proc, fd_count = sampler._do_reader_io()
            sampler._commit_snapshot_locked(proc, fd_count)
        state['current_time'] = 401.0
        with sampler._lock:
            proc, fd_count = sampler._do_reader_io()
            sampler._commit_snapshot_locked(proc, fd_count)
        state['current_time'] = 401.5
        with sampler._lock:
            proc, fd_count = sampler._do_reader_io()
            sampler._commit_snapshot_locked(proc, fd_count)
        sampler.stop()

        report = sampler.report()
        samples = report['samples']
        self.assertEqual(len(samples), 4)

        self.assertTrue(samples[0]['available'])
        self.assertIsNone(samples[0]['cpu_percent'])
        self.assertEqual(samples[0]['reason'], 'first_sample_baseline_unavailable')

        self.assertFalse(samples[1]['available'])
        self.assertIsNone(samples[1]['rss_bytes'])
        self.assertIsNone(samples[1]['cpu_percent'])
        self.assertEqual(samples[1]['reason'], 'generator_process_unavailable')

        self.assertTrue(samples[2]['available'])
        self.assertIsNone(samples[2]['cpu_percent'])
        self.assertEqual(samples[2]['reason'], 'no_origin')

        self.assertTrue(samples[3]['available'])
        self.assertIsNotNone(samples[3]['cpu_percent'])
        self.assertEqual(samples[3]['delta_ticks'], 20)
        self.assertIsNone(samples[3]['reason'])

    def test_reader_exception_handled_without_crashing(self):
        """Verify reader throwing OSError or other exception is safely captured."""
        def broken_reader(pid, proc_root=Path('/proc')):
            raise OSError('proc_fs_read_error')

        sampler = GeneratorResourceSampler(root_pid=5000, reader=broken_reader)
        sampler.start()
        sampler.stop()

        report = sampler.report()
        s0 = report['samples'][0]
        self.assertFalse(s0['available'])
        self.assertEqual(s0['reason'], 'reader_exception_OSError')

    def test_stop_not_blocked_by_slow_or_hanging_reader(self):
        """Review Finding 1: verify stop() finishes within join_timeout budget when reader blocks."""
        block_event = threading.Event()
        started_call = threading.Event()
        call_count = 0

        def slow_reader(pid, proc_root=Path('/proc')):
            nonlocal call_count
            call_count += 1
            if call_count > 1:
                started_call.set()
                block_event.wait()
            return {'available': True, 'rss_bytes': 100, 'cpu_ticks': 10, 'process_count': 1, 'processes': {}}

        sampler = GeneratorResourceSampler(
            interval=0.01,
            join_timeout=0.1,
            reader=slow_reader,
        )

        sampler.start()
        # Wait for worker thread to enter slow_reader on second snapshot
        self.assertTrue(started_call.wait(timeout=1.0))

        # stop() must not block on reader or hold state lock
        t0 = time.monotonic()
        report = sampler.stop()
        elapsed = time.monotonic() - t0

        # Budget is 0.1s join_timeout; must complete well under 0.25s
        self.assertLess(elapsed, 0.25, f"stop() blocked for {elapsed:.3f}s despite join_timeout=0.1")
        self.assertIn('collector_join_timeout', report['summary']['errors'])

        # Calling report() must not block either
        t_rep0 = time.monotonic()
        rep2 = sampler.report()
        self.assertLess(time.monotonic() - t_rep0, 0.05)
        self.assertEqual(rep2['summary']['errors'], report['summary']['errors'])

        # Restarting after stop must raise RuntimeError, not silently spawn
        with self.assertRaises(RuntimeError):
            sampler.start()

        # Release blocking reader so background thread cleans up cleanly
        block_event.set()
        time.sleep(0.05)

    def test_timing_validation_rejects_nan_and_inf(self):
        """Review Finding 3: verify finite math.isfinite validation on timing parameters."""
        # Non-finite and invalid interval
        with self.assertRaises(ValueError):
            GeneratorResourceSampler(interval=float('nan'))
        with self.assertRaises(ValueError):
            GeneratorResourceSampler(interval=float('inf'))
        with self.assertRaises(ValueError):
            GeneratorResourceSampler(interval=-float('inf'))
        with self.assertRaises(ValueError):
            GeneratorResourceSampler(interval=0.0)
        with self.assertRaises(ValueError):
            GeneratorResourceSampler(interval=-0.5)

        # Non-finite and invalid deadline
        with self.assertRaises(ValueError):
            GeneratorResourceSampler(deadline_seconds=float('nan'))
        with self.assertRaises(ValueError):
            GeneratorResourceSampler(deadline_seconds=float('inf'))
        with self.assertRaises(ValueError):
            GeneratorResourceSampler(deadline_seconds=0.0)

        # Non-finite and invalid join_timeout
        with self.assertRaises(ValueError):
            GeneratorResourceSampler(join_timeout=float('nan'))
        with self.assertRaises(ValueError):
            GeneratorResourceSampler(join_timeout=float('inf'))
        with self.assertRaises(ValueError):
            GeneratorResourceSampler(join_timeout=-1.0)

        # Invalid max_snapshots
        with self.assertRaises(ValueError):
            GeneratorResourceSampler(max_snapshots=0)
        with self.assertRaises(ValueError):
            GeneratorResourceSampler(max_snapshots=-5)

    def test_worker_stops_at_deadline_even_when_interval_exceeds_deadline(self):
        """Review Finding 3: worker waits min(interval, remaining_deadline) and stops at deadline."""
        dummy_proc = {'available': True, 'rss_bytes': 100, 'cpu_ticks': 10, 'process_count': 1, 'processes': {}}

        t0 = time.monotonic()
        sampler = GeneratorResourceSampler(
            interval=10.0,          # Interval is very large (10s)
            deadline_seconds=0.1,   # Deadline is very short (0.1s)
            reader=lambda pid, proc_root=None: dummy_proc,
        )
        sampler.start()

        # Wait up to 1s for thread to finish on its own deadline
        sampler._thread.join(timeout=1.0)
        elapsed = time.monotonic() - t0

        self.assertLess(elapsed, 0.5, f"Worker did not stop at deadline; took {elapsed:.3f}s")
        report = sampler.stop()
        self.assertIn('collector_deadline_exceeded', report['summary']['errors'])

    def test_csv_propagates_failures_on_malformed_row_and_injected_error(self):
        """Review Finding 2: verify CSV writer propagates exceptions on malformed row or write failure."""
        with tempfile.TemporaryDirectory(dir='/var/tmp/opencode') as tmpdir:
            # 1. Malformed row (non-dict item)
            malformed_samples = [{'timestamp': '2026-10-30T10:00:00Z'}, 'not_a_dict']
            bad_csv = Path(tmpdir) / 'malformed.csv'
            with self.assertRaises(ValueError):
                write_generator_resources_csv(malformed_samples, bad_csv)

            # 2. Non-finite float value in column
            nan_samples = [{'timestamp': '2026-10-30T10:00:00Z', 'cpu_percent': float('nan')}]
            nan_csv = Path(tmpdir) / 'nan.csv'
            with self.assertRaises(ValueError):
                write_generator_resources_csv(nan_samples, nan_csv)

            # 3. Injected write failure during serialization
            good_samples = [{'timestamp': '2026-10-30T10:00:00Z', 'available': True, 'cpu_percent': 10.0}]
            inject_csv = Path(tmpdir) / 'injected.csv'

            with patch('csv.DictWriter.writerow', side_effect=IOError('disk_write_error')):
                with self.assertRaises(IOError):
                    write_generator_resources_csv(good_samples, inject_csv)

    def test_csv_no_fake_zero_and_exclusive_file(self):
        """Verify CSV writer creates exclusive private 0600 file with blanks instead of fake zeros."""
        samples = [
            {
                'timestamp': '2026-10-30T10:00:00Z',
                'elapsed_seconds': 0.0,
                'interval_seconds': None,
                'available': True,
                'cpu_percent': None,
                'delta_ticks': None,
                'rss_bytes': 1048576,
                'process_count': 1,
                'current_fd_count': 5,
                'reason': 'first_sample_baseline_unavailable',
            },
            {
                'timestamp': '2026-10-30T10:00:00.500Z',
                'elapsed_seconds': 0.5,
                'interval_seconds': 0.5,
                'available': True,
                'cpu_percent': 0.0,  # real zero
                'delta_ticks': 0,
                'rss_bytes': 1048576,
                'process_count': 1,
                'current_fd_count': 5,
                'reason': None,
            },
            {
                'timestamp': '2026-10-30T10:00:01Z',
                'elapsed_seconds': 1.0,
                'interval_seconds': None,
                'available': False,
                'cpu_percent': None,
                'delta_ticks': None,
                'rss_bytes': None,
                'process_count': None,
                'current_fd_count': None,
                'reason': 'generator_process_unavailable',
            },
        ]

        report = {
            'schema_version': 1,
            'role': 'generator_B',
            'samples': samples,
        }

        with tempfile.TemporaryDirectory(dir='/var/tmp/opencode') as tmpdir:
            csv_path = Path(tmpdir) / 'resources-generator.csv'

            result_path = write_generator_resources_csv(report, csv_path)
            self.assertEqual(result_path, csv_path)
            self.assertTrue(csv_path.is_file())

            mode = oct(csv_path.stat().st_mode & 0o777)
            self.assertEqual(mode, '0o600')

            with csv_path.open('r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                self.assertEqual(tuple(reader.fieldnames), GENERATOR_RESOURCES_CSV_COLUMNS)
                rows = list(reader)

            self.assertEqual(len(rows), 3)

            # Row 0: CPU unavailable -> MUST BE BLANK, NOT ZERO!
            self.assertEqual(rows[0]['cpu_percent'], '')
            self.assertEqual(rows[0]['delta_ticks'], '')
            self.assertEqual(rows[0]['interval_seconds'], '')
            self.assertEqual(rows[0]['rss_bytes'], '1048576')
            self.assertEqual(rows[0]['current_fd_count'], '5')
            self.assertEqual(rows[0]['reason'], 'first_sample_baseline_unavailable')

            # Row 1: Real zero -> '0.000000' and '0'
            self.assertEqual(rows[1]['cpu_percent'], '0.000000')
            self.assertEqual(rows[1]['delta_ticks'], '0')
            self.assertEqual(rows[1]['interval_seconds'], '0.500000')
            self.assertEqual(rows[1]['reason'], '')

            # Row 2: Everything unavailable -> blanks!
            self.assertEqual(rows[2]['available'], 'false')
            self.assertEqual(rows[2]['cpu_percent'], '')
            self.assertEqual(rows[2]['delta_ticks'], '')
            self.assertEqual(rows[2]['rss_bytes'], '')
            self.assertEqual(rows[2]['process_count'], '')
            self.assertEqual(rows[2]['current_fd_count'], '')
            self.assertEqual(rows[2]['reason'], 'generator_process_unavailable')

            # Verify exclusivity: attempting to overwrite existing file raises FileExistsError
            with self.assertRaises(FileExistsError):
                write_generator_resources_csv(report, csv_path)

            # Verify target symlink rejection
            symlink_path = Path(tmpdir) / 'symlink.csv'
            symlink_path.symlink_to(csv_path)
            with self.assertRaises(ValueError):
                write_generator_resources_csv(report, symlink_path)

            # Verify non-existent parent rejection
            bad_parent_path = Path(tmpdir) / 'nonexistent' / 'test.csv'
            with self.assertRaises(ValueError):
                write_generator_resources_csv(report, bad_parent_path)

    def test_scopes_and_strict_json_allow_nan_false(self):
        """Review Finding 5: verify explicit scopes qualification and strict allow_nan=False."""
        dummy_proc = {'available': True, 'rss_bytes': 1024, 'cpu_ticks': 10, 'process_count': 1, 'processes': {}}
        sampler = GeneratorResourceSampler(reader=lambda pid, proc_root=None: dummy_proc)
        sampler.start()
        sampler.stop()

        report = sampler.report()

        # Scopes qualification
        scopes = report['scopes']
        self.assertEqual(scopes['fd_scope'], 'root_process_only')
        self.assertEqual(scopes['rss_scope'], 'sum_sampled_RSS_may_double_count_shared_pages_not_peak')

        summary = report['summary']
        self.assertEqual(summary['fd_scope'], 'root_process_only')
        self.assertEqual(summary['max_sample_rss_qualification'], 'sum_sampled_RSS_may_double_count_shared_pages_not_peak')

        # Strict JSON serialization with allow_nan=False
        serialized = json.dumps(report, allow_nan=False)
        self.assertIsInstance(serialized, str)

    def test_max_samples_cap(self):
        """Verify snapshot cap is strictly enforced and capped at <= 128."""
        s_large = GeneratorResourceSampler(max_snapshots=500)
        self.assertEqual(s_large.max_snapshots, 128)

        state = {'time': 10.0}
        dummy_proc = {'available': True, 'rss_bytes': 100, 'cpu_ticks': 10, 'process_count': 1, 'processes': {}}

        sampler = GeneratorResourceSampler(
            max_snapshots=3,
            reader=lambda pid, proc_root=None: dummy_proc,
            time_fn=lambda: state['time'],
        )
        sampler.start()
        for _ in range(5):
            state['time'] += 0.5
            with sampler._lock:
                proc, fd_count = sampler._do_reader_io()
                sampler._commit_snapshot_locked(proc, fd_count)
        sampler.stop()

        report = sampler.report()
        self.assertEqual(len(report['samples']), 3)
        self.assertIn('snapshot_cap_reached', report['summary']['errors'])

    def test_finite_thread_join_and_idempotent_stop(self):
        """Verify stop is idempotent and thread join finishes cleanly."""
        dummy_proc = {'available': True, 'rss_bytes': 100, 'cpu_ticks': 10, 'process_count': 1, 'processes': {}}
        sampler = GeneratorResourceSampler(
            interval=0.05,
            reader=lambda pid, proc_root=None: dummy_proc,
        )
        sampler.start()
        time.sleep(0.08)

        r1 = sampler.stop()
        self.assertTrue(sampler._stopped)
        self.assertFalse(sampler._is_running)

        r2 = sampler.stop()
        r3 = sampler.stop()
        self.assertEqual(r1['summary']['total_samples'], r2['summary']['total_samples'])
        self.assertEqual(r1['summary']['total_samples'], r3['summary']['total_samples'])

    def test_import_subprocess_with_pythonpath_removed(self):
        """Verify standalone subprocess import works even when PYTHONPATH is unset."""
        env = dict(os.environ)
        env.pop('PYTHONPATH', None)
        cmd = [
            sys.executable,
            '-c',
            'import scripts.paper_p8_resources as r; '
            'assert r.GeneratorResourceSampler is not None; '
            'reader = r._get_default_process_tree_reader(); '
            'assert callable(reader)',
        ]
        proc = subprocess.run(cmd, env=env, cwd=str(REPO_ROOT), capture_output=True, text=True)
        self.assertEqual(proc.returncode, 0, f"Import without PYTHONPATH failed: {proc.stderr}")

    def test_safe_fd_count(self):
        """Verify _safe_fd_count counts FDs for current process and safely returns None on errors."""
        count = _safe_fd_count(os.getpid())
        self.assertIsInstance(count, int)
        self.assertGreater(count, 0)

        self.assertIsNone(_safe_fd_count(-1))
        self.assertIsNone(_safe_fd_count(999999999))

    def test_real_own_process_smoke(self):
        """Short real own-process smoke test (< 2 seconds) with no loops outside collector."""
        start_time = time.monotonic()
        with GeneratorResourceSampler(root_pid=os.getpid(), interval=0.1, max_snapshots=4) as sampler:
            time.sleep(0.35)

        elapsed = time.monotonic() - start_time
        self.assertLess(elapsed, 2.0, "Smoke test must finish in under 2 seconds")

        report = sampler.report()
        summary = report['summary']
        self.assertGreaterEqual(summary['total_samples'], 2)
        self.assertGreaterEqual(summary['available_samples'], 2)

        s0 = report['samples'][0]
        self.assertTrue(s0['available'])
        self.assertGreater(s0['rss_bytes'], 0)
        self.assertGreater(s0['process_count'], 0)
        if s0['current_fd_count'] is not None:
            self.assertGreater(s0['current_fd_count'], 0)

        self.assertIsNotNone(summary['max_sample_rss_bytes'])
        self.assertGreater(summary['max_sample_rss_bytes'], 0)


if __name__ == '__main__':
    unittest.main()
