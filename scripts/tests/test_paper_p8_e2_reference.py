"""Tests for E2 equal_samples immutable binary/image reference reuse and qualification (ES.6a)."""
import hashlib
import json
import os
from pathlib import Path
import stat
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

_HERE = Path(__file__).resolve().parent
for _p in (str(_HERE), str(_HERE.parent)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from scripts.tests import native_compose_smoke as smoke
from scripts.tests import paper_p8_e2_support as e2
from scripts.tests import paper_p8_support as p8
from scripts import paper_p8_server as server


from datetime import datetime, timezone, timedelta

def _make_sample_context():
    now_utc = datetime.now(timezone.utc).replace(microsecond=0)
    anchor = now_utc - timedelta(seconds=120)
    return e2.build_e2_equal_samples_context(
        total_samples=6000,
        sample_rate=100,
        seed=42,
        timestamp_anchor=anchor.strftime('%Y-%m-%dT%H:%M:%SZ'),
        max_context_age_seconds=86400,
        now_utc=now_utc,
    )


def _make_sample_reference(td_path, *, samples_per_message=1, binary_bytes=b'binary_content_v1',
                           context=None, status='PASS', cleanup_verified=True,
                           full_p8_accepted=False, experiment='E2', workload='equal_samples',
                           revision='r' * 40, dirty_source_sha256='d' * 64,
                           backend_id='sha256:' + 'b' * 64, gateway_id='sha256:' + 'c' * 64,
                           service_images=None):
    if context is None:
        context = _make_sample_context()
    if service_images is None:
        service_images = {
            'postgres': 'sha256:' + '1' * 64,
            'mosquitto': 'sha256:' + '2' * 64,
            'nginx': 'sha256:' + '3' * 64,
        }
    bin_dir = td_path / 'bin'
    bin_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    bin_file = bin_dir / 'paperload'
    bin_file.write_bytes(binary_bytes)
    bin_file.chmod(0o700)
    bin_sha256 = hashlib.sha256(binary_bytes).hexdigest()

    image_ids = {
        'backend': backend_id,
        'gateway': gateway_id,
        **service_images,
    }

    report = {
        'schema_version': 1,
        'status': status,
        'experiment': experiment,
        'workload': workload,
        'workload_type': workload,
        'samples_per_message': samples_per_message,
        'context': context,
        'context_sha256': context['context_sha256'],
        'source_fingerprint': {
            'revision': revision,
            'branch': 'feat/paper-requirement',
            'dirty': True,
            'dirty_source_sha256': dirty_source_sha256,
        },
        'schema': '24:telemetry_ledger_timestamp',
        'image_ids': image_ids,
        'images': {k: {'id': v} for k, v in image_ids.items()},
        'binary_sha256': bin_sha256,
        'paperload_binary_sha256': bin_sha256,
        'binary_path': 'bin/paperload',
        'effective_flags': {
            'STAGE5_ENABLED': False,
            'STAGE5_WAKEUP_ENABLED': False,
            'WEBSOCKET_ENABLED': False,
        },
        'quota': {'rate_per_second': 2, 'burst': 4},
        'logical_topology': {'gateways': 2, 'sensors': 2, 'sensors_per_gateway': 1, 'users': 0, 'viewers': 0},
        'cleanup_verified': cleanup_verified,
        'exact_cleanup_verified': cleanup_verified,
        'cleanup_provenance': {'exact_cleanup_verified': cleanup_verified, 'cleanup_counts': {'containers': 5}},
        'full_p8_accepted': full_p8_accepted,
        'validation': {'valid': (status == 'PASS'), 'failures': []},
        'workload_outcome': 'VALIDATED' if status == 'PASS' else 'VALIDATION_FAILED',
    }

    ref_file = td_path / f'p8-e2-equal-samples-{samples_per_message}-remote-result.json'
    ref_file.write_text(json.dumps(report, indent=2))
    ref_file.chmod(0o600)
    return ref_file, report, bin_file


class FakeRegistry:
    def __init__(self, *, backend_id, gateway_id, service_images, tag_collision=False):
        self.backend_id = backend_id
        self.gateway_id = gateway_id
        self.service_images = dict(service_images)
        self.tag_collision = tag_collision
        self.calls = []
        self.tagged = {}
        self.items = []

    def command(self, argv):
        self.calls.append(list(argv))
        if argv[:2] == ['image', 'inspect']:
            target = argv[2]
            if target == self.backend_id:
                return SimpleNamespace(returncode=0, stdout=json.dumps([{'Id': self.backend_id}]))
            if target == self.gateway_id:
                return SimpleNamespace(returncode=0, stdout=json.dumps([{'Id': self.gateway_id}]))
            if target in self.service_images:
                return SimpleNamespace(returncode=0, stdout=json.dumps([{'Id': self.service_images[target]}]))
            for name, s_id in self.service_images.items():
                if target == s_id:
                    return SimpleNamespace(returncode=0, stdout=json.dumps([{'Id': s_id}]))
            if target in self.tagged:
                found_id = ('sha256:' + 'x' * 64 if self.tag_collision else self.tagged[target])
                return SimpleNamespace(returncode=0, stdout=json.dumps([{'Id': found_id}]))
            if self.tag_collision:
                return SimpleNamespace(returncode=0, stdout=json.dumps([{'Id': 'sha256:' + 'x' * 64}]))
            return SimpleNamespace(returncode=1, stdout='')
        if argv[:2] == ['image', 'tag']:
            src, dst = argv[2], argv[3]
            self.tagged[dst] = src
            return SimpleNamespace(returncode=0, stdout='')
        return SimpleNamespace(returncode=0, stdout='')


class E2ReferenceReuseTests(unittest.TestCase):
    def setUp(self):
        self.ctx = _make_sample_context()
        self.source_fp = {
            'revision': 'r' * 40,
            'branch': 'feat/paper-requirement',
            'dirty': True,
            'dirty_source_sha256': 'd' * 64,
        }

    def test_e2_reference_reuse_call_graph(self):
        """When --reference is passed, images and binary are reused without building."""
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            ref_file, report, bin_file = _make_sample_reference(td_path, samples_per_message=1)

            ctx_file = td_path / 'context.json'
            ctx_file.write_text(json.dumps(self.ctx))
            ctx_file.chmod(0o600)

            fixture_dir = td_path / 'fixture'
            fixture = smoke.prepare(
                fixture_dir,
                paper_p8_e2=True,
                paper_p8_e2_workload='equal_samples',
                paper_p8_e2_context=str(ctx_file),
                paper_p8_e2_reference=str(ref_file),
            )

            service_images = {
                'postgres:latest': report['image_ids']['postgres'],
                'mosquitto:latest': report['image_ids']['mosquitto'],
                'nginx:latest': report['image_ids']['nginx'],
            }
            registry = FakeRegistry(
                backend_id=report['image_ids']['backend'],
                gateway_id=report['image_ids']['gateway'],
                service_images=service_images,
            )

            support_mock = SimpleNamespace(
                _source_fingerprint=lambda root: dict(self.source_fp)
            )

            mock_model = {
                'services': {
                    'backend': {'image': 'iot-backend'},
                    'postgres': {'image': 'postgres:latest'},
                    'mosquitto': {'image': 'mosquitto:latest'},
                    'nginx': {'image': 'nginx:latest'},
                }
            }

            with patch.object(smoke, 'dry_effective_config', return_value=mock_model), \
                 patch.object(smoke, 'paper_p8_support', return_value=support_mock), \
                 patch.object(smoke, 'build_native_image') as mock_build_backend, \
                 patch.object(smoke, 'build_paperload') as mock_build_paperload, \
                 patch.object(smoke, 'generate_fixture_tls'), \
                 patch.object(smoke, 'allocate_bootstrap_store', return_value='store'):

                # Startup up to build phase
                try:
                    smoke.startup_only(fixture, registry=registry, positive_http=False,
                                       role_http=False, full_http=False,
                                       hold_callback=lambda **kw: None)
                except Exception:
                    pass

                # Verify build was NEVER called
                mock_build_backend.assert_not_called()
                mock_build_paperload.assert_not_called()

                # Verify binary was copied and has identical bytes
                reused_bin = fixture.private / 'bin' / 'paperload'
                self.assertTrue(reused_bin.is_file())
                self.assertEqual(hashlib.sha256(reused_bin.read_bytes()).hexdigest(),
                                 report['binary_sha256'])

                # Verify binary was retained in artifacts
                retained_bin = fixture.artifacts / 'bin' / 'paperload'
                self.assertTrue(retained_bin.is_file())
                self.assertEqual(hashlib.sha256(retained_bin.read_bytes()).hexdigest(),
                                 report['binary_sha256'])

    def test_e2_reference_cleanup_not_verified(self):
        """Reference without verified exact cleanup must be rejected before allocation."""
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            ref_file, _, _ = _make_sample_reference(td_path, cleanup_verified=False)
            ctx_file = td_path / 'context.json'
            ctx_file.write_text(json.dumps(self.ctx))
            ctx_file.chmod(0o600)

            fixture = smoke.prepare(
                td_path / 'fix',
                paper_p8_e2=True,
                paper_p8_e2_workload='equal_samples',
                paper_p8_e2_context=str(ctx_file),
                paper_p8_e2_reference=str(ref_file),
            )
            support_mock = SimpleNamespace(_source_fingerprint=lambda root: dict(self.source_fp))
            with patch.object(smoke, 'paper_p8_support', return_value=support_mock):
                with self.assertRaisesRegex(ValueError, 'e2_reference.*cleanup'):
                    smoke._load_e2_reference(ref_file, fixture)

    def test_e2_reference_bad_context_or_hash(self):
        """Mismatched context hash or context fields must fail closed."""
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            bad_ctx = dict(self.ctx)
            bad_ctx['context_sha256'] = 'e' * 64
            ref_file, _, _ = _make_sample_reference(td_path, context=bad_ctx)

            ctx_file = td_path / 'context.json'
            ctx_file.write_text(json.dumps(self.ctx))
            ctx_file.chmod(0o600)

            fixture = smoke.prepare(
                td_path / 'fix',
                paper_p8_e2=True,
                paper_p8_e2_workload='equal_samples',
                paper_p8_e2_context=str(ctx_file),
                paper_p8_e2_reference=str(ref_file),
            )
            support_mock = SimpleNamespace(_source_fingerprint=lambda root: dict(self.source_fp))
            with patch.object(smoke, 'paper_p8_support', return_value=support_mock):
                with self.assertRaisesRegex(ValueError, 'e2_reference_context_mismatch'):
                    smoke._load_e2_reference(ref_file, fixture)

    def test_e2_reference_bad_experiment_or_workload(self):
        """Non-E2 or non-equal_samples reference must be rejected."""
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            ref_e3, _, _ = _make_sample_reference(td_path, experiment='E3')
            ctx_file = td_path / 'context.json'
            ctx_file.write_text(json.dumps(self.ctx))
            ctx_file.chmod(0o600)

            with self.assertRaises(ValueError):
                smoke.prepare(
                    td_path / 'fix1',
                    paper_p8_e2=True,
                    paper_p8_e2_workload='equal_samples',
                    paper_p8_e2_context=str(ctx_file),
                    paper_p8_e2_reference=str(ref_e3),
                )

            ref_steady, _, _ = _make_sample_reference(td_path / 'sub', workload='steady_burst')
            with self.assertRaises(ValueError):
                smoke.prepare(
                    td_path / 'fix2',
                    paper_p8_e2=True,
                    paper_p8_e2_workload='equal_samples',
                    paper_p8_e2_context=str(ctx_file),
                    paper_p8_e2_reference=str(ref_steady),
                )

    def test_e2_reference_missing_images_and_mismatch(self):
        """Malformed or mismatched service image IDs must fail before allocate."""
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            ref_file, report, _ = _make_sample_reference(td_path)
            ctx_file = td_path / 'context.json'
            ctx_file.write_text(json.dumps(self.ctx))
            ctx_file.chmod(0o600)

            # Test 1: Corrupted backend ID in reference
            corrupted_report = json.loads(ref_file.read_text())
            corrupted_report['image_ids']['backend'] = 'invalid_digest'
            bad_ref = td_path / 'bad_img_ref.json'
            bad_ref.write_text(json.dumps(corrupted_report))
            bad_ref.chmod(0o600)

            fixture = smoke.prepare(
                td_path / 'fix',
                paper_p8_e2=True,
                paper_p8_e2_workload='equal_samples',
                paper_p8_e2_context=str(ctx_file),
                paper_p8_e2_reference=str(bad_ref),
            )
            support_mock = SimpleNamespace(_source_fingerprint=lambda root: dict(self.source_fp))
            with patch.object(smoke, 'paper_p8_support', return_value=support_mock):
                with self.assertRaisesRegex(ValueError, 'e2_reference_images_invalid'):
                    smoke._load_e2_reference(bad_ref, fixture)

    def test_e2_reference_missing_or_mismatched_binary(self):
        """Missing or altered paperload binary must fail before workload and never silently rebuild."""
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            ref_file, report, bin_file = _make_sample_reference(td_path)
            ctx_file = td_path / 'context.json'
            ctx_file.write_text(json.dumps(self.ctx))
            ctx_file.chmod(0o600)

            fixture = smoke.prepare(
                td_path / 'fix',
                paper_p8_e2=True,
                paper_p8_e2_workload='equal_samples',
                paper_p8_e2_context=str(ctx_file),
                paper_p8_e2_reference=str(ref_file),
            )
            support_mock = SimpleNamespace(_source_fingerprint=lambda root: dict(self.source_fp))

            # Case A: Corrupted binary bytes
            bin_file.write_bytes(b'tampered_binary_bytes')
            with patch.object(smoke, 'paper_p8_support', return_value=support_mock):
                with self.assertRaisesRegex(ValueError, 'e2_reference_binary_mismatch'):
                    smoke._load_e2_reference(ref_file, fixture)

            # Case B: Missing binary file
            bin_file.unlink()
            with patch.object(smoke, 'paper_p8_support', return_value=support_mock):
                with self.assertRaisesRegex(ValueError, 'e2_reference_binary_missing'):
                    smoke._load_e2_reference(ref_file, fixture)

    def test_e2_reference_tag_collision(self):
        """Tag collision with conflicting image ID must raise target_tag_collision."""
        image_id = 'sha256:' + 'a' * 64
        registry = FakeRegistry(backend_id=image_id, gateway_id='sha256:' + 'c' * 64,
                                service_images={}, tag_collision=True)
        with self.assertRaisesRegex(ValueError, 'target_tag_collision'):
            smoke._reuse_e5_image(registry, {'backend': image_id}, 'backend', 'target-tag')

    def test_e2_reference_source_mismatch(self):
        """Source fingerprint revision or dirty digest difference must fail closed."""
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            ref_file, _, _ = _make_sample_reference(td_path, revision='x' * 40)
            ctx_file = td_path / 'context.json'
            ctx_file.write_text(json.dumps(self.ctx))
            ctx_file.chmod(0o600)

            fixture = smoke.prepare(
                td_path / 'fix',
                paper_p8_e2=True,
                paper_p8_e2_workload='equal_samples',
                paper_p8_e2_context=str(ctx_file),
                paper_p8_e2_reference=str(ref_file),
            )
            support_mock = SimpleNamespace(_source_fingerprint=lambda root: dict(self.source_fp))
            with patch.object(smoke, 'paper_p8_support', return_value=support_mock):
                with self.assertRaisesRegex(ValueError, 'e2_reference_source_mismatch'):
                    smoke._load_e2_reference(ref_file, fixture)

    def test_e2_reference_either_profile_ordering(self):
        """Either S=1 or S=100 can run first and serve as reference for the other."""
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            ctx_file = td_path / 'context.json'
            ctx_file.write_text(json.dumps(self.ctx))
            ctx_file.chmod(0o600)

            support_mock = SimpleNamespace(_source_fingerprint=lambda root: dict(self.source_fp))

            # Case A: S=1 was first, now running S=100 with S=1 reference
            ref_s1, _, _ = _make_sample_reference(td_path / 'run_s1', samples_per_message=1, context=self.ctx)
            fix_s100 = smoke.prepare(
                td_path / 'fix_s100',
                paper_p8_e2=True,
                paper_p8_e2_workload='equal_samples',
                paper_p8_e2_context=str(ctx_file),
                paper_p8_e2_reference=str(ref_s1),
            )
            with patch.object(smoke, 'paper_p8_support', return_value=support_mock):
                loaded = smoke._load_e2_reference(ref_s1, fix_s100)
                self.assertIsNotNone(loaded)
                self.assertEqual(loaded['report']['samples_per_message'], 1)

            # Case B: S=100 was first, now running S=1 with S=100 reference
            ref_s100, _, _ = _make_sample_reference(td_path / 'run_s100', samples_per_message=100, context=self.ctx)
            fix_s1 = smoke.prepare(
                td_path / 'fix_s1',
                paper_p8_e2=True,
                paper_p8_e2_workload='equal_samples',
                paper_p8_e2_context=str(ctx_file),
                paper_p8_e2_reference=str(ref_s100),
            )
            with patch.object(smoke, 'paper_p8_support', return_value=support_mock):
                loaded = smoke._load_e2_reference(ref_s100, fix_s1)
                self.assertIsNotNone(loaded)
                self.assertEqual(loaded['report']['samples_per_message'], 100)

    def test_server_prevalidation_accepts_valid_reference(self):
        """paper_p8_server prevalidation succeeds for valid reference and rejects invalid."""
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            ref_file, report, _ = _make_sample_reference(td_path)
            # Prevalidation with matching context should pass
            preval = e2.prevalidate_e2_reference(report, self.ctx)
            self.assertTrue(preval)

            # Prevalidation with bad context sha256 should fail
            bad_ctx = dict(self.ctx)
            bad_ctx['context_sha256'] = 'f' * 64
            with self.assertRaises(ValueError):
                e2.prevalidate_e2_reference(report, bad_ctx)

            # Prevalidation with cleanup_verified=False should fail
            bad_report = dict(report)
            bad_report['cleanup_verified'] = False
            bad_report['exact_cleanup_verified'] = False
            with self.assertRaises(ValueError):
                e2.prevalidate_e2_reference(bad_report, self.ctx)


if __name__ == '__main__':
    unittest.main()
