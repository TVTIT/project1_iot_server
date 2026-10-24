#!/usr/bin/env python3
"""Comprehensive Unit Tests for DynSec Bootstrap CLI Runner (Slice 3a).

Tests mock all Docker interactions and bootstrap runs to verify:
- Pinned image verification (architecture, OS, repo digests, digest match)
- Missing image rejection / pull refusal
- Configuration security (0600 file in 0700 dir, no secrets in argv)
- Preflight volume presence check and collision rejection
- Bootstrap execution output validation (store_created, volume name, NOT_READY)
- Receipt label verification (including task, run, and managed labels)
- Readback helper container execution (log-driver none, network none, readonly)
- Permission validation (1883:1883:700 dir, 1883:1883:600 file)
- Default deny ACL verification
- Client count and role verification (exactly bootstrap_manager and bootstrap_backend)
- Expected ACL literals verification (no tautological assertion)
- Collision execution refusal and store immutability check
- Authoritative volume cleanup (only if ownership verified, absence confirmed)
- Cleanup failure vetoing success
- Transient container baseline verification (mqtt-boot-* leakage detection)
- Safe report output format (allowlisted keys only, no passwords/hashes/secrets)
- [Defect 1] Authoritative absence: daemon failure, timeout, non-404 error vetoes success
- [Defect 1] Helper inspect exit state and ExitCode check
- [Defect 1] Helper ownership verification before removal
- [Defect 2] Partial bootstrap failure: owned leftover volume cleanup attempt
- [Defect 2] Foreign leftover volume refusal (no deletion)
- [Defect 2] Aggregation of cleanup errors without masking independent cleanups
- [Defect 2] Config unlink failure tracked safely
- [Defect 3] Principal validation: usernames, roles, groups, disabled flags, native hash schema
- [Defect 4] Leakage checks: secret scanning in stdout, stderr, and fixed diagnostic exception text
- [Defect 5] Atomic report publication and symlink / overwrite rejection
"""
import copy
import hashlib
import importlib.util
import json
import os
import signal
import subprocess
import stat
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

BOOTSTRAP_MODULE_PATH = Path(__file__).parents[1] / 'mqtt-bootstrap.py'


def load_bootstrap_module():
    spec = importlib.util.spec_from_file_location('mqtt_bootstrap_read_only', BOOTSTRAP_MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

try:
    from . import dynsec_bootstrap_cli as cli
except (ImportError, ValueError):
    import dynsec_bootstrap_cli as cli


def create_fake_store(mgr_user='mgr_user', be_user='be_user'):
    control = '$CONTROL/dynamic-security/v1'
    response = control + '/response'
    mgr_acls = [
        {'acltype': 'publishClientSend', 'topic': control, 'allow': True, 'priority': 0},
        {'acltype': 'publishClientReceive', 'topic': response, 'allow': True, 'priority': 0},
        {'acltype': 'subscribeLiteral', 'topic': response, 'allow': True, 'priority': 0},
        {'acltype': 'unsubscribeLiteral', 'topic': response, 'allow': True, 'priority': 0},
    ]
    be_acls = [
        {'acltype': 'publishClientReceive', 'topic': 'gateways/+/telemetry/#', 'allow': True, 'priority': 0},
        {'acltype': 'subscribeLiteral', 'topic': 'gateways/+/telemetry/#', 'allow': True, 'priority': 0},
        {'acltype': 'unsubscribeLiteral', 'topic': 'gateways/+/telemetry/#', 'allow': True, 'priority': 0},
        {'acltype': 'publishClientSend', 'topic': 'gateways/+/acks/#', 'allow': True, 'priority': 0},
        {'acltype': 'publishClientSend', 'topic': 'gateways/+/commands/#', 'allow': True, 'priority': 0},
        {'acltype': 'publishClientReceive', 'topic': 'gateways/+/responses/#', 'allow': True, 'priority': 0},
        {'acltype': 'subscribeLiteral', 'topic': 'gateways/+/responses/#', 'allow': True, 'priority': 0},
        {'acltype': 'unsubscribeLiteral', 'topic': 'gateways/+/responses/#', 'allow': True, 'priority': 0},
    ]
    # Native client hashes (base64 64 bytes password, 12 bytes salt, positive int iterations)
    pw_b64 = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=="
    salt_b64 = "AAAAAAAAAAAAAAAA"
    return {
        'clients': [
            {
                'username': mgr_user,
                'password': pw_b64,
                'salt': salt_b64,
                'iterations': 101,
                'roles': [{'rolename': 'bootstrap_manager'}],
            },
            {
                'username': be_user,
                'password': pw_b64,
                'salt': salt_b64,
                'iterations': 101,
                'roles': [{'rolename': 'bootstrap_backend'}],
            },
        ],
        'roles': [
            {'rolename': 'bootstrap_manager', 'acls': mgr_acls},
            {'rolename': 'bootstrap_backend', 'acls': be_acls},
        ],
        'defaultACLAccess': {
            'publishClientSend': False,
            'publishClientReceive': False,
            'subscribe': False,
            'unsubscribe': False,
        }
    }


class TestDynsecBootstrapCliRunner(unittest.TestCase):
    def test_readback_mount_receipt_rejects_anonymous_or_missing_masks(self):
        good = {'HostConfig': {'Tmpfs': cli.READBACK_TMPFS}, 'Mounts': [
            {'Type': 'volume', 'Name': 'fixture', 'Destination': '/mosquitto/dynsec', 'RW': False}]}
        cli.verify_readback_masks(good, 'fixture')
        for change in ('missing', 'anonymous', 'wrong_uid'):
            obj = copy.deepcopy(good)
            if change == 'missing':
                obj['HostConfig']['Tmpfs'] = {}
            elif change == 'anonymous':
                obj['Mounts'].append({'Type': 'volume', 'Name': 'unknown', 'Destination': '/mosquitto/data', 'RW': True})
            else:
                obj['HostConfig']['Tmpfs']['/mosquitto/log'] = 'rw,size=1m'
            with self.assertRaisesRegex(cli.RunnerError, 'mount receipt mismatch'):
                cli.verify_readback_masks(obj, 'fixture')

    def setUp(self):
        self.pinned_image = cli.PINNED_IMAGE
        self.pinned_config_id = cli.PINNED_CONFIG_ID
        self.fake_image_inspect = [{
            'Architecture': 'amd64',
            'Os': 'linux',
            'RepoDigests': [self.pinned_image],
            'Id': cli.PINNED_CONFIG_ID
        }]
        self.fake_store = create_fake_store()
        self.fake_store_raw = json.dumps(self.fake_store).encode('utf-8')

    def test_verify_pinned_image_literal_verified_tuple(self):
        # Literal independently verified valid inspect tuple:
        # RepoDigests with root OCI index reference/pin, Architecture amd64, Os linux, Id with verified config ID
        literal_inspect = [{
            'Architecture': 'amd64',
            'Os': 'linux',
            'RepoDigests': ['eclipse-mosquitto@sha256:d12c8f80dfc65b768bb9acecc7ef182b976f71fb681640b66358e5e0cf94e9e9'],
            'Id': 'sha256:d25945831d6b8935b016caacc6f612ad4876ba124601b240e64953886cf264b4'
        }]
        runner = mock.Mock(return_value=mock.Mock(
            returncode=0,
            stdout=json.dumps(literal_inspect).encode('utf-8')
        ))
        img_id = cli.verify_pinned_image(self.pinned_image, runner=runner)
        self.assertEqual(img_id, 'sha256:d25945831d6b8935b016caacc6f612ad4876ba124601b240e64953886cf264b4')

    def test_cli_and_bootstrap_identity_constants_match(self):
        production = load_bootstrap_module()
        self.assertEqual(cli.PINNED_IMAGE, production.PINNED_IMAGE)
        self.assertEqual(cli.PINNED_DIGEST, production.PINNED_DIGEST)
        self.assertEqual(cli.PINNED_CONFIG_ID, production.PINNED_CONFIG_ID)
        self.assertEqual(cli.PINNED_PLATFORM_MANIFEST_DIGEST, production.PINNED_PLATFORM_MANIFEST_DIGEST)
        self.assertEqual(cli.PINNED_CONFIG_ID,
                         'sha256:d25945831d6b8935b016caacc6f612ad4876ba124601b240e64953886cf264b4')

    def test_verify_pinned_image_index_digest_without_descriptor_rejected(self):
        # A root Id is acceptable only with the required root descriptor and secondary inspect.
        index_as_id_inspect = [{
            'Architecture': 'amd64',
            'Os': 'linux',
            'RepoDigests': [self.pinned_image],
            'Id': 'sha256:d12c8f80dfc65b768bb9acecc7ef182b976f71fb681640b66358e5e0cf94e9e9'
        }]
        runner = mock.Mock(return_value=mock.Mock(
            returncode=0,
            stdout=json.dumps(index_as_id_inspect).encode('utf-8')
        ))
        with self.assertRaisesRegex(cli.RunnerError, 'missing or malformed root index descriptor metadata'):
            cli.verify_pinned_image(self.pinned_image, runner=runner)

    def test_containerd_image_inspect_requires_verified_platform_evidence(self):
        root = [{**self.fake_image_inspect[0], 'Id': 'sha256:' + cli.PINNED_DIGEST,
                 'Descriptor': {'digest': 'sha256:' + cli.PINNED_DIGEST,
                                'mediaType': 'application/vnd.oci.image.index.v1+json'}}]
        platform = [{'Architecture': 'amd64', 'Os': 'linux',
                     'Id': cli.PINNED_PLATFORM_MANIFEST_DIGEST,
                     'Descriptor': {'digest': cli.PINNED_PLATFORM_MANIFEST_DIGEST,
                                    'mediaType': 'application/vnd.oci.image.manifest.v1+json'}}]
        runner = mock.Mock(side_effect=[
            mock.Mock(returncode=0, stdout=json.dumps(root).encode()),
            mock.Mock(returncode=0, stdout=json.dumps(platform).encode())])
        self.assertEqual(cli.verify_pinned_image(self.pinned_image, runner=runner),
                         'sha256:' + cli.PINNED_DIGEST)
        self.assertEqual(runner.call_args_list[1].args[0],
                         ['docker', 'image', 'inspect', '--platform', 'linux/amd64', self.pinned_image])
        self.assertEqual(runner.call_args_list[1].kwargs['timeout'], 10)

        for bad_root, bad_platform in (
                ({**root[0], 'Descriptor': {'digest': cli.PINNED_PLATFORM_MANIFEST_DIGEST,
                                           'mediaType': 'application/vnd.oci.image.manifest.v1+json'}}, platform[0]),
                (root[0], {**platform[0], 'Descriptor': {'digest': 'sha256:' + cli.PINNED_DIGEST,
                                                        'mediaType': 'application/vnd.oci.image.index.v1+json'}}),
                (root[0], {**platform[0], 'Architecture': 'arm64'}),
                (root[0], {**platform[0], 'Id': cli.PINNED_CONFIG_ID}),
        ):
            with self.subTest(root=bad_root, platform=bad_platform):
                runner = mock.Mock(side_effect=[
                    mock.Mock(returncode=0, stdout=json.dumps([bad_root]).encode()),
                    mock.Mock(returncode=0, stdout=json.dumps([bad_platform]).encode())])
                with self.assertRaises(cli.RunnerError):
                    cli.verify_pinned_image(self.pinned_image, runner=runner)

    def test_containerd_secondary_inspect_failure_timeout_and_malformed_output_rejected(self):
        root = [{**self.fake_image_inspect[0], 'Id': 'sha256:' + cli.PINNED_DIGEST,
                 'Descriptor': {'digest': 'sha256:' + cli.PINNED_DIGEST,
                                'mediaType': 'application/vnd.oci.image.index.v1+json'}}]
        for secondary in (mock.Mock(returncode=1, stdout=b'', stderr=b'private marker'),
                          subprocess.TimeoutExpired('inspect', 10),
                          mock.Mock(returncode=0, stdout=b'{"secret-marker"')):
            runner = mock.Mock(side_effect=[mock.Mock(returncode=0, stdout=json.dumps(root).encode()), secondary])
            with self.assertRaises(cli.RunnerError) as caught:
                cli.verify_pinned_image(self.pinned_image, runner=runner)
            self.assertNotIn('marker', str(caught.exception))

    def test_classic_descriptor_pairs_are_bound_and_unknown_or_swapped_rejected(self):
        accepted = [
            {'digest': cli.PINNED_CONFIG_ID, 'mediaType': 'application/vnd.oci.image.config.v1+json'},
            {'digest': 'sha256:' + cli.PINNED_DIGEST, 'mediaType': 'application/vnd.oci.image.index.v1+json'},
            {'digest': cli.PINNED_PLATFORM_MANIFEST_DIGEST, 'mediaType': 'application/vnd.oci.image.manifest.v1+json'},
        ]
        for descriptor in accepted:
            data = [{**self.fake_image_inspect[0], 'Descriptor': descriptor}]
            runner = mock.Mock(return_value=mock.Mock(returncode=0, stdout=json.dumps(data).encode()))
            self.assertEqual(cli.verify_pinned_image(self.pinned_image, runner=runner), cli.PINNED_CONFIG_ID)
        for descriptor in ({'digest': cli.PINNED_CONFIG_ID, 'mediaType': 'application/vnd.oci.image.index.v1+json'},
                           {'digest': 'sha256:' + '0' * 64, 'mediaType': 'application/vnd.oci.image.config.v1+json'},
                           'malformed'):
            data = [{**self.fake_image_inspect[0], 'Descriptor': descriptor}]
            runner = mock.Mock(return_value=mock.Mock(returncode=0, stdout=json.dumps(data).encode()))
            with self.assertRaises(cli.RunnerError):
                cli.verify_pinned_image(self.pinned_image, runner=runner)

    def test_verify_pinned_image_success(self):
        runner = mock.Mock()
        runner.return_value = mock.Mock(
            returncode=0,
            stdout=json.dumps(self.fake_image_inspect).encode('utf-8')
        )
        img_id = cli.verify_pinned_image(self.pinned_image, runner=runner)
        self.assertEqual(img_id, cli.PINNED_CONFIG_ID)

    def test_verify_pinned_image_rejects_wrong_registry_pin(self):
        bad = copy.deepcopy(self.fake_image_inspect)
        bad[0]['RepoDigests'] = ['eclipse-mosquitto@sha256:' + '0' * 64]
        self.assert_image_rejected(bad, 'RepoDigests exact pin mismatch')

    def test_verify_pinned_image_rejects_missing_or_malformed_metadata(self):
        marker = 'do-not-leak-metadata'
        for value in ([], [{}], [{'Architecture': 'amd64', 'Os': 'linux', 'RepoDigests': [self.pinned_image]}],
                      [{'Architecture': 'amd64', 'Os': 'linux', 'RepoDigests': 'invalid', 'Id': cli.PINNED_CONFIG_ID}],
                      [None], {'Architecture': 'amd64'},
                      [{**self.fake_image_inspect[0], 'Id': None}],
                      [{**self.fake_image_inspect[0], 'Id': 123}],
                      [{**self.fake_image_inspect[0], 'Id': ''}],
                      [{**self.fake_image_inspect[0], 'Id': 'malformed'}],
                      [{**self.fake_image_inspect[0], 'Id': marker}],
                      [{'Architecture': 'amd64', 'Os': 'linux', 'Id': cli.PINNED_CONFIG_ID}],
                      [{**self.fake_image_inspect[0], 'RepoDigests': ['wrong']}],
                      [{**self.fake_image_inspect[0], 'Architecture': 'arm64'}]):
            with self.subTest(value=value):
                runner = mock.Mock(return_value=mock.Mock(returncode=0, stdout=json.dumps(value).encode()))
                with self.assertRaises(cli.RunnerError) as caught:
                    cli.verify_pinned_image(self.pinned_image, runner=runner)
                self.assertNotIn(marker, str(caught.exception))

    def test_verify_pinned_image_malformed_json_has_fixed_error(self):
        runner = mock.Mock(return_value=mock.Mock(returncode=0, stdout=b'{"Id":"secret-marker"'))
        with self.assertRaisesRegex(cli.RunnerError, '^failed to parse image inspect output$') as caught:
            cli.verify_pinned_image(self.pinned_image, runner=runner)
        self.assertNotIn('secret-marker', str(caught.exception))

    def test_verify_pinned_image_rejects_duplicate_keys_and_multiple_objects(self):
        for raw in (b'[{"Id":"sha256:one","Id":"sha256:two"}]',
                    json.dumps(self.fake_image_inspect * 2).encode()):
            runner = mock.Mock(return_value=mock.Mock(returncode=0, stdout=raw))
            with self.assertRaises(cli.RunnerError):
                cli.verify_pinned_image(self.pinned_image, runner=runner)

    def test_verify_pinned_image_rejects_wrong_os(self):
        bad = copy.deepcopy(self.fake_image_inspect)
        bad[0]['Os'] = 'windows'
        self.assert_image_rejected(bad, 'linux/amd64 required')

    def assert_image_rejected(self, data, message):
        runner = mock.Mock(return_value=mock.Mock(returncode=0, stdout=json.dumps(data).encode()))
        with self.assertRaisesRegex(cli.RunnerError, message):
            cli.verify_pinned_image(self.pinned_image, runner=runner)

    def test_verify_pinned_image_missing_fails(self):
        runner = mock.Mock(return_value=mock.Mock(returncode=1, stderr=b'Error: No such image'))
        with self.assertRaisesRegex(cli.RunnerError, 'pinned image not found locally'):
            cli.verify_pinned_image(self.pinned_image, runner=runner)

    def test_verify_pinned_image_arch_mismatch(self):
        bad_inspect = copy.deepcopy(self.fake_image_inspect)
        bad_inspect[0]['Architecture'] = 'arm64'
        runner = mock.Mock(return_value=mock.Mock(
            returncode=0,
            stdout=json.dumps(bad_inspect).encode('utf-8')
        ))
        with self.assertRaisesRegex(cli.RunnerError, 'linux/amd64 required'):
            cli.verify_pinned_image(self.pinned_image, runner=runner)

    def test_verify_pinned_image_digest_mismatch(self):
        bad_inspect = copy.deepcopy(self.fake_image_inspect)
        bad_inspect[0]['Id'] = 'sha256:0000000000000000000000000000000000000000000000000000000000000000'
        runner = mock.Mock(return_value=mock.Mock(
            returncode=0,
            stdout=json.dumps(bad_inspect).encode('utf-8')
        ))
        with self.assertRaisesRegex(cli.RunnerError, 'image Id immutable digest mismatch'):
            cli.verify_pinned_image(self.pinned_image, runner=runner)

    def test_check_volume_absent_and_collision(self):
        # Case 1: Absent (inspection returns returncode 1 with 'no such volume')
        runner_absent = mock.Mock(return_value=mock.Mock(returncode=1, stderr=b'Error: no such volume'))
        cli.check_volume_absent('test_vol', runner=runner_absent)

        # Case 2: Exists (collision)
        runner_exists = mock.Mock(return_value=mock.Mock(returncode=0, stdout=b'[{}]'))
        with self.assertRaisesRegex(cli.RunnerError, 'volume collision'):
            cli.check_volume_absent('test_vol', runner=runner_exists)

        # Case 3: Daemon error
        runner_err = mock.Mock(return_value=mock.Mock(returncode=1, stderr=b'daemon down'))
        with self.assertRaisesRegex(cli.RunnerError, 'volume presence check failed'):
            cli.check_volume_absent('test_vol', runner=runner_err)

    def test_verify_volume_receipt_labels(self):
        expected_labels = {
            'com.docker.compose.volume': 'mosquitto_dynsec',
            'bootstrap.managed': 'true',
            'test.task': 'unit-test'
        }
        inspect_data = [{
            'Name': 'vol123',
            'Labels': expected_labels
        }]
        runner_ok = mock.Mock(return_value=mock.Mock(
            returncode=0,
            stdout=json.dumps(inspect_data).encode('utf-8')
        ))
        res = cli.verify_volume_receipt('vol123', expected_labels, runner=runner_ok)
        self.assertEqual(res['Name'], 'vol123')

        # Label mismatch
        bad_inspect = [{
            'Name': 'vol123',
            'Labels': {'test.task': 'wrong'}
        }]
        runner_bad = mock.Mock(return_value=mock.Mock(
            returncode=0,
            stdout=json.dumps(bad_inspect).encode('utf-8')
        ))
        with self.assertRaisesRegex(cli.RunnerError, 'volume receipt label mismatch'):
            cli.verify_volume_receipt('vol123', expected_labels, runner=runner_bad)

    def test_readback_and_verify_store_success(self):
        cid = 'a' * 64
        readback_stdout = b'1883:1883:700\n1883:1883:600\n' + self.fake_store_raw
        helper_inspect = [{
            'Id': cid,
            'Name': '/dynsec-readback-helper-123',
            'Image': self.pinned_config_id,
            'HostConfig': {'Tmpfs': cli.READBACK_TMPFS},
            'Mounts': [{'Type': 'volume', 'Name': 'test_vol', 'Destination': '/mosquitto/dynsec', 'RW': False}],
            'Config': {
                'Labels': {'test.run': 'run123', 'test.task': 'dynsec-readback-helper'}
            },
            'State': {'ExitCode': 0, 'Status': 'exited'}
        }]

        def fake_runner(cmd, **kwargs):
            if cmd[:2] == ['docker', 'create']:
                # Assert security flags in helper creation
                self.assertIn('--network', cmd)
                self.assertIn('none', cmd)
                self.assertIn('--log-driver', cmd)
                self.assertIn('--user', cmd)
                self.assertIn('1883:1883', cmd)
                self.assertIn('--pull=never', cmd)
                masks = [cmd[i + 1] for i, arg in enumerate(cmd) if arg == '--tmpfs']
                self.assertEqual(masks, [p + ':rw,noexec,nosuid,nodev,size=1m,mode=0700,uid=1883,gid=1883'
                                        for p in ('/mosquitto/data', '/mosquitto/log')])
                # Verify readonly mount
                mount_arg = [arg for arg in cmd if arg.startswith('type=volume')]
                self.assertTrue(any('readonly' in m for m in mount_arg))
                return mock.Mock(returncode=0, stdout=cid.encode('utf-8'))
            elif cmd[:2] == ['docker', 'start']:
                return mock.Mock(returncode=0, stdout=readback_stdout)
            elif cmd[:3] == ['docker', 'container', 'inspect']:
                if not hasattr(fake_runner, 'inspected'):
                    fake_runner.inspected = True
                    return mock.Mock(returncode=0, stdout=json.dumps(helper_inspect).encode('utf-8'))
                return mock.Mock(returncode=1, stderr=b'no such container')
            elif cmd[:2] == ['docker', 'rm']:
                return mock.Mock(returncode=0)
            return mock.Mock(returncode=0)

        digest = cli.readback_and_verify_store(
            'test_vol', self.pinned_config_id, 'mgr_user', 'be_user',
            run_nonce='run123', task_label='dynsec-readback-helper', runner=fake_runner
        )
        self.assertEqual(digest, hashlib.sha256(self.fake_store_raw).hexdigest())

    def test_root_index_helper_receipt_requires_platform_manifest_descriptor(self):
        cid = 'b' * 64
        root_image = 'sha256:' + cli.PINNED_DIGEST
        removed = []
        inspected = {'Id': cid, 'Name': '/dynsec-readback-helper-test', 'Image': root_image,
                     'HostConfig': {'Tmpfs': cli.READBACK_TMPFS},
                     'Mounts': [{'Type': 'volume', 'Name': 'test_vol',
                                 'Destination': '/mosquitto/dynsec', 'RW': False}],
                     'Config': {'Labels': {'test.run': 'run123', 'test.task': 'dynsec-readback-helper'}},
                     'State': {'ExitCode': 0, 'Status': 'exited'}}

        def fake_runner(cmd, **kwargs):
            if cmd[:2] == ['docker', 'create']:
                return mock.Mock(returncode=0, stdout=cid.encode())
            if cmd[:2] == ['docker', 'start']:
                return mock.Mock(returncode=0, stdout=b'1883:1883:700\n1883:1883:600\n' + self.fake_store_raw)
            if cmd[:3] == ['docker', 'container', 'inspect']:
                if removed:
                    return mock.Mock(returncode=1, stderr=b'no such container')
                return mock.Mock(returncode=0, stdout=json.dumps([inspected]).encode())
            if cmd[:3] == ['docker', 'rm', '-f']:
                removed.append(cmd[-1])
                return mock.Mock(returncode=0)
            raise AssertionError(cmd)

        with self.assertRaisesRegex(cli.RunnerError, 'manifest descriptor mismatch'):
            cli.readback_and_verify_store('test_vol', root_image, 'mgr_user', 'be_user',
                                          run_nonce='run123', task_label='dynsec-readback-helper',
                                          runner=fake_runner)
        self.assertEqual(removed, [])
        inspected['ImageManifestDescriptor'] = {
            'digest': cli.PINNED_PLATFORM_MANIFEST_DIGEST,
            'mediaType': 'application/vnd.oci.image.manifest.v1+json'}
        self.assertEqual(cli.readback_and_verify_store('test_vol', root_image, 'mgr_user', 'be_user',
                                                       run_nonce='run123', task_label='dynsec-readback-helper',
                                                       runner=fake_runner), hashlib.sha256(self.fake_store_raw).hexdigest())
        self.assertTrue(removed)

    # --- DEFECT 1: Authoritative Absence & Helper State ---
    def test_helper_inspect_exit_code_failure(self):
        cid = 'a' * 64
        readback_stdout = b'1883:1883:700\n1883:1883:600\n' + self.fake_store_raw
        bad_helper_inspect = [{
            'HostConfig': {'Tmpfs': cli.READBACK_TMPFS},
            'Mounts': [{'Type': 'volume', 'Name': 'test_vol', 'Destination': '/mosquitto/dynsec', 'RW': False}],
            'Id': cid,
            'Name': '/dynsec-readback-helper-123',
            'Image': self.pinned_config_id,
            'Config': {'Labels': {'test.run': 'run123', 'test.task': 'dynsec-readback-helper'}},
            'State': {'ExitCode': 137, 'Status': 'exited'}
        }]

        def fake_runner(cmd, **kwargs):
            if cmd[:2] == ['docker', 'create']:
                return mock.Mock(returncode=0, stdout=cid.encode('utf-8'))
            elif cmd[:2] == ['docker', 'start']:
                return mock.Mock(returncode=0, stdout=readback_stdout)
            elif cmd[:3] == ['docker', 'container', 'inspect']:
                if not hasattr(fake_runner, 'inspected'):
                    fake_runner.inspected = True
                    return mock.Mock(returncode=0, stdout=json.dumps(bad_helper_inspect).encode('utf-8'))
                return mock.Mock(returncode=1, stderr=b'no such container')
            elif cmd[:2] == ['docker', 'rm']:
                return mock.Mock(returncode=0)
            return mock.Mock(returncode=0)

        with self.assertRaisesRegex(cli.RunnerError, 'helper container execution failed with non-zero exit code'):
            cli.readback_and_verify_store(
                'test_vol', self.pinned_config_id, 'mgr_user', 'be_user',
                run_nonce='run123', task_label='dynsec-readback-helper', runner=fake_runner
            )

    def test_helper_cleanup_daemon_error_not_treated_as_absence(self):
        cid = 'a' * 64
        readback_stdout = b'1883:1883:700\n1883:1883:600\n' + self.fake_store_raw
        helper_inspect = [{
            'Id': cid,
            'Name': '/dynsec-readback-helper-123',
            'Image': self.pinned_config_id,
            'Config': {'Labels': {'test.run': 'run123', 'test.task': 'dynsec-readback-helper'}},
            'State': {'ExitCode': 0, 'Status': 'exited'}
        }]

        def fake_runner(cmd, **kwargs):
            if cmd[:2] == ['docker', 'create']:
                return mock.Mock(returncode=0, stdout=cid.encode('utf-8'))
            elif cmd[:2] == ['docker', 'start']:
                return mock.Mock(returncode=0, stdout=readback_stdout)
            elif cmd[:3] == ['docker', 'container', 'inspect']:
                if not hasattr(fake_runner, 'inspected'):
                    fake_runner.inspected = True
                    return mock.Mock(returncode=0, stdout=json.dumps(helper_inspect).encode('utf-8'))
                # Non-404 error during absence check!
                return mock.Mock(returncode=1, stderr=b'Error response from daemon: connection reset')
            elif cmd[:2] == ['docker', 'rm']:
                return mock.Mock(returncode=0)
            return mock.Mock(returncode=0)

        with self.assertRaisesRegex(cli.RunnerError, 'helper container absence verification failed'):
            cli.readback_and_verify_store(
                'test_vol', self.pinned_config_id, 'mgr_user', 'be_user',
                run_nonce='run123', task_label='dynsec-readback-helper', runner=fake_runner
            )

    def test_foreign_helper_receipt_refused(self):
        cid = 'a' * 64
        removed = []
        readback_stdout = b'1883:1883:700\n1883:1883:600\n' + self.fake_store_raw
        foreign_helper_inspect = [{
            'Id': cid,
            'Name': '/dynsec-readback-helper-123',
            'Image': self.pinned_config_id,
            'Config': {'Labels': {'test.run': 'other_run', 'test.task': 'other_task'}},
            'State': {'ExitCode': 0, 'Status': 'exited'}
        }]

        def fake_runner(cmd, **kwargs):
            if cmd[:2] == ['docker', 'create']:
                return mock.Mock(returncode=0, stdout=cid.encode('utf-8'))
            elif cmd[:2] == ['docker', 'start']:
                return mock.Mock(returncode=0, stdout=readback_stdout)
            elif cmd[:3] == ['docker', 'container', 'inspect']:
                return mock.Mock(returncode=0, stdout=json.dumps(foreign_helper_inspect).encode('utf-8'))
            elif cmd[:2] == ['docker', 'rm']:
                removed.append(cmd)
                return mock.Mock(returncode=0)
            return mock.Mock(returncode=0)

        with self.assertRaisesRegex(cli.RunnerError, 'helper container receipt ownership mismatch'):
            cli.readback_and_verify_store(
                'test_vol', self.pinned_config_id, 'mgr_user', 'be_user',
                run_nonce='run123', task_label='dynsec-readback-helper', runner=fake_runner
            )
        self.assertEqual(removed, [])

    # --- DEFECT 3: Principal & Hash Schema Validation ---
    def test_readback_swapped_usernames_rejected(self):
        cid = 'a' * 64
        swapped_store = create_fake_store(mgr_user='be_user', be_user='mgr_user')
        readback_stdout = b'1883:1883:700\n1883:1883:600\n' + json.dumps(swapped_store).encode('utf-8')
        helper_inspect = [{
            'HostConfig': {'Tmpfs': cli.READBACK_TMPFS},
            'Mounts': [{'Type': 'volume', 'Name': 'test_vol', 'Destination': '/mosquitto/dynsec', 'RW': False}],
            'Id': cid,
            'Name': '/dynsec-readback-helper-123',
            'Image': self.pinned_config_id,
            'Config': {'Labels': {'test.run': 'run123', 'test.task': 'dynsec-readback-helper'}},
            'State': {'ExitCode': 0, 'Status': 'exited'}
        }]

        def fake_runner(cmd, **kwargs):
            if cmd[:2] == ['docker', 'create']:
                return mock.Mock(returncode=0, stdout=cid.encode('utf-8'))
            elif cmd[:2] == ['docker', 'start']:
                return mock.Mock(returncode=0, stdout=readback_stdout)
            elif cmd[:3] == ['docker', 'container', 'inspect']:
                if not hasattr(fake_runner, 'inspected'):
                    fake_runner.inspected = True
                    return mock.Mock(returncode=0, stdout=json.dumps(helper_inspect).encode('utf-8'))
                return mock.Mock(returncode=1, stderr=b'no such container')
            elif cmd[:2] == ['docker', 'rm']:
                return mock.Mock(returncode=0)
            return mock.Mock(returncode=0)

        with self.assertRaisesRegex(cli.RunnerError, 'manager client role mismatch'):
            cli.readback_and_verify_store(
                'test_vol', self.pinned_config_id, 'mgr_user', 'be_user',
                run_nonce='run123', task_label='dynsec-readback-helper', runner=fake_runner
            )

    def test_readback_disabled_client_or_groups_rejected(self):
        cid = 'a' * 64
        bad_store = create_fake_store()
        bad_store['clients'][0]['disabled'] = True
        readback_stdout = b'1883:1883:700\n1883:1883:600\n' + json.dumps(bad_store).encode('utf-8')
        helper_inspect = [{
            'HostConfig': {'Tmpfs': cli.READBACK_TMPFS},
            'Mounts': [{'Type': 'volume', 'Name': 'test_vol', 'Destination': '/mosquitto/dynsec', 'RW': False}],
            'Id': cid,
            'Name': '/dynsec-readback-helper-123',
            'Image': self.pinned_config_id,
            'Config': {'Labels': {'test.run': 'run123', 'test.task': 'dynsec-readback-helper'}},
            'State': {'ExitCode': 0, 'Status': 'exited'}
        }]

        def fake_runner(cmd, **kwargs):
            if cmd[:2] == ['docker', 'create']:
                return mock.Mock(returncode=0, stdout=cid.encode('utf-8'))
            elif cmd[:2] == ['docker', 'start']:
                return mock.Mock(returncode=0, stdout=readback_stdout)
            elif cmd[:3] == ['docker', 'container', 'inspect']:
                if not hasattr(fake_runner, 'inspected'):
                    fake_runner.inspected = True
                    return mock.Mock(returncode=0, stdout=json.dumps(helper_inspect).encode('utf-8'))
                return mock.Mock(returncode=1, stderr=b'no such container')
            elif cmd[:2] == ['docker', 'rm']:
                return mock.Mock(returncode=0)
            return mock.Mock(returncode=0)

        with self.assertRaisesRegex(cli.RunnerError, 'client disabled flag must be absent or False'):
            cli.readback_and_verify_store(
                'test_vol', self.pinned_config_id, 'mgr_user', 'be_user',
                run_nonce='run123', task_label='dynsec-readback-helper', runner=fake_runner
            )

    def test_readback_invalid_hash_schema_rejected(self):
        cid = 'a' * 64
        bad_store = create_fake_store()
        bad_store['clients'][0]['iterations'] = 0
        readback_stdout = b'1883:1883:700\n1883:1883:600\n' + json.dumps(bad_store).encode('utf-8')
        helper_inspect = [{
            'HostConfig': {'Tmpfs': cli.READBACK_TMPFS},
            'Mounts': [{'Type': 'volume', 'Name': 'test_vol', 'Destination': '/mosquitto/dynsec', 'RW': False}],
            'Id': cid,
            'Name': '/dynsec-readback-helper-123',
            'Image': self.pinned_config_id,
            'Config': {'Labels': {'test.run': 'run123', 'test.task': 'dynsec-readback-helper'}},
            'State': {'ExitCode': 0, 'Status': 'exited'}
        }]

        def fake_runner(cmd, **kwargs):
            if cmd[:2] == ['docker', 'create']:
                return mock.Mock(returncode=0, stdout=cid.encode('utf-8'))
            elif cmd[:2] == ['docker', 'start']:
                return mock.Mock(returncode=0, stdout=readback_stdout)
            elif cmd[:3] == ['docker', 'container', 'inspect']:
                if not hasattr(fake_runner, 'inspected'):
                    fake_runner.inspected = True
                    return mock.Mock(returncode=0, stdout=json.dumps(helper_inspect).encode('utf-8'))
                return mock.Mock(returncode=1, stderr=b'no such container')
            elif cmd[:2] == ['docker', 'rm']:
                return mock.Mock(returncode=0)
            return mock.Mock(returncode=0)

        with self.assertRaisesRegex(cli.RunnerError, 'client native hash schema invalid'):
            cli.readback_and_verify_store(
                'test_vol', self.pinned_config_id, 'mgr_user', 'be_user',
                run_nonce='run123', task_label='dynsec-readback-helper', runner=fake_runner
            )

    # --- DEFECT 4: Leakage & Secret Scanning ---
    def test_run_cmd_fixed_diagnostic_on_exception(self):
        def failing_runner(*args, **kwargs):
            raise OSError("SecretKey=1234567890abcdef at path /internal")

        with self.assertRaises(cli.RunnerError) as ctx:
            cli.run_cmd(['docker', 'version'], runner=failing_runner)
        err_msg = str(ctx.exception)
        self.assertNotIn("SecretKey", err_msg)
        self.assertEqual(err_msg, "command execution failed")

    def test_secret_in_stdout_or_stderr_raises_leakage_error(self):
        secret = "secret_password_value_12345"
        with tempfile.NamedTemporaryFile('w') as tf:
            tf.write('{}')
            tf.flush()
            runner = mock.Mock(return_value=mock.Mock(
                returncode=0,
                stdout=f"{{'status': 'store_created', 'leak': '{secret}'}}".encode(),
                stderr=b""
            ))
            with self.assertRaisesRegex(cli.RunnerError, 'plaintext password detected in process output'):
                cli.execute_bootstrap(Path(tf.name), secrets_to_scan=[secret], runner=runner)

    # --- DEFECT 2: Partial Bootstrap Failure & Cleanup Resilience ---
    def test_bootstrap_failure_with_owned_leftover_volume_cleaned(self):
        volume_cleaned = False

        def mock_runner(cmd, **kwargs):
            nonlocal volume_cleaned
            c0, c1 = cmd[0], cmd[1]
            if c0 == sys.executable:
                return mock.Mock(returncode=1, stderr=b"bootstrap failed internally", stdout=b"")
            if c0 == 'docker':
                if c1 == 'image':
                    return mock.Mock(returncode=0, stdout=json.dumps(self.fake_image_inspect).encode())
                elif c1 == 'ps':
                    return mock.Mock(returncode=0, stdout=b"")
                elif c1 == 'volume':
                    sub = cmd[2]
                    if sub == 'inspect':
                        if not hasattr(mock_runner, 'preflight'):
                            mock_runner.preflight = True
                            return mock.Mock(returncode=1, stderr=b"no such volume")
                        if not hasattr(mock_runner, 'inspected'):
                            mock_runner.inspected = True
                            rec = [{'Name': cmd[3], 'Labels': {
                                'com.docker.compose.volume': 'mosquitto_dynsec',
                                'bootstrap.managed': 'true',
                                'test.task': 'dynsec-bootstrap-test',
                                'test.run': '11223344556677889900aabbccddeeff'
                            }}]
                            return mock.Mock(returncode=0, stdout=json.dumps(rec).encode())
                        return mock.Mock(returncode=1, stderr=b"no such volume")
                    elif sub == 'rm':
                        volume_cleaned = True
                        return mock.Mock(returncode=0)
            return mock.Mock(returncode=0)

        with mock.patch('secrets.token_hex') as mock_hex, \
             mock.patch('secrets.token_urlsafe', return_value='password_123456789012345678901234'):
            mock_hex.side_effect = [
                '11223344556677889900aabbccddeeff',  # run_nonce
                '1111',  # manager_user suffix
                '2222',  # backend_user suffix
            ]
            expected_vol = 'mosquitto_dynsec_11223344556677889900aabb'

            def mock_runner_wrapper(cmd, **kwargs):
                c = list(cmd)
                if len(c) >= 4 and c[0] == 'docker' and c[1] == 'volume' and c[2] == 'inspect' and c[3] == expected_vol:
                    if not hasattr(mock_runner, 'preflight'):
                        mock_runner.preflight = True
                        return mock.Mock(returncode=1, stderr=b'no such volume')
                    if not hasattr(mock_runner, 'inspected'):
                        mock_runner.inspected = True
                        rec = [{'Name': expected_vol, 'Labels': {
                            'com.docker.compose.volume': 'mosquitto_dynsec',
                            'bootstrap.managed': 'true',
                            'test.task': 'dynsec-bootstrap-test',
                            'test.run': '11223344556677889900aabbccddeeff'
                        }}]
                        return mock.Mock(returncode=0, stdout=json.dumps(rec).encode())
                    if not hasattr(mock_runner, 'receipt_checked'):
                        mock_runner.receipt_checked = True
                        rec = [{'Name': expected_vol, 'Labels': {
                            'com.docker.compose.volume': 'mosquitto_dynsec',
                            'bootstrap.managed': 'true',
                            'test.task': 'dynsec-bootstrap-test',
                            'test.run': '11223344556677889900aabbccddeeff'
                        }}]
                        return mock.Mock(returncode=0, stdout=json.dumps(rec).encode())
                    return mock.Mock(returncode=1, stderr=b'no such volume')
                elif len(c) >= 4 and c[0] == 'docker' and c[1] == 'volume' and c[2] == 'rm' and c[3] == expected_vol:
                    nonlocal volume_cleaned
                    volume_cleaned = True
                    return mock.Mock(returncode=0)
                return mock_runner(cmd, **kwargs)

            with self.assertRaisesRegex(cli.RunnerError, 'bootstrap execution failed'):
                cli.run_pipeline(runner=mock_runner_wrapper)
        self.assertTrue(volume_cleaned, "Leftover owned volume must be cleaned up on failure")

    def test_foreign_leftover_volume_refused_deletion(self):
        volume_removed = False

        def mock_runner(cmd, **kwargs):
            nonlocal volume_removed
            c0, c1 = cmd[0], cmd[1]
            if c0 == sys.executable:
                return mock.Mock(returncode=1, stderr=b"failed", stdout=b"")
            if c0 == 'docker':
                if c1 == 'image':
                    return mock.Mock(returncode=0, stdout=json.dumps(self.fake_image_inspect).encode())
                elif c1 == 'ps':
                    return mock.Mock(returncode=0, stdout=b"")
                elif c1 == 'volume':
                    sub = cmd[2]
                    if sub == 'inspect':
                        if not hasattr(mock_runner, 'preflight'):
                            mock_runner.preflight = True
                            return mock.Mock(returncode=1, stderr=b"no such volume")
                        rec = [{'Name': cmd[3], 'Labels': {'com.docker.compose.volume': 'foreign'}}]
                        return mock.Mock(returncode=0, stdout=json.dumps(rec).encode())
                    elif sub == 'rm':
                        volume_removed = True
                        return mock.Mock(returncode=0)
            return mock.Mock(returncode=0)

        with mock.patch('secrets.token_hex', return_value='test_nonce'):
            with self.assertRaises(cli.RunnerError):
                cli.run_pipeline(runner=mock_runner)
        self.assertFalse(volume_removed, "Foreign leftover volume must NEVER be removed")

    def test_cleanup_volume_absence_verified(self):
        def fake_runner(cmd, **kwargs):
            if cmd[:3] == ['docker', 'volume', 'inspect']:
                if not hasattr(fake_runner, 'inspect_called'):
                    fake_runner.inspect_called = True
                    rec = [{'Name': 'vol1', 'Labels': {'com.docker.compose.volume': 'mosquitto_dynsec'}}]
                    return mock.Mock(returncode=0, stdout=json.dumps(rec).encode())
                return mock.Mock(returncode=1, stderr=b'no such volume')
            elif cmd[:3] == ['docker', 'volume', 'rm']:
                return mock.Mock(returncode=0)
            return mock.Mock(returncode=0)

        cli.cleanup_volume('vol1', {'com.docker.compose.volume': 'mosquitto_dynsec'}, runner=fake_runner)

    def test_cleanup_volume_failure_vetoes(self):
        def fake_runner(cmd, **kwargs):
            if cmd[:3] == ['docker', 'volume', 'inspect']:
                rec = [{'Name': 'vol1', 'Labels': {'com.docker.compose.volume': 'mosquitto_dynsec'}}]
                return mock.Mock(returncode=0, stdout=json.dumps(rec).encode())
            elif cmd[:3] == ['docker', 'volume', 'rm']:
                return mock.Mock(returncode=1, stderr=b'volume in use')
            return mock.Mock(returncode=0)

        with self.assertRaisesRegex(cli.RunnerError, 'failed to remove volume'):
            cli.cleanup_volume('vol1', {'com.docker.compose.volume': 'mosquitto_dynsec'}, runner=fake_runner)

    def test_transient_container_leakage_detected(self):
        baseline = {'cid1': 'other_container'}
        runner = mock.Mock(return_value=mock.Mock(
            returncode=0,
            stdout=b'cid1 other_container\ncid2 mqtt-boot-leaked\n'
        ))
        with self.assertRaisesRegex(cli.RunnerError, 'transient bootstrap containers leaked into daemon state'):
            cli.verify_transient_containers_baseline(baseline, runner=runner)

    def test_config_unlink_failure_tracked_in_cleanup_errors(self):
        # When unlinking the config fails, cleanup error is reported
        with mock.patch('pathlib.Path.unlink', side_effect=OSError('permission denied')), \
             mock.patch.object(cli, 'execute_bootstrap') as mock_boot:
            mock_boot.side_effect = cli.RunnerError('bootstrap execution failed')
            with self.assertRaises(cli.RunnerError):
                cli.run_pipeline(runner=mock.Mock(returncode=0))

    def test_cleanup_actions_attempted_after_earlier_failure(self):
        # Even if volume cleanup fails, transient container check is still attempted
        actions = []
        def tracking_runner(cmd, **kwargs):
            c0, c1 = cmd[0], cmd[1]
            if c0 == 'docker':
                if c1 == 'ps':
                    actions.append('container_baseline')
                    return mock.Mock(returncode=0, stdout=b'')
                elif c1 == 'image':
                    return mock.Mock(returncode=0, stdout=json.dumps(self.fake_image_inspect).encode())
                elif c1 == 'volume':
                    sub = cmd[2]
                    if sub == 'inspect':
                        actions.append('volume_inspect')
                        return mock.Mock(returncode=1, stderr=b'no such volume')
            elif c0 == sys.executable:
                actions.append('bootstrap')
                raise cli.RunnerError('bootstrap error')
            return mock.Mock(returncode=0, stdout=b'')

        with self.assertRaises(cli.RunnerError):
            cli.run_pipeline(runner=tracking_runner)
        self.assertIn('container_baseline', actions)
        self.assertIn('volume_inspect', actions)

    def _create_fake_preflight_runner(self):
        def fake_runner(cmd, **kwargs):
            c0, c1 = cmd[0], cmd[1]
            if c0 == 'docker':
                if c1 == 'ps':
                    return mock.Mock(returncode=0, stdout=b"")
                elif c1 == 'image':
                    return mock.Mock(returncode=0, stdout=json.dumps(self.fake_image_inspect).encode())
                elif c1 == 'volume':
                    # Preflight absence check
                    return mock.Mock(returncode=1, stderr=b"no such volume")
            return mock.Mock(returncode=0, stdout=b"")
        return fake_runner

    # --- DEFECT 2: Protected Configuration Creation & Failure Cleanup ---
    def test_config_creation_chmod_failure_cleans_up_directory(self):
        cleaned_dirs = []
        real_rmdir = os.rmdir

        def mock_rmdir(path):
            cleaned_dirs.append(path)
            real_rmdir(path)

        runner = self._create_fake_preflight_runner()
        with mock.patch("os.chmod", side_effect=OSError("chmod denied")), \
             mock.patch("os.rmdir", side_effect=mock_rmdir):
            with self.assertRaises(OSError):
                cli.run_pipeline(runner=runner)
        self.assertTrue(len(cleaned_dirs) > 0, "Private temp directory must be removed on chmod failure")

    def test_config_creation_open_failure_cleans_up_directory(self):
        cleaned_dirs = []
        real_rmdir = os.rmdir

        def mock_rmdir(path):
            cleaned_dirs.append(path)
            real_rmdir(path)

        runner = self._create_fake_preflight_runner()
        with mock.patch("os.open", side_effect=OSError("open denied")), \
             mock.patch("os.rmdir", side_effect=mock_rmdir):
            with self.assertRaises(OSError):
                cli.run_pipeline(runner=runner)
        self.assertTrue(len(cleaned_dirs) > 0, "Private temp directory must be removed on open failure")

    def test_config_creation_json_dump_failure_cleans_up_file_and_directory(self):
        created_paths = []
        real_open = os.open

        def mock_open(path, flags, mode=0o777):
            created_paths.append(path)
            return real_open(path, flags, mode)

        runner = self._create_fake_preflight_runner()
        with mock.patch("os.open", side_effect=mock_open), \
             mock.patch("json.dump", side_effect=ValueError("json serialization failure")):
            with self.assertRaises(ValueError):
                cli.run_pipeline(runner=runner)

        self.assertTrue(len(created_paths) > 0)
        for p in created_paths:
            self.assertFalse(os.path.exists(p), f"Created config {p} must not exist after cleanup")
            parent_dir = os.path.dirname(p)
            self.assertFalse(os.path.exists(parent_dir), f"Directory {parent_dir} must not exist after cleanup")

    # --- DEFECT 2: Bootstrap Nonce Tampering / Replacement Refusal ---
    def test_volume_cleanup_refuses_modified_bootstrap_nonce(self):
        def fake_runner(cmd, **kwargs):
            c0, c1 = cmd[0], cmd[1]
            if c0 == 'docker' and c1 == 'volume':
                if cmd[2] == 'inspect':
                    # First inspect captures original bootstrap nonce
                    if not hasattr(fake_runner, 'first_inspect'):
                        fake_runner.first_inspect = True
                        rec = [{'Name': 'vol1', 'Labels': {
                            'com.docker.compose.volume': 'mosquitto_dynsec',
                            'bootstrap.managed': 'true',
                            'bootstrap.nonce': 'orig_nonce_123',
                            'test.task': 'dynsec-bootstrap-test',
                            'test.run': 'run123'
                        }}]
                        return mock.Mock(returncode=0, stdout=json.dumps(rec).encode())
                    else:
                        # Second inspect during cleanup sees replaced volume with different bootstrap nonce!
                        rec = [{'Name': 'vol1', 'Labels': {
                            'com.docker.compose.volume': 'mosquitto_dynsec',
                            'bootstrap.managed': 'true',
                            'bootstrap.nonce': 'replaced_nonce_999',
                            'test.task': 'dynsec-bootstrap-test',
                            'test.run': 'run123'
                        }}]
                        return mock.Mock(returncode=0, stdout=json.dumps(rec).encode())
            return mock.Mock(returncode=0)

        with self.assertRaises(cli.RunnerError):
            cli.cleanup_volume(
                'vol1',
                {
                    'com.docker.compose.volume': 'mosquitto_dynsec',
                    'bootstrap.managed': 'true',
                    'bootstrap.nonce': 'orig_nonce_123',
                    'test.task': 'dynsec-bootstrap-test',
                    'test.run': 'run123'
                },
                runner=fake_runner
            )

    # --- DEFECT 5: Atomic Report Publication & Overwrite Rejection ---
    def test_report_publication_rejects_overwrite_and_symlink(self):
        with tempfile.TemporaryDirectory() as td:
            existing_file = Path(td) / "report.json"
            existing_file.write_text("{}")
            with self.assertRaisesRegex(cli.RunnerError, "report path already exists; overwriting is forbidden"):
                cli.publish_safe_report(existing_file, {"status": "success"})

            symlink_target = Path(td) / "link.json"
            symlink_target.symlink_to(existing_file)
            with self.assertRaisesRegex(cli.RunnerError, "report path already exists; overwriting is forbidden"):
                cli.publish_safe_report(symlink_target, {"status": "success"})

    def test_report_publication_temp_unlink_failure_vetoes(self):
        with tempfile.TemporaryDirectory() as td:
            target_file = Path(td) / "safe_report.json"
            with mock.patch("pathlib.Path.unlink", side_effect=OSError("unlink error")):
                with self.assertRaisesRegex(cli.RunnerError, "failed to cleanup temporary report file"):
                    cli.publish_safe_report(target_file, {"status": "success"})

    def test_full_pipeline_mocked_success_and_safe_report(self):
        cid = 'b' * 64
        inspect_called_count = 0
        bootstrap_called_count = 0
        saved_labels = {}
        readback_stdout = b'1883:1883:700\n1883:1883:600\n' + self.fake_store_raw
        helper_inspect = [{
            'Id': cid,
            'Name': '/dynsec-readback-helper-123',
            'Image': self.pinned_config_id,
            'Config': {
                'Labels': {'test.run': '11223344556677889900aabbccddeeff', 'test.task': 'dynsec-bootstrap-test'}
            },
            'HostConfig': {'Tmpfs': cli.READBACK_TMPFS},
            'Mounts': [{'Type': 'volume', 'Name': 'mosquitto_dynsec_11223344556677889900aabb', 'Destination': '/mosquitto/dynsec', 'RW': False}],
            'State': {'ExitCode': 0, 'Status': 'exited'}
        }]

        def mock_system_runner(cmd, **kwargs):
            nonlocal inspect_called_count, bootstrap_called_count, saved_labels
            c0, c1 = cmd[0], cmd[1]
            if c0 == sys.executable:
                bootstrap_called_count += 1
                if bootstrap_called_count == 1:
                    cfg_file = [cmd[i+1] for i, a in enumerate(cmd) if a == '--config'][0]
                    with open(cfg_file) as cf:
                        cfg_data = json.load(cf)
                    saved_labels = {
                        'com.docker.compose.volume': 'mosquitto_dynsec',
                        'bootstrap.managed': 'true',
                        'bootstrap.nonce': 'boot_nonce',
                    }
                    if 'labels' in cfg_data:
                        saved_labels.update(cfg_data['labels'])
                    out = {
                        'status': 'store_created',
                        'volume_name': cfg_data['volume_name'],
                        'runtime_status': 'NOT_READY',
                        'ownership': '1883:1883',
                        'mode': 'container'
                    }
                    return mock.Mock(returncode=0, stdout=json.dumps(out).encode(), stderr=b'')
                else:
                    return mock.Mock(returncode=1, stderr=b'volume collision', stdout=b'')

            if c0 == 'docker':
                if c1 == 'image' and cmd[2] == 'inspect':
                    return mock.Mock(returncode=0, stdout=json.dumps(self.fake_image_inspect).encode())
                elif c1 == 'volume':
                    sub = cmd[2]
                    if sub == 'inspect':
                        vol_name = cmd[3]
                        inspect_called_count += 1
                        if inspect_called_count == 1:
                            return mock.Mock(returncode=1, stderr=b'no such volume')
                        elif inspect_called_count >= 5:
                            return mock.Mock(returncode=1, stderr=b'no such volume')
                        else:
                            rec = [{'Name': vol_name, 'Labels': saved_labels}]
                            return mock.Mock(returncode=0, stdout=json.dumps(rec).encode())
                    elif sub == 'rm':
                        return mock.Mock(returncode=0)
                elif c1 == 'ps':
                    return mock.Mock(returncode=0, stdout=b'cid_existing existing_name\n')
                elif c1 == 'create':
                    return mock.Mock(returncode=0, stdout=cid.encode())
                elif c1 == 'start':
                    return mock.Mock(returncode=0, stdout=readback_stdout)
                elif c1 == 'rm':
                    return mock.Mock(returncode=0)
                elif c1 == 'container' and cmd[2] == 'inspect':
                    if not hasattr(mock_system_runner, 'helper_inspected_count'):
                        mock_system_runner.helper_inspected_count = 0
                    mock_system_runner.helper_inspected_count += 1
                    # Inspect 1: Helper 1 inspect exit state (0)
                    # Inspect 2: Helper 1 cleanup check (absence -> return 1 with 'no such container')
                    # Inspect 3: Helper 2 inspect exit state (0)
                    # Inspect 4: Helper 2 cleanup check (absence -> return 1 with 'no such container')
                    if mock_system_runner.helper_inspected_count in (1, 3):
                        return mock.Mock(returncode=0, stdout=json.dumps(helper_inspect).encode(), stderr=b'')
                    return mock.Mock(returncode=1, stderr=b'no such container', stdout=b'')

            return mock.Mock(returncode=0, stdout=b'')

        with tempfile.TemporaryDirectory() as td:
            rep_path = Path(td) / "safe_report.json"
            with mock.patch('secrets.token_hex') as mock_hex:
                mock_hex.side_effect = [
                    '11223344556677889900aabbccddeeff',  # run_nonce
                    '1111',  # manager
                    '2222',  # backend
                    '33333333',  # helper_nonce in readback
                    '44444444',  # helper_nonce in collision
                    '55555555',  # report temp
                ]
                with mock.patch.object(cli, 'create_fake_store', return_value=self.fake_store, create=True):
                    # Update fake_store usernames to match mgr_1111 and be_2222
                    matched_store = create_fake_store('mgr_1111', 'be_2222')
                    matched_raw = json.dumps(matched_store).encode()
                    readback_stdout = b'1883:1883:700\n1883:1883:600\n' + matched_raw
                    report = cli.run_pipeline(report_path=rep_path, runner=mock_system_runner)

            self.assertEqual(report['status'], 'success')
            self.assertTrue(report['cleaned_up'])
            self.assertTrue(report['collision_rejected'])
            self.assertEqual(report['ownership'], '1883:1883')
            self.assertEqual(report['runtime_status'], 'NOT_READY')
            self.assertEqual(set(report.keys()), cli.SAFE_REPORT_KEYS)
            self.assertEqual(report['image_ref'], 'eclipse-mosquitto@sha256:d12c8f80dfc65b768bb9acecc7ef182b976f71fb681640b66358e5e0cf94e9e9')
            self.assertEqual(report['image_digest'], 'sha256:d12c8f80dfc65b768bb9acecc7ef182b976f71fb681640b66358e5e0cf94e9e9')
            self.assertNotEqual(report['image_digest'], cli.PINNED_CONFIG_ID)

            with open(rep_path) as rf:
                saved = json.load(rf)
            self.assertEqual(saved['status'], 'success')
            saved_str = json.dumps(saved)
            self.assertNotIn('password', saved_str.lower())
            self.assertNotIn('salt', saved_str.lower())
            self.assertNotIn('iterations', saved_str.lower())



class CustomInterruption(BaseException):
    """Custom BaseException subclass to verify generic Exception handlers do not catch it."""
    pass


class TestDynsecBootstrapCliCancellation(unittest.TestCase):
    def test_native_child_wait_has_finite_budget(self):
        import ast
        import inspect

        tree = ast.parse(inspect.getsource(cli.run_bootstrap_command))
        unbounded = [node for node in ast.walk(tree)
                     if isinstance(node, ast.keyword) and node.arg == 'timeout'
                     and isinstance(node.value, ast.Constant) and node.value.value is None]
        self.assertEqual(unbounded, [], 'Native cancellation cannot wait indefinitely')

    def test_dedicated_interruption_bypasses_generic_exception_handler(self):
        self.assertTrue(issubclass(cli.RunnerInterrupted, BaseException))
        self.assertFalse(issubclass(cli.RunnerInterrupted, Exception))
        with self.assertRaises(cli.RunnerInterrupted):
            cli.run_cmd(['unused'], runner=mock.Mock(side_effect=cli.RunnerInterrupted(15)))

    def test_first_signal_raises_and_repeated_signal_is_suppressed(self):
        for signum in (signal.SIGINT, signal.SIGTERM):
            with self.subTest(signum=signum), cli.cancellation_scope() as state:
                handler = signal.getsignal(signum)
                with self.assertRaises(cli.RunnerInterrupted):
                    handler(signum, None)
                handler(signal.SIGINT, None)
                self.assertEqual(state.signum, signum)

    def test_signal_during_cleanup_cannot_turn_into_success(self):
        with cli.cancellation_scope() as state:
            state.unwinding = True
            state.handle(signal.SIGINT, None)
            self.assertEqual(state.signum, signal.SIGINT)

    def test_deferred_helper_creation_signal_still_cleans_receipt(self):
        cid = 'd' * 64
        removed = []

        def runner(cmd, **kwargs):
            if cmd[:2] == ['docker', 'create']:
                signal.getsignal(signal.SIGINT)(signal.SIGINT, None)
                return mock.Mock(returncode=0, stdout=cid.encode())
            if cmd[:2] == ['docker', 'rm']:
                removed.append(cmd[-1])
                return mock.Mock(returncode=0)
            if removed:
                return mock.Mock(returncode=1, stderr=b'no such container')
            receipt = [{'Id': cid, 'Image': cli.PINNED_IMAGE,
                        'HostConfig': {'Tmpfs': cli.READBACK_TMPFS},
                        'Mounts': [{'Type': 'volume', 'Name': 'vol', 'Destination': '/mosquitto/dynsec', 'RW': False}],
                        'Config': {'Labels': {'test.task': 'task', 'test.run': 'run'}}}]
            return mock.Mock(returncode=0, stdout=json.dumps(receipt).encode())

        with cli.cancellation_scope():
            with self.assertRaises(cli.RunnerInterrupted):
                cli.readback_and_verify_store('vol', cli.PINNED_IMAGE, 'mgr', 'be',
                                              run_nonce='run', task_label='task', runner=runner)
        self.assertEqual(removed, [cid])

    def test_handler_restoration_failure_vetoes(self):
        real_signal = signal.signal
        calls = []

        def failing_restore(signum, handler):
            calls.append(signum)
            if len(calls) == 3:
                raise OSError('sensitive failure detail')
            return real_signal(signum, handler)

        old_int = signal.getsignal(signal.SIGINT)
        old_term = signal.getsignal(signal.SIGTERM)
        try:
            with mock.patch.object(signal, 'signal', side_effect=failing_restore):
                with self.assertRaisesRegex(cli.RunnerError, '^signal handler restoration failed$'):
                    with cli.cancellation_scope():
                        pass
            self.assertEqual(len(calls), 4)
        finally:
            real_signal(signal.SIGINT, old_int)
            real_signal(signal.SIGTERM, old_term)

    def test_child_boundary_defers_signal_until_child_returns(self):
        events = []
        process = mock.Mock(pid=12345, returncode=0)
        def wait(**kwargs):
            self.assertGreater(kwargs['timeout'], 0)
            signal.getsignal(signal.SIGTERM)(signal.SIGTERM, None)
            events.append('child completed')
            return 0
        process.wait.side_effect = wait

        with cli.cancellation_scope():
            with mock.patch.object(cli.subprocess, 'Popen', return_value=process) as spawn:
                with self.assertRaises(cli.RunnerInterrupted):
                    cli.run_bootstrap_command(['child'])
                self.assertTrue(spawn.call_args.kwargs['start_new_session'])
        self.assertEqual(events, ['child completed'])

    def test_deadline_cooperates_then_reaps(self):
        process = mock.Mock(pid=12345, returncode=1)
        process.wait.side_effect = [subprocess.TimeoutExpired('child', 1), 1]
        with mock.patch.object(cli.subprocess, 'Popen', return_value=process), \
                mock.patch.object(cli.time, 'monotonic', side_effect=[0, 0, 2]), \
                mock.patch.object(cli.os, 'killpg', side_effect=ProcessLookupError) as kill:
            with self.assertRaisesRegex(cli.RunnerError, 'deadline exceeded'):
                cli.run_bootstrap_command(['child'], timeout=1)
        process.send_signal.assert_called_once_with(signal.SIGTERM)
        kill.assert_called_once_with(12345, 0)
        self.assertEqual(process.wait.call_count, 2)

    def test_escalation_vetoes_cleanup_even_after_join(self):
        process = mock.Mock(pid=12345)
        process.wait.side_effect = [subprocess.TimeoutExpired('child', 1),
                                   subprocess.TimeoutExpired('child', 1), 0]
        with mock.patch.object(cli.subprocess, 'Popen', return_value=process), \
                mock.patch.object(cli.time, 'monotonic', side_effect=[0, 0, 2]), \
                mock.patch.object(cli.os, 'killpg') as kill:
            with self.assertRaisesRegex(cli.IncompleteCleanup, 'INCOMPLETE_CLEANUP'):
                cli.run_bootstrap_command(['child'], timeout=1, grace=1)
        process.send_signal.assert_called_once_with(signal.SIGTERM)
        kill.assert_called_once_with(12345, signal.SIGKILL)
        self.assertEqual(process.wait.call_count, 3)

    def test_live_child_join_failure_is_incomplete(self):
        process = mock.Mock(pid=12345)
        process.wait.side_effect = subprocess.TimeoutExpired('child', 1)
        with mock.patch.object(cli.subprocess, 'Popen', return_value=process), \
                mock.patch.object(cli.time, 'monotonic', side_effect=[0, 0, 2]), \
                mock.patch.object(cli.os, 'killpg'):
            with self.assertRaises(cli.IncompleteCleanup):
                cli.run_bootstrap_command(['child'], timeout=1, grace=1)

    def test_invalid_budgets_rejected_before_spawn(self):
        for budget in (0, -1, float('inf'), float('nan')):
            with mock.patch.object(cli.subprocess, 'Popen') as spawn:
                with self.assertRaises(cli.RunnerError):
                    cli.run_bootstrap_command(['child'], timeout=budget)
                spawn.assert_not_called()

    def test_first_signal_is_forwarded_to_exact_child_and_joined(self):
        process = mock.Mock(pid=12345, returncode=1)
        def first_wait(**kwargs):
            signal.getsignal(signal.SIGTERM)(signal.SIGTERM, None)
            raise subprocess.TimeoutExpired('child', kwargs['timeout'])
        calls = []
        def wait(**kwargs):
            calls.append(kwargs['timeout'])
            if len(calls) == 1:
                return first_wait(**kwargs)
            return 1
        process.wait.side_effect = wait
        with cli.cancellation_scope(), \
                mock.patch.object(cli.subprocess, 'Popen', return_value=process), \
                mock.patch.object(cli.os, 'killpg', side_effect=ProcessLookupError) as kill:
            with self.assertRaises(cli.RunnerInterrupted):
                cli.run_bootstrap_command(['child'], grace=2)
        process.send_signal.assert_called_once_with(signal.SIGTERM)
        self.assertEqual(calls[-1], 2)
        kill.assert_called_once_with(12345, 0)

    def test_child_cleanup_flag_vetoes_success(self):
        process = mock.Mock(pid=12345, returncode=0)
        def spawn(cmd, **kwargs):
            kwargs['stderr'].write(b'incomplete cleanup; protected recovery receipt')
            return process
        with mock.patch.object(cli.subprocess, 'Popen', side_effect=spawn), \
                mock.patch.object(cli.os, 'killpg', side_effect=ProcessLookupError):
            with self.assertRaises(cli.IncompleteCleanup):
                cli.run_bootstrap_command(['child'])

    def test_live_descendant_vetoes_completed_child_success(self):
        process = mock.Mock(pid=12345, returncode=0)
        with mock.patch.object(cli.subprocess, 'Popen', return_value=process), \
                mock.patch.object(cli.os, 'killpg') as group:
            with self.assertRaisesRegex(cli.IncompleteCleanup, 'descendants still alive'):
                cli.run_bootstrap_command(['child'])
        group.assert_called_once_with(12345, 0)

    def test_escalated_pipeline_keeps_inputs_and_does_not_remove_volume(self):
        with tempfile.TemporaryDirectory(dir='/tmp/opencode') as directory:
            private = Path(directory) / 'retained'
            private.mkdir(mode=0o700)
            with mock.patch.object(cli, 'get_container_baseline', return_value={}), \
                    mock.patch.object(cli, 'verify_pinned_image', return_value='sha256:' + 'a'*64), \
                    mock.patch.object(cli, 'check_volume_absent'), \
                    mock.patch.object(cli.tempfile, 'mkdtemp', return_value=str(private)), \
                    mock.patch.object(cli, 'execute_bootstrap', side_effect=cli.IncompleteCleanup('INCOMPLETE_CLEANUP')), \
                    mock.patch.object(cli, 'cleanup_volume') as remove:
                with self.assertRaises(cli.IncompleteCleanup):
                    cli.run_pipeline(runner=mock.Mock())
                remove.assert_not_called()
                self.assertTrue((private / 'config.json').exists())
                self.assertEqual((private / 'config.json').stat().st_mode & 0o777, 0o600)

    def setUp(self):
        self.pinned_image = cli.PINNED_IMAGE
        self.pinned_config_id = cli.PINNED_CONFIG_ID
        self.fake_image_inspect = [{
            'Architecture': 'amd64',
            'Os': 'linux',
            'RepoDigests': [self.pinned_image],
            'Id': cli.PINNED_CONFIG_ID
        }]
        self.fake_store = create_fake_store('mgr_1111', 'be_2222')
        self.fake_store_raw = json.dumps(self.fake_store).encode('utf-8')

    def test_interruption_not_suppressed_and_no_success_report(self):
        """Interruption via BaseException must propagate as RunnerInterrupted / not return success report."""
        def mock_runner(cmd, **kwargs):
            c0 = cmd[0]
            if c0 == 'docker':
                if cmd[1] == 'image':
                    return mock.Mock(returncode=0, stdout=json.dumps(self.fake_image_inspect).encode())
                elif cmd[1] == 'ps':
                    return mock.Mock(returncode=0, stdout=b'')
                elif cmd[1] == 'volume' and cmd[2] == 'inspect':
                    return mock.Mock(returncode=1, stderr=b'no such volume')
            elif c0 == sys.executable:
                raise CustomInterruption("Simulated SIGTERM delivery during bootstrap")
            return mock.Mock(returncode=0, stdout=b'')

        with tempfile.TemporaryDirectory() as td:
            rep_path = Path(td) / "safe_report.json"
            # It must raise RunnerInterrupted (a BaseException) or CustomInterruption, never return success report
            with self.assertRaises((cli.RunnerInterrupted, CustomInterruption)):
                cli.run_pipeline(report_path=rep_path, runner=mock_runner)
            self.assertFalse(rep_path.exists(), "Success report must not be published on interruption")

    def test_signal_handlers_restored_reliably(self):
        """Prior signal handlers for SIGINT and SIGTERM must be restored after execution or interruption."""
        import signal
        original_int = signal.getsignal(signal.SIGINT)
        original_term = signal.getsignal(signal.SIGTERM)

        def mock_runner(cmd, **kwargs):
            if cmd[0] == sys.executable:
                # During execution, temporary handler should be in place
                current_int = signal.getsignal(signal.SIGINT)
                current_term = signal.getsignal(signal.SIGTERM)
                self.assertNotEqual(current_int, original_int)
                self.assertNotEqual(current_term, original_term)
                raise cli.RunnerInterrupted(signal.SIGTERM)
            elif cmd[0] == 'docker':
                if cmd[1] == 'image':
                    return mock.Mock(returncode=0, stdout=json.dumps(self.fake_image_inspect).encode())
                elif cmd[1] == 'ps':
                    return mock.Mock(returncode=0, stdout=b'')
                elif cmd[1] == 'volume' and cmd[2] == 'inspect':
                    return mock.Mock(returncode=1, stderr=b'no such volume')
            return mock.Mock(returncode=0, stdout=b'')

        try:
            cli.run_pipeline(runner=mock_runner)
        except cli.RunnerInterrupted:
            pass

        self.assertEqual(signal.getsignal(signal.SIGINT), original_int)
        self.assertEqual(signal.getsignal(signal.SIGTERM), original_term)

    def test_interruption_cleans_protected_input_and_exact_owned_volume(self):
        """When interrupted after volume creation, protected config and exact owned volume are cleaned."""
        volume_cleaned = False
        config_path = None

        with mock.patch('secrets.token_hex') as mock_hex, \
             mock.patch('secrets.token_urlsafe', return_value='pwd_123456789012345678901234'):
            mock_hex.side_effect = [
                '11223344556677889900aabbccddeeff',  # run_nonce
                '1111',  # manager
                '2222',  # backend
            ]
            expected_vol = 'mosquitto_dynsec_11223344556677889900aabb'

            def mock_runner(cmd, **kwargs):
                nonlocal volume_cleaned, config_path
                c0 = cmd[0]
                if c0 == sys.executable:
                    config_path = Path(cmd[cmd.index('--config') + 1])
                    self.assertEqual(stat.S_IMODE(config_path.stat().st_mode), 0o600)
                    self.assertEqual(stat.S_IMODE(config_path.parent.stat().st_mode), 0o700)
                    # Bootstrap succeeds and creates volume
                    out = {
                        'status': 'store_created',
                        'volume_name': expected_vol,
                        'runtime_status': 'NOT_READY',
                        'ownership': '1883:1883',
                        'mode': 'container'
                    }
                    return mock.Mock(returncode=0, stdout=json.dumps(out).encode(), stderr=b'')
                if c0 == 'docker':
                    if cmd[1] == 'image':
                        return mock.Mock(returncode=0, stdout=json.dumps(self.fake_image_inspect).encode())
                    elif cmd[1] == 'ps':
                        return mock.Mock(returncode=0, stdout=b'')
                    elif cmd[1] == 'volume':
                        sub = cmd[2]
                        if sub == 'inspect':
                            if not hasattr(mock_runner, 'preflight'):
                                mock_runner.preflight = True
                                return mock.Mock(returncode=1, stderr=b'no such volume')
                            # After bootstrap, inspect returns owned volume receipt
                            rec = [{'Name': expected_vol, 'Labels': {
                                'com.docker.compose.volume': 'mosquitto_dynsec',
                                'bootstrap.managed': 'true',
                                'bootstrap.nonce': 'boot_nonce_123',
                                'test.task': 'dynsec-bootstrap-test',
                                'test.run': '11223344556677889900aabbccddeeff'
                            }}]
                            if not hasattr(mock_runner, 'verified'):
                                mock_runner.verified = True
                                # Interruption right after receipt verification
                                raise cli.RunnerInterrupted(15)  # SIGTERM
                            # During cleanup
                            if not volume_cleaned:
                                return mock.Mock(returncode=0, stdout=json.dumps(rec).encode())
                            return mock.Mock(returncode=1, stderr=b'no such volume')
                        elif sub == 'rm' and cmd[3] == expected_vol:
                            volume_cleaned = True
                            return mock.Mock(returncode=0)
                return mock.Mock(returncode=0, stdout=b'')

            with self.assertRaises(cli.RunnerInterrupted):
                cli.run_pipeline(runner=mock_runner)

            self.assertTrue(volume_cleaned, "Owned volume must be cleaned up on interruption")
            self.assertFalse(config_path.exists())
            self.assertFalse(config_path.parent.exists())

    def test_interruption_during_readback_cleans_helper_container(self):
        """When interrupted while helper container runs, helper container must be cleaned."""
        cid = 'c' * 64
        helper_cleaned = False

        helper_inspect = [{
            'Id': cid,
            'Name': '/dynsec-readback-helper-interrupted',
            'HostConfig': {'Tmpfs': cli.READBACK_TMPFS},
            'Mounts': [{'Type': 'volume', 'Name': 'test_vol', 'Destination': '/mosquitto/dynsec', 'RW': False}],
            'Image': self.pinned_config_id,
            'Config': {
                'Labels': {'test.run': 'run123', 'test.task': 'dynsec-readback-helper'}
            },
            'State': {'ExitCode': 0, 'Status': 'running'}
        }]

        def mock_runner(cmd, **kwargs):
            nonlocal helper_cleaned
            c0, c1 = cmd[0], cmd[1]
            if c0 == 'docker':
                if c1 == 'create':
                    return mock.Mock(returncode=0, stdout=cid.encode())
                elif c1 == 'start':
                    # Signal arrives during helper execution
                    raise cli.RunnerInterrupted(2)  # SIGINT
                elif c1 == 'container' and cmd[2] == 'inspect':
                    if not helper_cleaned:
                        return mock.Mock(returncode=0, stdout=json.dumps(helper_inspect).encode())
                    return mock.Mock(returncode=1, stderr=b'no such container')
                elif c1 == 'rm' and cmd[2] == '-f' and cmd[3] == cid:
                    helper_cleaned = True
                    return mock.Mock(returncode=0)
            return mock.Mock(returncode=0)

        with self.assertRaises(cli.RunnerInterrupted):
            cli.readback_and_verify_store(
                'test_vol', self.pinned_config_id, 'mgr_user', 'be_user',
                run_nonce='run123', task_label='dynsec-readback-helper', runner=mock_runner
            )
        self.assertTrue(helper_cleaned, "Helper container must be force-removed when interrupted during execution")

    def test_foreign_resources_not_deleted_on_interruption(self):
        """Foreign leftover volume must not be deleted on interruption."""
        foreign_vol_deleted = False

        def mock_runner(cmd, **kwargs):
            nonlocal foreign_vol_deleted
            c0 = cmd[0]
            if c0 == sys.executable:
                raise cli.RunnerInterrupted(15)
            if c0 == 'docker':
                if cmd[1] == 'image':
                    return mock.Mock(returncode=0, stdout=json.dumps(self.fake_image_inspect).encode())
                elif cmd[1] == 'ps':
                    return mock.Mock(returncode=0, stdout=b'')
                elif cmd[1] == 'volume':
                    if cmd[2] == 'inspect':
                        if not hasattr(mock_runner, 'preflight'):
                            mock_runner.preflight = True
                            return mock.Mock(returncode=1, stderr=b'no such volume')
                        # Foreign labels
                        rec = [{'Name': cmd[3], 'Labels': {'com.docker.compose.volume': 'foreign'}}]
                        return mock.Mock(returncode=0, stdout=json.dumps(rec).encode())
                    elif cmd[2] == 'rm':
                        foreign_vol_deleted = True
                        return mock.Mock(returncode=0)
            return mock.Mock(returncode=0)

        with self.assertRaisesRegex(cli.RunnerError, 'cleanup failure'):
            cli.run_pipeline(runner=mock_runner)
        self.assertFalse(foreign_vol_deleted, "Foreign volume must NEVER be deleted during interruption unwind")

    def test_cleanup_failure_vetoes_on_interruption(self):
        """If volume cleanup fails during interruption unwind, it must raise RunnerError or aggregate failure."""
        def mock_runner(cmd, **kwargs):
            c0 = cmd[0]
            if c0 == sys.executable:
                out = {
                    'status': 'store_created',
                    'volume_name': 'mosquitto_dynsec_run123',
                    'runtime_status': 'NOT_READY',
                    'ownership': '1883:1883',
                    'mode': 'container'
                }
                return mock.Mock(returncode=0, stdout=json.dumps(out).encode(), stderr=b'')
            if c0 == 'docker':
                if cmd[1] == 'image':
                    return mock.Mock(returncode=0, stdout=json.dumps(self.fake_image_inspect).encode())
                elif cmd[1] == 'ps':
                    return mock.Mock(returncode=0, stdout=b'')
                elif cmd[1] == 'volume':
                    if cmd[2] == 'inspect':
                        if not hasattr(mock_runner, 'preflight'):
                            mock_runner.preflight = True
                            return mock.Mock(returncode=1, stderr=b'no such volume')
                        rec = [{'Name': cmd[3], 'Labels': {
                            'com.docker.compose.volume': 'mosquitto_dynsec',
                            'bootstrap.managed': 'true',
                            'bootstrap.nonce': 'boot_nonce',
                            'test.task': 'dynsec-bootstrap-test',
                            'test.run': 'run123'
                        }}]
                        if not hasattr(mock_runner, 'interrupted'):
                            mock_runner.interrupted = True
                            raise cli.RunnerInterrupted(15)
                        # During cleanup inspect
                        return mock.Mock(returncode=0, stdout=json.dumps(rec).encode())
                    elif cmd[2] == 'rm':
                        # Cleanup fails!
                        return mock.Mock(returncode=1, stderr=b'volume in use')
            return mock.Mock(returncode=0)

        with mock.patch('secrets.token_hex', return_value='run123'):
            with self.assertRaises(cli.RunnerError) as ctx:
                cli.run_pipeline(runner=mock_runner)
            self.assertIn("cleanup failure", str(ctx.exception))

    def test_main_cli_reports_fixed_safe_interrupted_message(self):
        """CLI main() handles RunnerInterrupted and prints fixed safe message with no raw trace."""
        import io
        from unittest.mock import patch

        with patch.object(cli, 'run_pipeline', side_effect=cli.RunnerInterrupted(15)), patch.object(sys, 'argv', ['runner']):
            stderr_buf = io.StringIO()
            with patch('sys.stderr', stderr_buf):
                exit_code = cli.main()
            self.assertEqual(exit_code, 1)
            err_output = stderr_buf.getvalue()
            self.assertIn("RunnerError: execution interrupted (SIGTERM)", err_output)
            self.assertNotIn("Traceback", err_output)


if __name__ == '__main__':
    unittest.main()
