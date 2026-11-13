"""Focused tests for the bounded Linux E2 resource snapshot."""
import importlib.util
import json
import sys
import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path
from types import SimpleNamespace

MODULE_PATH = Path(__file__).with_name('paper_p8_e2_resources.py')
SPEC = importlib.util.spec_from_file_location('paper_p8_e2_resources', MODULE_PATH)
resources = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = resources
SPEC.loader.exec_module(resources)


class Registry:
    def __init__(self, objects):
        self.objects = objects
        self.calls = []

    def resolve(self, entry):
        self.calls.append(entry)
        return self.objects.get(entry)


def write(root, path, content):
    target = root / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content, encoding='ascii')


class ResourceReaderTests(unittest.TestCase):
    def fixture(self, root):
        proc, cg, sysroot = root / 'proc', root / 'cg', root / 'sys'
        cgroup = cg / 'docker' / 'abc'
        write(proc, '123/cgroup', '0::/docker/abc\n')
        for name, value in {
            'cpu.max': '50000 100000\n',
            'cpu.stat': 'usage_usec 100\nuser_usec 60\nsystem_usec 40\nnr_periods 3\nnr_throttled 1\nthrottled_usec 2\n',
            'memory.current': '1000\n', 'memory.max': 'max\n',
            'memory.swap.current': '0\n', 'memory.swap.max': 'max\n',
            'memory.events': 'low 0\nhigh 1\nmax 0\noom 0\noom_kill 0\n',
            'io.stat': '8:0 rbytes=10 wbytes=20 rios=1 wios=2\n',
        }.items():
            write(cgroup, name, value)
        write(proc, 'stat', 'cpu 100 2 30 400 5 6 7 8 9 10\n')
        write(proc, 'meminfo', 'MemTotal: 1000 kB\nMemFree: 200 kB\nMemAvailable: 600 kB\nSwapTotal: 300 kB\nSwapFree: 250 kB\n')
        write(proc, 'vmstat', 'pswpin 4\npswpout 5\n')
        write(proc, 'diskstats', '8 0 sda 10 1 20 30 40 2 50 60 7 80 90\n8 1 sda1 1 0 2 3 4 0 5 6 1 8 9\n')
        (sysroot / 'dev/block/8:0').mkdir(parents=True)
        (sysroot / 'dev/block/8:1').mkdir(parents=True)
        (sysroot / 'dev/block/8:1/partition').write_text('1', encoding='ascii')
        return proc, cg, sysroot

    def test_valid_v2_and_host_counters_are_sanitized(self):
        with tempfile.TemporaryDirectory(dir='/var/tmp/opencode') as tmp:
            proc, cg, sysroot = self.fixture(Path(tmp))
            entry = {'kind': 'backend', 'Id': 'abc'}
            registry = Registry({id(entry): None})
            # Dict entries are unhashable; resolution is by object identity.
            registry.resolve = lambda item: SimpleNamespace(Id='container-id', State=SimpleNamespace(Pid=123))
            result = resources.read_linux_resources(registry, [entry], proc_root=proc, cgroup_root=cg, sys_root=sysroot)
            backend = result['containers'][0]
            self.assertTrue(backend['available'])
            self.assertEqual(backend['cpu']['quota']['value']['quota_usec'], 50000)
            self.assertEqual(backend['memory']['max_bytes']['value'], None)
            self.assertEqual(backend['io']['value'][0]['write_bytes'], 20)
            self.assertEqual(result['host']['cpu']['value']['iowait_seconds'], 0.05)
            self.assertEqual(result['host']['diskstats']['value'][0]['device'], 'sda')
            self.assertEqual(len(result['host']['diskstats']['value']), 1)
            encoded = json.dumps(result, allow_nan=False)
            self.assertNotIn('/docker/abc', encoded)
            self.assertNotIn('container-id', encoded)

    def test_unlimited_quota_and_v1_missing_and_resolve_failure_are_unavailable(self):
        with tempfile.TemporaryDirectory(dir='/var/tmp/opencode') as tmp:
            root = Path(tmp)
            proc, cg, sysroot = self.fixture(root)
            write(proc, '123/cgroup', '5:cpu:/legacy\n')
            write(cg, 'docker/abc/cpu.max', 'max 100000\n')
            entry = SimpleNamespace(kind='postgres')
            registry = Registry({})
            registry.resolve = lambda _: SimpleNamespace(Id='id', State=SimpleNamespace(Pid=123))
            result = resources.read_linux_resources(registry, [entry], proc_root=proc, cgroup_root=cg, sys_root=sysroot)
            self.assertFalse(result['containers'][0]['available'])
            self.assertEqual(result['containers'][0]['reason'], 'cgroup_v2_unavailable')
            entry2 = SimpleNamespace(kind='backend')
            registry.resolve = lambda _: None
            result = resources.read_linux_resources(registry, [entry2], proc_root=proc, cgroup_root=cg, sys_root=sysroot)
            self.assertEqual(result['containers'][0]['reason'], 'ownership_unverified')

    def test_escape_and_malformed_values_fail_closed_without_secrets(self):
        with tempfile.TemporaryDirectory(dir='/var/tmp/opencode') as tmp:
            proc, cg, sysroot = self.fixture(Path(tmp))
            write(proc, '123/cgroup', '0::/../../etc\n')
            entry = SimpleNamespace(kind='backend')
            registry = Registry({})
            registry.resolve = lambda _: SimpleNamespace(Id='secret-id', State=SimpleNamespace(Pid=123), Config={'Env': ['PASSWORD=hunter2']})
            result = resources.read_linux_resources(registry, [entry], proc_root=proc, cgroup_root=cg, sys_root=sysroot)
            self.assertEqual(result['containers'][0]['reason'], 'invalid_cgroup_path')
            write(proc, '123/cgroup', '0::/docker/abc\n')
            write(cg, 'docker/abc/memory.current', '-1\n')
            result = resources.read_linux_resources(registry, [entry], proc_root=proc, cgroup_root=cg, sys_root=sysroot)
            self.assertEqual(result['containers'][0]['memory']['current_bytes'],
                             {'available': False, 'reason': 'malformed'})
            encoded = json.dumps(result, allow_nan=False)
            self.assertNotIn('hunter2', encoded)
            self.assertNotIn('secret-id', encoded)

    def test_bounds_container_and_device_counts_and_unlimited_quota(self):
        with tempfile.TemporaryDirectory(dir='/var/tmp/opencode') as tmp:
            proc, cg, sysroot = self.fixture(Path(tmp))
            write(proc, '123/cgroup', '0::/docker/abc\n')
            write(cg, 'docker/abc/cpu.max', 'max 100000\n')
            entries = [SimpleNamespace(kind='backend') for _ in range(10)]
            registry = Registry({})
            registry.resolve = lambda _: SimpleNamespace(Id='x', State=SimpleNamespace(Pid=123))
            disk_lines = []
            for index in range(18):
                device = sysroot / 'dev/block' / f'9:{index}'
                device.mkdir(parents=True, exist_ok=True)
                disk_lines.append(f'9 {index} d{index} 1 0 2 3 4 0 5 6 1 8 9')
            write(proc, 'diskstats', '\n'.join(disk_lines) + '\n')
            result = resources.read_linux_resources(registry, entries, proc_root=proc, cgroup_root=cg, sys_root=sysroot)
            self.assertLessEqual(len(result['containers']), resources.MAX_CONTAINERS)
            self.assertIsNone(result['containers'][0]['cpu']['quota']['value']['quota_usec'])
            self.assertEqual(len(result['host']['diskstats']['value']), resources.MAX_DEVICES)

    def test_native_receipt_mapping_roles_io_discard_fields_and_timestamps(self):
        with tempfile.TemporaryDirectory(dir='/var/tmp/opencode') as tmp:
            proc, cg, sysroot = self.fixture(Path(tmp))
            write(cg, 'docker/abc/io.stat',
                  '8:0 rbytes=10 wbytes=20 rios=1 wios=2 dbytes=30 dios=4\n')
            backend = {'name': 'ncs-run-backend-container', 'receipt': {'id': 'backend-receipt'}}
            postgres = {'name': 'ncs-run-postgres-container', 'receipt': {'id': 'postgres-receipt'}}
            calls = []

            class OwnedRegistry:
                def resolve(self, entry):
                    calls.append(entry)
                    return SimpleNamespace(Id='owned-id', State=SimpleNamespace(Pid=123))

            containers = {'broker': {'name': 'broker'}, 'unrelated': {'name': 'x'},
                          'backend': backend, 'postgres': postgres}
            with patch.object(resources.os, 'sysconf', return_value=250):
                result = resources.read_linux_resources(OwnedRegistry(), containers,
                                                        proc_root=proc, cgroup_root=cg, sys_root=sysroot)
            self.assertEqual(calls, [backend, postgres])
            self.assertEqual([item['kind'] for item in result['containers']], ['backend', 'postgres'])
            self.assertEqual(result['containers'][0]['io']['value'][0]['read_bytes'], 10)
            self.assertEqual(result['host']['cpu']['value']['iowait_seconds'], 0.02)
            partial_cpu = resources._host_cpu('cpu 1 2 3 4 5 6 7\n')
            self.assertIsNone(partial_cpu['steal_seconds'])
            self.assertIsNone(partial_cpu['guest_seconds'])
            self.assertIn('sample_start', result)
            self.assertIn('sample_end', result)
            self.assertIn('utc', result['sample_start'])
            self.assertIn('monotonic_seconds', result['sample_end'])
            write(cg, 'docker/abc/io.stat', '8:0 rbytes=1 rbytes=2\n')
            malformed = resources.read_linux_resources(OwnedRegistry(), {'backend': backend},
                                                        proc_root=proc, cgroup_root=cg, sys_root=sysroot)
            self.assertEqual(malformed['containers'][0]['io'],
                             {'available': False, 'reason': 'malformed'})

    def test_diskstats_skips_many_loop_devices_before_backing_disk(self):
        with tempfile.TemporaryDirectory(dir='/var/tmp/opencode') as tmp:
            proc, cg, sysroot = self.fixture(Path(tmp))
            lines = []
            for index in range(20):
                (sysroot / 'dev/block' / f'7:{index}').mkdir(parents=True, exist_ok=True)
                lines.append(f'7 {index} loop{index} 1 0 2 3 4 0 5 6 1 8 9')
            (sysroot / 'dev/block/8:0').mkdir(parents=True, exist_ok=True)
            lines.append('8 0 sda 10 1 20 30 40 2 50 60 7 80 90')
            write(proc, 'diskstats', '\n'.join(lines) + '\n')
            result = resources.read_linux_resources(Registry({}), [], proc_root=proc,
                                                    cgroup_root=cg, sys_root=sysroot)
            disk = result['host']['diskstats']['value']
            self.assertEqual([item['device'] for item in disk], ['sda'])


if __name__ == '__main__':
    unittest.main()
