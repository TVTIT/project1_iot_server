import json
import hashlib
import sqlite3
import tempfile
import unittest
import uuid
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import paper_p4_support as p4
import native_compose_smoke as native
import stage3_uplink_runner as uplink


class PaperP4SupportTests(unittest.TestCase):
    def test_namespace_denial_uses_new_broker_audit_record_not_client_exit(self):
        topic = 'gateways/gw_b/telemetry/sensor_b'
        before = 'Client connected\n'
        after = before + f'Denied PUBLISH from gw_a (d0, q1, r0, m1, "{topic}")\n'
        self.assertTrue(p4.broker_denial_observed(before, after, topic))
        self.assertFalse(p4.broker_denial_observed(before, before, topic))
        self.assertFalse(p4.broker_denial_observed(
            before, before + 'Denied PUBLISH from gw_a ("gateways/gw_c/telemetry/sensor_c")\n', topic))

    def test_subscription_log_messages_accepts_pty_subscription_tuple(self):
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / 'observer.log'
            log.write_bytes(
                b"Client p4-observer received PUBLISH (d0, q1, r0, m1, 'gateways/gw_a/acks/id', ... )\n"
                b'{"protocol_version":1,"message_type":"telemetry_ack",'
                b'"message_id":"id","status":"committed",'
                b'"committed_at":"2026-10-25T00:00:00Z"}\n')
            # The PTY-based observer appends its master FD to the legacy tuple.
            subscription = (object(), object(), log, 17)
            messages = native.paper_p1_subscription_log_messages(subscription)
            self.assertEqual(len(messages), 1)
            self.assertEqual(messages[0][0], 'gateways/gw_a/acks/id')
            self.assertIn(b'"status":"committed"', messages[0][1])

    def test_workload_configuration_is_finite_comparable_and_bounded(self):
        config = p4.workload_config()
        self.assertEqual(config['a_batches'], 35)
        self.assertEqual(config['b_steady_batches'], 3)
        self.assertEqual(config['b_burst_batches'], 20)
        self.assertEqual(config['steady_samples_per_batch'], 100)
        self.assertEqual(config['burst_samples_per_batch'], 10)
        self.assertLessEqual(config['drain_deadline_seconds'], 90)
        self.assertEqual(config['offered_unique_messages'], 59)
        self.assertEqual(config['offered_unique_samples'], 4010)
        self.assertEqual(config['offered_unique_messages'], sum(
            config[key] for key in ('a_batches', 'b_steady_batches', 'b_burst_batches', 'b_drain_batches')))
        self.assertEqual(config['offered_unique_samples'],
            config['a_batches'] * config['steady_samples_per_batch']
            + config['b_steady_batches'] * config['steady_samples_per_batch']
            + (config['b_burst_batches'] + config['b_drain_batches']) * config['burst_samples_per_batch'])

    def test_effective_profile_checks_fixed_resource_limits_and_mode(self):
        class Fixture:
            paper_p4_mode = 'optimized'
            values = {'MQTT_QUEUE_CAPACITY': '256', 'MQTT_MAX_PAYLOAD_BYTES': '65536',
                      'MQTT_WORKER_COUNT': '4', 'DATABASE_MAX_CONNS': '10'}

        environment = {**p4.profile('optimized'), **Fixture.values,
                       'STAGE5_ENABLED': 'false', 'METRICS_ENABLED': 'true'}
        effective = {'services': {'backend': {'environment': environment}}}
        self.assertTrue(p4.validate_effective_profile(Fixture(), effective))
        self.assertTrue(p4.validate_effective_environment(Fixture(), environment))
        for key, value in [('MQTT_QUEUE_CAPACITY', '257'), ('STAGE5_ENABLED', 'true'),
                           ('MQTT_SHAPER_ENABLED', 'false')]:
            with self.subTest(key=key):
                tampered = {'services': {'backend': {'environment': dict(environment, **{key: value})}}}
                with self.assertRaisesRegex(ValueError, '^paper_p4_profile_rejected$'):
                    p4.validate_effective_profile(Fixture(), tampered)
                with self.assertRaisesRegex(ValueError, '^paper_p4_profile_rejected$'):
                    p4.validate_effective_environment(Fixture(), tampered['services']['backend']['environment'])

    def test_microbatch_deployment_mapping_is_opt_in_and_keeps_control_defaults(self):
        root = Path(__file__).resolve().parents[2]
        env_example = (root / '.env.example').read_text()
        compose = (root / 'docker-compose.yml').read_text()
        expected = {
            'MQTT_MICROBATCH_ENABLED': 'false',
            'MQTT_MICROBATCH_MAX_MESSAGES': '32',
            'MQTT_MICROBATCH_MAX_ROWS': '500',
            'MQTT_MICROBATCH_MAX_PAYLOAD_BYTES': '4194304',
            'MQTT_MICROBATCH_MAX_WAIT': '1s',
        }
        for key, default in expected.items():
            with self.subTest(key=key):
                self.assertIn(f'{key}={default}', env_example)
                self.assertIn(f'{key}: ${{{key}:-{default}}}', compose)
        self.assertIn('STAGE5_ENABLED=false', env_example)
        self.assertIn('STAGE5_ENABLED: ${STAGE5_ENABLED:-false}', compose)
        self.assertIn('MQTT_SHAPER_ENABLED=false', env_example)
        self.assertIn('MQTT_SHAPER_ENABLED: ${MQTT_SHAPER_ENABLED:-false}', compose)

    def test_broker_config_limits_are_pinned_and_leave_packet_overhead(self):
        text = '\n'.join(f'{name} {value}' for name, value in p4.BROKER_PROFILE.items())
        self.assertEqual(p4.validate_broker_profile(text), p4.BROKER_PROFILE)
        with self.assertRaisesRegex(ValueError, '^paper_p4_broker_profile_rejected$'):
            p4.validate_broker_profile(text.replace('max_packet_size 2097152', 'max_packet_size 1048576'))

    def test_accounting_requires_observed_values_but_allows_explicit_unavailable(self):
        p4.validate_accounting({'offered_unique': 58, 'backend_attempts': 58,
                                'committed_unique': 58, 'duplicate': 0, 'rejected': 0,
                                'pending_rows': 0, 'pending_bytes': 0,
                                'wire_bytes': None})
        with self.assertRaisesRegex(ValueError, '^paper_p4_accounting_rejected$'):
            p4.validate_accounting({'offered_unique': 58, 'committed_unique': 58})
        with self.assertRaisesRegex(ValueError, '^paper_p4_accounting_rejected$'):
            p4.validate_accounting({'offered_unique': 58, 'backend_attempts': 57,
                                    'committed_unique': 58, 'duplicate': 0, 'rejected': 0,
                                    'pending_rows': 0, 'pending_bytes': 0, 'wire_bytes': None})

    def test_acceptance_is_vetoed_on_failed_check_and_artifact_redacts_secrets(self):
        checks = {'shaper_rejected_burst': {'status': 'FAIL'}, 'drained': {'status': 'PASS'}}
        self.assertFalse(p4.acceptance_ready(checks))
        required = {'a_progress_during_b_burst', 'a_application_ack_during_b_burst',
                    'a_container_active_during_b_burst',
                    'shaper_profile_response', 'wrong_namespace_denied',
                    'own_namespace_payload_captured',
                    'unique_ledgers_and_raw_samples', 'temporal_per_message',
                    'real_gateway_summaries_and_empty_outboxes', 'rs256_https_and_namespace_control',
                    'baseline_burst_state_recorded', 'duplicate_replay_deduplicated'}
        self.assertTrue(p4.acceptance_ready({name: {'status': 'PASS'} for name in required}))
        self.assertFalse(p4.acceptance_ready({name: {'status': 'PASS'} for name in required - {'wrong_namespace_denied'}}))
        safe = p4.safe_artifact({'password': 'do-not-write', 'payload': 'private',
                                 'counts': {'committed': 58}, 'wire_bytes': None})
        rendered = json.dumps(safe)
        self.assertNotIn('do-not-write', rendered)
        self.assertNotIn('private', rendered)
        self.assertEqual(safe['counts']['committed'], 58)
        self.assertIsNone(safe['wire_bytes'])

    def test_run_fake_smoke_executes_full_optimized_path(self):
        class FakeRegistry:
            def __init__(self):
                self.items = []
                self.volumes = set()
                self.gateway_running = {}
                self.fixture = None
                self.metrics = {key: 0 for key in native.P1_COUNTERS + native.P1_GAUGES}
                self.metric_calls = 0
                self.a_messages = 0
                self.b_messages = 0
                self.burst_pending_id = str(uuid.uuid4())
                self.duplicate_replay = False
                self.broker_denial_log = ''
                self.payload = json.dumps({'message_id': str(uuid.uuid4()), 'samples': []},
                                          separators=(',', ':')).encode()

            def intent(self, kind, name, image, labels):
                return {'kind': kind, 'name': name, 'labels': labels}

            def allocating(self, item):
                if item['kind'] == 'volume':
                    self.volumes.add(item['name'])
                self.items.append(item)

            def resolve(self, item):
                if item.get('kind') == 'volume':
                    return {'Name': item['name']} if item['name'] in self.volumes else None
                if item.get('kind') == 'container':
                    running = self.gateway_running.get(item['name'], False)
                    return {'Id': item['name'], 'Name': item['name'], 'State': {'Running': running}}
                return None

            def command(self, args, input=None):
                if args[:2] == ['volume', 'create']:
                    self.volumes.add(args[-1])
                    return SimpleNamespace(returncode=0, stdout=b'', stderr=b'')
                if args and args[0] == 'cp':
                    target = Path(args[-1]) / 'outbox.db'
                    with sqlite3.connect(target) as db:
                        db.execute('CREATE TABLE outbox(id INTEGER, message_id TEXT, payload BLOB, status TEXT)')
                        db.execute('INSERT INTO outbox VALUES(1,?,?,?)',
                                   (self.burst_pending_id, self.payload, 'pending'))
                    return SimpleNamespace(returncode=0, stdout=b'', stderr=b'')
                if args and args[0] == 'stats':
                    rows = []
                    for container_id in args[4:]:
                        name = next((item['name'] for item in self.items if item['name'] == container_id), container_id)
                        rows.append(json.dumps({'Name': name, 'CPUPerc': '1.0%',
                                                'MemUsage': '2MiB / 4GiB', 'MemPerc': '1%'}))
                    return SimpleNamespace(returncode=0, stdout=('\n'.join(rows)).encode(), stderr=b'')
                if args[:2] == ['exec', '-i']:
                    body = input.decode(errors='ignore')
                    if 'gateways/gw_p4_b/' in body and body.startswith('gw_p4_b\n'):
                        self.duplicate_replay = True
                        self.metrics['ACKPublished'] += 1
                        return SimpleNamespace(returncode=0, stdout=b'', stderr=b'')
                    if body.startswith('gw_p4_a\n') and 'gateways/gw_p4_b/telemetry/sensor_b\n' in body:
                        self.broker_denial_log += (
                            'Denied PUBLISH from gw_p4_a '
                            '("gateways/gw_p4_b/telemetry/sensor_b")\n')
                        return SimpleNamespace(returncode=0, stdout=b'', stderr=b'')
                    return SimpleNamespace(returncode=1, stdout=b'', stderr=b'ACL denied')
                return SimpleNamespace(returncode=0, stdout=b'', stderr=b'')

        class Fixture:
            paper_p4_mode = 'optimized'
            project = 'paper-p4-fake'
            values = {'MQTT_USERNAME': 'backend', 'MQTT_PASSWORD': 'private'}

        class HTTPS:
            pass

        class State:
            def checkpoint(self):
                pass

        class Smoke:
            HTTPSEndpoint = HTTPS

            def __init__(self, root, registry):
                self.fixture = Fixture()
                self.fixture.private = root / 'private'
                self.fixture.artifacts = root / 'artifacts'
                self.fixture.private.mkdir()
                self.fixture.artifacts.mkdir()
                (self.fixture.private / 'certs').mkdir()
                (self.fixture.private / 'mosquitto.conf').write_text('\n'.join(
                    f'{key} {value}' for key, value in p4.BROKER_PROFILE.items()))
                self.registry = registry
                self.registry.fixture = self.fixture
                self.a_id = 'a2'
                self.specs = [
                    SimpleNamespace(gateway_id='gw_p4_a', sensor_id='sensor_a', sensor_unit='degC',
                                    sensor_urn='urn:ngsi-ld:Sensor:gw_p4_a:sensor_a'),
                    SimpleNamespace(gateway_id='gw_p4_b', sensor_id='sensor_b', sensor_unit='degC',
                                    sensor_urn='urn:ngsi-ld:Sensor:gw_p4_b:sensor_b')]
                self.payload = registry.payload
                self.snapshots = []
                self.base_counts = {'a': {'messages': 0, 'samples': 0},
                                    'b': {'messages': 0, 'samples': 0},
                                    'temporal_a': 0, 'temporal_b': 0}
                self.active_subscribers = []

            def prepare_paper_workload(self, fixture, registry, endpoint, containers, images, state):
                env = {**p4.profile('optimized'), **p4.RESOURCE_PROFILE}
                effective = {'services': {'backend': {'environment': env}}}
                resources = {'backend': {'NanoCpus': 2000000000, 'Memory': 4294967296}}
                return ('pg', {'cpus': 2, 'memory_bytes': 4294967296}, effective, resources,
                        200, {'admin': 'admin-jwt', 'a': 'a-jwt', 'b': 'b-jwt'}, self.specs,
                        ['password-a', 'password-b'], '/sensor')

            def paper_p4_support(self): return p4
            def paper_p2_support(self): return SimpleNamespace(decode_token=lambda token: ({'alg': 'RS256'}, {'sub': token}))
            def paper_p0_git(self, fixture, *args):
                return {'rev-parse': 'rev', 'branch': 'branch', 'status': ''}.get(args[0], '')
            def fixture_services(self, fixture): return ['backend']
            def private_write(self, path, data): Path(path).write_text(data)
            def durable_json(self, path, data): Path(path).write_text(json.dumps(data))
            def paper_p0_stats(self, registry, containers, phase):
                return {'phase': phase, 'started_at': 't0', 'finished_at': 't1',
                        'services': {'backend': {'CPUPerc': '1.0%', 'MemUsage': '2MiB / 4GiB', 'MemPerc': '1%'}}}
            def paper_p1_resources_csv(self, path, snapshots, limits):
                native.paper_p1_resources_csv(path, snapshots, limits)
            def paper_p1_metrics_csv(self, path, samples):
                native.paper_p1_metrics_csv(path, samples)
            def paper_p1_subscriber(self, *args, **kwargs):
                sub = object()
                self.active_subscribers.append(sub)
                return sub
            def paper_p1_capture_subscription(self, subscription, topic, timeout):
                return self.payload
            def paper_p1_stop_subscription(self, subscription): pass
            def paper_p1_subscription_log_messages(self, subscription):
                ack = json.dumps({'protocol_version': 1, 'message_type': 'telemetry_ack',
                    'message_id': self.a_id, 'status': 'committed',
                    'committed_at': '2026-10-25T00:00:00Z'}, separators=(',', ':')).encode()
                return [('gateways/gw_p4_a/acks/' + self.a_id, ack)]
            def http_request(self, endpoint, method, path, token=None):
                self.registry.metric_calls += 1
                if self.registry.a_messages >= 2:
                    self.registry.metrics.update(Received=60, ReceivedBytes=60000, Accepted=59,
                        Committed=59, SamplesNew=4010, ACKPublished=max(59, self.registry.metrics['ACKPublished']),
                        DuplicateCommitted=int(self.registry.duplicate_replay),
                        ShaperRateLimited=5, ShaperPassed=5)
                snap = {'process_instance': 'backend-process', 'elapsed_seconds': self.registry.metric_calls,
                        'observed_at': f'2026-10-25T00:00:{self.registry.metric_calls % 60:02d}Z',
                        'ingress': dict(self.registry.metrics)}
                self.snapshots.append(snap)
                return 200, {}, snap
            def db_psql(self, registry, pg, query, variables=None):
                variables = variables or {}
                if "'temporal_a'" in query:
                    a, b = registry.a_messages, registry.b_messages
                    return json.dumps({'a': {'messages': a, 'samples': a * 100},
                        'b': {'messages': b, 'samples': 300 if b == 3 else b * 100 if b < 3 else 510},
                        'temporal_a': a, 'temporal_b': b})
                if "'ledger', md5" in query:
                    return json.dumps({'ledger': 'same', 'raw': 'same', 'temporal': 'same', 'state': 'same'})
                if 'SELECT payload_hash FROM processed_messages' in query:
                    return hashlib.sha256(self.payload).hexdigest()
                if "'payload_hash',payload_hash" in query:
                    pending_id = registry.burst_pending_id
                    return json.dumps([{'message_id': pending_id,
                                        'payload_hash': hashlib.sha256(registry.payload).hexdigest()}])
                if "json_agg(message_id::text" in query:
                    ids = [f'a{i}' for i in range(1, registry.a_messages + 1)] if variables.get('gw') == 'gw_p4_a' else []
                    return json.dumps(ids)
                if 'SELECT count(*) FROM processed_messages WHERE gateway_id' in query:
                    return '1'
                if "'sample_count',sample_count" in query:
                    return json.dumps([{'gateway_id': 'gw_p4_a', 'message_id': f'a{i}', 'sample_count': 100}
                                       for i in range(35)] +
                                      [{'gateway_id': 'gw_p4_b', 'message_id': f'b{i}', 'sample_count': 10}
                                       for i in range(24)])
                if 'json_agg(json_build_array(sequence_num,value)' in query:
                    return '[]'
                if 'SELECT count(*) FROM processed_messages p' in query:
                    return '0'
                raise AssertionError('unhandled fake SQL query')

            def paper_p0_gateway_cli(self, fixture, registry, image, volume, env_file, ca_file,
                                    tag, args, state, during=None, tick=None, timeout=45,
                                    allow_incomplete=False):
                name = fixture.project + '-gateway-' + tag
                item = {'kind': 'container', 'name': name}
                registry.items.append(item)
                registry.gateway_running[name] = True
                if tag == 'p4-a-steady':
                    registry.a_messages = 1
                    if during: during()
                    registry.a_messages = 35
                    registry.b_messages = 24
                    registry.metrics.update(Received=59, ReceivedBytes=59000, Accepted=59,
                                            Committed=59, SamplesNew=4010, ACKPublished=59)
                    registry.gateway_running[name] = False
                    return ('run summary: enqueued=35 publish_attempts=35 app_acknowledged=35 '
                            'pending_rows=0 pending_bytes=0 quarantined_rows=0 quarantined_bytes=0 completed=true\n')
                if tag == 'p4-b-steady':
                    registry.b_messages = 3
                    registry.gateway_running[name] = False
                    return ('run summary: enqueued=3 publish_attempts=3 app_acknowledged=3 '
                            'pending_rows=0 pending_bytes=0 quarantined_rows=0 quarantined_bytes=0 completed=true\n')
                if tag == 'p4-b-burst':
                    if tick:
                        registry.a_messages = 2
                        registry.metrics['ShaperRateLimited'] = 5
                        registry.metrics['ShaperPassed'] = 5
                        tick()
                    registry.b_messages = 23
                    registry.gateway_running[name] = False
                    return ('run summary: enqueued=20 publish_attempts=25 app_acknowledged=10 '
                            'pending_rows=1 pending_bytes=100 quarantined_rows=0 quarantined_bytes=0 completed=false\n')
                if tag == 'p4-b-drain':
                    registry.b_messages = 24
                    registry.gateway_running[name] = False
                    return ('run summary: enqueued=1 publish_attempts=2 app_acknowledged=2 '
                            'pending_rows=0 pending_bytes=0 quarantined_rows=0 quarantined_bytes=0 completed=true\n')
                if args == ['count']:
                    registry.gateway_running[name] = False
                    return 'pending_rows=0 pending_bytes=0 quarantined_rows=0 quarantined_bytes=0\n'
                raise AssertionError(tag)

        with tempfile.TemporaryDirectory() as directory:
            registry = FakeRegistry()
            smoke = Smoke(Path(directory), registry)
            endpoint = HTTPS()
            containers = {'mosquitto': {'receipt': {'id': 'broker-id'}}}
            images = {'gateway': 'gateway-image', 'backend': 'backend-image'}
            with patch.object(uplink, 'container_logs',
                              side_effect=['', 'Denied PUBLISH from gw_p4_a '
                                           '("gateways/gw_p4_b/telemetry/sensor_b")\n']):
                result = p4.run(smoke, smoke.fixture, endpoint, registry, containers, images, State())
            self.assertTrue(result['full_p4_accepted'])
            self.assertEqual(result['workload']['offered_unique_samples'], 4010)
            self.assertEqual(result['checks']['duplicate_replay_deduplicated']['status'], 'PASS')
            self.assertEqual(result['checks']['a_application_ack_during_b_burst']['status'], 'PASS')


if __name__ == '__main__':
    unittest.main()
