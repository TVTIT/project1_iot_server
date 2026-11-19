import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

import paper_p8_e4_durable_client as durable


class DurableClientTests(unittest.TestCase):
    def test_pending_readback_is_readonly_and_exports_identity_hash(self):
        with tempfile.TemporaryDirectory(dir='/var/tmp/opencode') as tmp:
            root = Path(tmp); root.chmod(0o700)
            dbpath = root / 'outbox.sqlite'
            header = {'protocol_version': 1, 'message_id': 'id-1',
                'message_type': 'telemetry_batch', 'gateway_id': 'gw1',
                'sensor_id': 'p8-e4-durable', 'boot_id': 'boot1',
                'first_sequence': 7, 'sample_count': 100, 'measured_at': '2026-01-01T00:00:00Z',
                'sample_interval_us': 10000, 'samples': [{'value': 1}] * 100}
            payload = json.dumps(header, separators=(',', ':')).encode()
            conn = sqlite3.connect(dbpath)
            conn.execute('CREATE TABLE outbox (message_id TEXT, boot_id TEXT, gateway_id TEXT, '
                'sensor_id TEXT, first_sequence INTEGER, sample_interval_us INTEGER, '
                'sample_count INTEGER, payload BLOB, byte_size INTEGER, status TEXT)')
            conn.execute('INSERT INTO outbox VALUES (?,?,?,?,?,?,?,?,?,?)',
                ('id-1','boot1','gw1','p8-e4-durable',7,10000,100,payload,len(payload),'pending'))
            conn.commit(); conn.close(); dbpath.chmod(0o600)
            before = dbpath.read_bytes()
            result = durable.readonly_pending_snapshot(dbpath, 'gw1', 'p8-e4-durable')
            self.assertEqual(result['payload_sha256'], hashlib.sha256(payload).hexdigest())
            self.assertEqual(result['sequence'], 7)
            self.assertEqual(dbpath.read_bytes(), before)

    def test_rejects_wrong_sensor_and_multiple_pending_rows(self):
        with tempfile.TemporaryDirectory(dir='/var/tmp/opencode') as tmp:
            path = Path(tmp) / 'outbox.sqlite'
            conn = sqlite3.connect(path)
            conn.execute('CREATE TABLE outbox (message_id TEXT, boot_id TEXT, gateway_id TEXT, '
                'sensor_id TEXT, first_sequence INTEGER, sample_interval_us INTEGER, '
                'sample_count INTEGER, payload BLOB, byte_size INTEGER, status TEXT)')
            conn.commit(); conn.close(); path.chmod(0o600)
            with self.assertRaisesRegex(ValueError, 'durable_pending_row_count'):
                durable.readonly_pending_snapshot(path, 'gw1', 'p8-e4-durable')

    def test_typed_input_fails_closed_on_unapproved_fields_and_existing_outbox(self):
        with tempfile.TemporaryDirectory(dir='/var/tmp/opencode') as tmp:
            root = Path(tmp); root.chmod(0o700)
            (root/'inputs/certs').mkdir(mode=0o700, parents=True)
            (root/'inputs/private/gateways').mkdir(mode=0o700, parents=True)
            (root/'outputs').mkdir(mode=0o700)
            ca, password = root / 'inputs/certs/mqtt-ca.pem', root / 'inputs/private/gateways/gw1.json'
            ca.write_text('ca'); password.write_text('pw')
            ca.chmod(0o600); password.chmod(0o600)
            password.write_text(json.dumps({'username':'gw1','password':'pw'})); password.chmod(0o600)
            data = {'schema_version': 1, 'experiment': 'E4', 'gateway_id': 'gw1',
                'sensor_id': 'p8-e4-durable', 'mqtt_endpoint': 'mqtts://127.0.0.1:8883',
                'mqtt_server_name': 'localhost', 'ca_file': str(ca), 'username': 'gw1',
                'password_file': str(password), 'outbox_path': str(root/'outputs/durable.sqlite'),
                'sample_interval_us': 10000, 'samples_per_batch': 100, 'max_rows': 64,
                'max_bytes': 1 << 20, 'shutdown_timeout_seconds': 10,
                'session_id':'s1', 'run_id':'r1'}
            binding={'session':{'session_id':'s1','run_ids':['r1']}}
            self.assertEqual(durable.validate_inputs(data, workspace=root, stage='fault', binding=binding)['sensor_id'], 'p8-e4-durable')
            with self.assertRaisesRegex(ValueError, 'durable_input_schema_rejected'):
                durable.validate_inputs({**data, 'admin_url': 'not-allowed'}, workspace=root, stage='fault', binding=binding)
            Path(data['outbox_path']).touch(mode=0o600)
            with self.assertRaisesRegex(ValueError, 'durable_outbox_must_be_new'):
                durable.validate_inputs(data, workspace=root, stage='fault', binding=binding)


if __name__ == '__main__':
    unittest.main()
