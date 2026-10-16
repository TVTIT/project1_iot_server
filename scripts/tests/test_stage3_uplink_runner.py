#!/usr/bin/env python3
"""Focused unit tests for the Task 3.6 first-slice runner (no docker required).

These cover the pure contract of ``stage3_uplink_runner``: scenario
strictness, the explicit NOT RUN declaration, two-Gateway fixture
parameterization/isolation, the per-Gateway SQL scoping, and the resource
cleanup contract. The real image fixture is exercised separately by
``scripts/test-stage3-uplink.sh``.
"""
import dataclasses
import hashlib
import json
import sqlite3
import struct
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts/tests'))

import stage3_uplink_runner as R  # noqa: E402


class ScenarioContractTests(unittest.TestCase):
    def test_normal_scenario_resolves(self):
        self.assertEqual(R.resolve_scenario('normal'), 'normal')

    def test_tls_credential_scenario_resolves(self):
        self.assertEqual(R.resolve_scenario('tls-credential'), 'tls-credential')

    def test_acl_control_scenario_resolves(self):
        self.assertEqual(R.resolve_scenario('acl-control'), 'acl-control')

    def test_ack_fault_scenario_resolves(self):
        self.assertEqual(R.resolve_scenario('ack-fault'), 'ack-fault')

    def test_db_timeout_scenario_resolves(self):
        self.assertEqual(R.resolve_scenario('db-timeout'), 'db-timeout')

    def test_queue_full_scenario_resolves(self):
        self.assertEqual(R.resolve_scenario('queue-full'), 'queue-full')

    def test_gateway_reconnect_scenario_resolves(self):
        self.assertEqual(R.resolve_scenario('gateway-reconnect'), 'gateway-reconnect')

    def test_process_restart_scenario_resolves(self):
        self.assertEqual(R.resolve_scenario('process-restart'), 'process-restart')

    def test_broker_controlled_restart_scenario_resolves(self):
        self.assertEqual(R.resolve_scenario('broker-controlled-restart'), 'broker-controlled-restart')

    def test_all_scenario_resolves(self):
        self.assertEqual(R.resolve_scenario('all'), 'all')

    def test_implemented_scenarios_are_the_nine_slices(self):
        self.assertEqual(set(R.IMPLEMENTED_SCENARIOS),
                         {'normal', 'tls-credential', 'acl-control', 'ack-fault', 'db-timeout', 'queue-full', 'gateway-reconnect', 'process-restart', 'broker-controlled-restart'})

    def test_unknown_scenario_fails_closed(self):
        with self.assertRaises(ValueError):
            R.resolve_scenario('denial')
        with self.assertRaises(ValueError):
            R.resolve_scenario('')
        with self.assertRaises(ValueError):
            R.resolve_scenario('ack-spoof')

    def test_notrun_is_empty_when_all_scenarios_implemented(self):
        lines = R.notrun_lines()
        self.assertEqual(len(lines), 0)
        self.assertEqual(len(R.REMAINING_GROUPS), 0)


class NegativeEvidenceTests(unittest.TestCase):
    """The 3.6.3 denial oracles must never PASS on elapsed time alone."""

    def test_gateway_tls_denial_requires_a_concrete_cert_denial(self):
        # A bounded run that only elapsed without a certificate denial is not PASS.
        self.assertFalse(R.gateway_tls_denial_ok(
            exit_code=1, ready_reached=False, ack_observed=False, cert_denial_observed=False))
        # A run that reached ready or produced an ACK is never a denial.
        self.assertFalse(R.gateway_tls_denial_ok(
            exit_code=1, ready_reached=True, ack_observed=False, cert_denial_observed=True))
        self.assertFalse(R.gateway_tls_denial_ok(
            exit_code=1, ready_reached=False, ack_observed=True, cert_denial_observed=True))
        # A clean exit is never a denial.
        self.assertFalse(R.gateway_tls_denial_ok(
            exit_code=0, ready_reached=False, ack_observed=False, cert_denial_observed=True))
        # All conditions together are a denial.
        self.assertTrue(R.gateway_tls_denial_ok(
            exit_code=1, ready_reached=False, ack_observed=False, cert_denial_observed=True))

    def test_backend_san_denial_requires_a_certificate_reason(self):
        # /readyz becoming ready can never be a denial.
        self.assertFalse(R.backend_san_denial_ok(ready_http_ok=True, cert_reason='hostname mismatch'))
        # A bounded probe that elapsed with no certificate reason is not PASS.
        self.assertFalse(R.backend_san_denial_ok(ready_http_ok=False, cert_reason=None))
        self.assertTrue(R.backend_san_denial_ok(ready_http_ok=False, cert_reason='hostname mismatch'))

    def test_credential_denial_requires_reject_and_untouched_pending(self):
        self.assertFalse(R.credential_denial_ok(
            connack_rejected=False, cli_exit_code=1, pending_before=1, pending_after=1))
        self.assertFalse(R.credential_denial_ok(
            connack_rejected=True, cli_exit_code=0, pending_before=1, pending_after=1))
        self.assertFalse(R.credential_denial_ok(
            connack_rejected=True, cli_exit_code=1, pending_before=1, pending_after=0))
        self.assertFalse(R.credential_denial_ok(
            connack_rejected=True, cli_exit_code=1, pending_before=0, pending_after=0))
        self.assertTrue(R.credential_denial_ok(
            connack_rejected=True, cli_exit_code=1, pending_before=1, pending_after=1))

    def test_classify_tls_error(self):
        self.assertEqual(R.classify_tls_error(None), 'ok')
        self.assertEqual(R.classify_tls_error('certificate verify failed: Hostname mismatch'), 'hostname')
        self.assertEqual(R.classify_tls_error('certificate verify failed: self-signed certificate'), 'unknown_authority')
        self.assertEqual(R.classify_tls_error('certificate verify failed: unable to get local issuer certificate'),
                         'unknown_authority')
        self.assertEqual(R.classify_tls_error('ssl error: timed out'), 'other')

    def test_extract_gateway_tls_denial_reason(self):
        self.assertEqual(
            R.extract_gateway_tls_denial_reason('time=2026-10-16 level=WARN msg="gatewaysim transport connect failed" event=connect_failed reason=tls_unknown_authority'),
            'tls_unknown_authority')
        self.assertEqual(
            R.extract_gateway_tls_denial_reason('event=connect_failed reason="tls_certificate_verification"'),
            'tls_certificate_verification')
        self.assertEqual(
            R.extract_gateway_tls_denial_reason('event=connect_failed reason=tls_hostname_mismatch'),
            'tls_hostname_mismatch')
        self.assertEqual(
            R.extract_gateway_tls_denial_reason('event=connect_failed reason=tls_certificate_invalid'),
            'tls_certificate_invalid')
        # Bounded rejection: generic timeout or other connection failure is not a TLS reason.
        self.assertIsNone(R.extract_gateway_tls_denial_reason('event=connect_failed reason=deadline_exceeded'))
        self.assertIsNone(R.extract_gateway_tls_denial_reason('event=connect_failed reason=connection_failed'))
        self.assertIsNone(R.extract_gateway_tls_denial_reason('event=connect_failed reason=authentication_rejected'))
        self.assertIsNone(R.extract_gateway_tls_denial_reason(''))


class AclDenialOracleTests(unittest.TestCase):
    """3.6.3 rows 4-5 and 7 oracles never PASS on PUBACK/timeout alone."""

    def test_cross_publish_denial_requires_broker_and_side_effect_evidence(self):
        base = dict(broker_denied=True, db_unchanged=True, message_id_absent=True,
                    no_app_ack=True, receiver_positive=True)
        self.assertTrue(R.acl_cross_publish_denial_ok(**base))
        # A PUBACK (or its absence) is not an input: dropping any single piece of
        # real evidence must fail the oracle.
        for key in ('broker_denied', 'db_unchanged', 'message_id_absent',
                    'no_app_ack', 'receiver_positive'):
            broken = dict(base)
            broken[key] = False
            self.assertFalse(R.acl_cross_publish_denial_ok(**broken), key)

    def test_cross_subscribe_denial_requires_exact_suback_and_positive_controls(self):
        self.assertTrue(R.acl_cross_subscribe_denial_ok(
            target_suback_code=0x80, own_positive_suback_code=0x00,
            receiver_ack_observed=True, cross_gateway_delivery_observed=False))
        # A granted target SUBACK is never a denial.
        self.assertFalse(R.acl_cross_subscribe_denial_ok(
            target_suback_code=0x00, own_positive_suback_code=0x00,
            receiver_ack_observed=True, cross_gateway_delivery_observed=False))
        # No own-topic positive control is not a PASS.
        self.assertFalse(R.acl_cross_subscribe_denial_ok(
            target_suback_code=0x80, own_positive_suback_code=0x80,
            receiver_ack_observed=True, cross_gateway_delivery_observed=False))
        # A missing receiver ACK (timeout alone) is not a PASS.
        self.assertFalse(R.acl_cross_subscribe_denial_ok(
            target_suback_code=0x80, own_positive_suback_code=0x00,
            receiver_ack_observed=False, cross_gateway_delivery_observed=False))
        # An observed cross-Gateway delivery is a real failure.
        self.assertFalse(R.acl_cross_subscribe_denial_ok(
            target_suback_code=0x80, own_positive_suback_code=0x00,
            receiver_ack_observed=True, cross_gateway_delivery_observed=True))

    def test_control_topic_denial_requires_readback_and_suback_denials(self):
        base = dict(broker_denied=True, gateway_probe_absent_before=True,
                    gateway_probe_absent_after=True, response_absent=True,
                    readback_detects_mutation=True, control_suback_code=0x80,
                    response_suback_code=0x80, manager_positive=True)
        self.assertTrue(R.acl_control_denial_ok(**base))
        for key in ('broker_denied', 'gateway_probe_absent_before', 'gateway_probe_absent_after',
                    'response_absent', 'readback_detects_mutation', 'manager_positive'):
            broken = dict(base)
            broken[key] = False
            self.assertFalse(R.acl_control_denial_ok(**broken), key)
        # A granted control or response SUBACK is never a denial.
        for key in ('control_suback_code', 'response_suback_code'):
            broken = dict(base)
            broken[key] = 0x00
            self.assertFalse(R.acl_control_denial_ok(**broken), key)


class AckFaultOracleTests(unittest.TestCase):
    """3.6.3 row 6 / 3.6.4 oracles never PASS on timeout/PUBACK alone."""

    def test_ack_block_effective_requires_behaviour_both_ways(self):
        self.assertTrue(R.ack_block_effective(allowed_delivered=True, blocked_delivered=False))
        # A readback/attachment without the behaviour is never enough.
        self.assertFalse(R.ack_block_effective(allowed_delivered=False, blocked_delivered=False))
        self.assertFalse(R.ack_block_effective(allowed_delivered=True, blocked_delivered=True))
        self.assertFalse(R.ack_block_effective(allowed_delivered=False, blocked_delivered=True))

    def test_lost_ack_resend_oracle(self):
        base = dict(committed=True, pending_while_blocked=True, resend_observed=True,
                    resend_bytes_match=True, db_unchanged_during=True, receive_restored=True,
                    pending_after=0, simulator_exit_code=0, cli_pending_rows=0)
        self.assertTrue(R.lost_ack_resend_ok(**base))
        for key in base:
            broken = dict(base)
            broken[key] = 1 if key in ('pending_after', 'simulator_exit_code', 'cli_pending_rows') else False
            self.assertFalse(R.lost_ack_resend_ok(**broken), key)

    def test_ack_spoof_denial_oracle(self):
        base = dict(broker_denied_own=True, broker_denied_target=True, no_spoof_delivery=True,
                    pending_bytes_stable=True, db_unchanged=True, receiver_positive=True)
        self.assertTrue(R.ack_spoof_denial_ok(**base))
        for key in base:
            broken = dict(base)
            broken[key] = False
            self.assertFalse(R.ack_spoof_denial_ok(**broken), key)


class AckFaultHelperTests(unittest.TestCase):
    """Focused checks for the ack-fault helpers (no docker required)."""

    class _FakeManager:
        """Records DynSec commands; a deleteRole failure exercises the swallow."""

        def __init__(self, client):
            self.calls = []
            self._client = client

        def request(self, command, **fields):
            self.calls.append((command, fields))
            if command == 'getClient':
                return {'client': self._client}
            if command == 'deleteRole':
                raise RuntimeError('deleteRole failure must be swallowed after detach')
            return {}

    def test_attach_and_detach_issue_a_reversible_dynsec_sequence(self):
        mgr = self._FakeManager({'roles': []})
        R.attach_ack_receive_block(mgr, 'gw_a', 'gateways/gw_a/acks/#')
        commands = [c for c, _ in mgr.calls]
        self.assertEqual(commands, ['createRole', 'addRoleACL', 'addClientRole'])
        acl = next(f for c, f in mgr.calls if c == 'addRoleACL')
        self.assertEqual(acl['acltype'], 'publishClientReceive')
        self.assertEqual(acl['topic'], 'gateways/gw_a/acks/#')
        self.assertFalse(acl['allow'])
        role = next(f for c, f in mgr.calls if c == 'addClientRole')
        self.assertEqual(role['priority'], R.ACK_BLOCK_ROLE_PRIORITY)
        # Detach removes the client role and swallows a deleteRole failure.
        mgr.calls.clear()
        R.detach_ack_receive_block(mgr, 'gw_a')
        self.assertEqual([c for c, _ in mgr.calls], ['removeClientRole', 'deleteRole'])

    def test_ack_block_attached_reads_the_role_list(self):
        attached = self._FakeManager({'roles': [{'rolename': 'gateway_gw_a_ack_block'}]})
        self.assertTrue(R.ack_block_attached(attached, 'gw_a'))
        absent = self._FakeManager({'roles': [{'rolename': 'gateway_gw_a_uplink'}]})
        self.assertFalse(R.ack_block_attached(absent, 'gw_a'))

    def test_build_commit_ack_is_the_exact_five_field_contract(self):
        mid = '0195e18c-9fc1-7a42-9064-69ea49e63bf2'
        raw = R.build_commit_ack(mid)
        obj = json.loads(raw.decode())
        self.assertEqual(set(obj), {'protocol_version', 'message_type', 'message_id',
                                    'status', 'committed_at'})
        self.assertEqual(obj['protocol_version'], 1)
        self.assertEqual(obj['message_type'], 'telemetry_ack')
        self.assertEqual(obj['message_id'], mid)
        self.assertEqual(obj['status'], 'committed')
        self.assertTrue(obj['committed_at'].endswith('Z'))
        # The shared strict v1 ACK contract must accept it.
        R.assert_ack_fields(raw, mid)

    def test_ack_block_role_name_is_stable_and_gateway_scoped(self):
        self.assertEqual(R.ack_block_role_name('gw_a'), 'gateway_gw_a_ack_block')
        self.assertNotEqual(R.ack_block_role_name('gw_a'), R.ack_block_role_name('gw_b'))

    def test_pending_fingerprints_are_byte_sensitive(self):
        rows = [{'message_id': 'm1', 'topic': 't/1', 'payload': b'abc', 'byte_size': 3}]
        fp = R.pending_fingerprints(rows)
        self.assertEqual(fp['m1'], ('t/1', hashlib.sha256(b'abc').hexdigest(), 3))
        changed = [{'message_id': 'm1', 'topic': 't/1', 'payload': b'abd', 'byte_size': 3}]
        self.assertNotEqual(R.pending_fingerprints(changed), fp)

    def test_read_sqlite_pending_reads_only_pending_rows_with_bytes(self):
        with tempfile.TemporaryDirectory() as d:
            db = Path(d) / 'outbox.db'
            con = sqlite3.connect(str(db))
            con.execute(
                "CREATE TABLE outbox (id INTEGER PRIMARY KEY AUTOINCREMENT, message_id TEXT NOT NULL, "
                "gateway_id TEXT, sensor_id TEXT, boot_id TEXT, topic TEXT, payload BLOB, byte_size INTEGER, "
                "first_sequence INTEGER, sample_count INTEGER, sample_interval_us INTEGER, status TEXT, "
                "reason TEXT, enqueued_at TEXT)")
            con.execute(
                "INSERT INTO outbox (message_id, gateway_id, sensor_id, boot_id, topic, payload, byte_size, "
                "first_sequence, sample_count, sample_interval_us, status, enqueued_at) VALUES "
                "('p1','gw_a','s','b','gateways/gw_a/telemetry/s',?,3,0,100,10000,'pending','t0')",
                (b'abc',))
            con.execute(
                "INSERT INTO outbox (message_id, gateway_id, sensor_id, boot_id, topic, payload, byte_size, "
                "first_sequence, sample_count, sample_interval_us, status, enqueued_at) VALUES "
                "('q1','gw_a','s','b','gateways/gw_a/telemetry/s',?,3,0,100,10000,'quarantined','t0')",
                (b'xyz',))
            con.commit()
            con.close()
            rows = R.read_sqlite_pending(db)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]['message_id'], 'p1')
            self.assertEqual(rows[0]['payload'], b'abc')
            self.assertEqual(rows[0]['status'], 'pending')
            # A read-only open must not have created a journal or mutated the file.
            self.assertFalse((Path(d) / 'outbox.db-journal').exists())


class ManagerReadbackTests(unittest.TestCase):
    """The DynSec readback must never silently parse to an empty name set."""

    class _FakeManager:
        def __init__(self, data):
            self._data = data

        def request(self, command):
            assert command == 'listClients', command
            return self._data

    def test_parses_bare_string_usernames(self):
        # Mosquitto 2.0.18 default listClients returns bare usernames.
        names = R.manager_client_names(
            self._FakeManager({'totalCount': 2, 'clients': ['bootstrap_manager', 'gw_a']}))
        self.assertEqual(names, {'bootstrap_manager', 'gw_a'})

    def test_parses_object_usernames_too(self):
        names = R.manager_client_names(self._FakeManager({'clients': [{'username': 'x'}, 'y']}))
        self.assertEqual(names, {'x', 'y'})

    def test_empty_and_malformed_data_yield_no_names(self):
        self.assertEqual(R.manager_client_names(self._FakeManager({'clients': []})), set())
        self.assertEqual(R.manager_client_names(self._FakeManager({})), set())
        self.assertEqual(R.manager_client_names(self._FakeManager(None)), set())


class DeniedPublishLogTests(unittest.TestCase):
    def test_only_a_new_denied_line_for_the_topic_counts(self):
        topic = 'gateways/gw_b/telemetry/sensor'
        before = '123: New client connected\n'
        after = before + f"124: Denied PUBLISH from poc_x (d0, q1, r0, m1, '{topic}', ... (9 bytes))\n"
        self.assertTrue(R.denied_publish_logged(before, after, topic))
        # A denied line for a different topic is not evidence.
        self.assertFalse(R.denied_publish_logged(
            before, before + "124: Denied PUBLISH from poc_x (d0, q1, r0, m1, 'other', ...)\n", topic))
        # A pre-existing identical line is not a new observation.
        self.assertFalse(R.denied_publish_logged(after, after, topic))
        # A plain SUBACK/PUBACK-only log is not a denial observation.
        self.assertFalse(R.denied_publish_logged(before, before + '124: Received PUBACK\n', topic))


class FixtureDenyLoggingTests(unittest.TestCase):
    """The debug-logging opt-in must touch only the disposable fixture copy."""

    def test_appends_debug_only_to_the_fixture_copy_and_is_idempotent(self):
        with tempfile.TemporaryDirectory() as d:
            broker = Path(d) / 'broker'
            broker.mkdir()
            conf = broker / 'mosquitto.conf'
            conf.write_text('log_dest stdout\nlog_type notice\n')
            R.enable_fixture_deny_logging(Path(d))
            text = conf.read_text()
            self.assertIn('log_type notice', text)  # existing level preserved
            self.assertIn('log_type debug', text)
            self.assertEqual(text.count('log_type debug'), 1)
            # Second call must be a no-op (no duplicated level, no re-write).
            R.enable_fixture_deny_logging(Path(d))
            self.assertEqual(conf.read_text().count('log_type debug'), 1)

    def test_never_rewrites_the_checked_in_repository_config(self):
        repo_conf = ROOT / 'config/mosquitto/mosquitto.conf'
        before = repo_conf.read_text()
        with tempfile.TemporaryDirectory() as d:
            broker = Path(d) / 'broker'
            broker.mkdir()
            (broker / 'mosquitto.conf').write_text(before)
            R.enable_fixture_deny_logging(Path(d))
        self.assertEqual(repo_conf.read_text(), before)
        self.assertNotIn('log_type debug', before)



class _FakeSocket:
    """Minimal socket double: replays queued bytes and records what was sent."""

    def __init__(self, incoming: bytes):
        self._buf = bytearray(incoming)
        self.sent = bytearray()

    def sendall(self, data):
        self.sent.extend(data)

    def recv(self, n):
        if not self._buf:
            raise TimeoutError
        chunk = bytes(self._buf[:n])
        del self._buf[:n]
        return chunk

    def settimeout(self, _timeout):
        pass

    def close(self):
        pass


class AclProbeFramingTests(unittest.TestCase):
    """Exercise the probe framing so a missing import/framing bug cannot pass."""

    def _probe(self, incoming: bytes):
        probe = object.__new__(R.AclProbe)
        probe.s = _FakeSocket(incoming)
        probe.pid = 0
        return probe

    def test_subscribe_code_reads_a_denied_suback(self):
        probe = self._probe(b'\x90\x03\x00\x01\x80')
        self.assertEqual(probe.subscribe_code('gateways/gw_b/acks/#'), 0x80)
        # The SUBSCRIBE frame was actually written (MQTT 3.1.1, QoS0 flag).
        self.assertTrue(probe.s.sent.startswith(b'\x82'))

    def test_subscribe_code_reads_a_granted_suback(self):
        probe = self._probe(b'\x90\x03\x00\x01\x00')
        self.assertEqual(probe.subscribe_code('gateways/gw_a/acks/#'), 0x00)

    def test_publish_qos1_reads_a_puback(self):
        probe = self._probe(b'\x40\x02\x00\x01')
        self.assertTrue(probe.publish_qos1('gateways/gw_b/telemetry/x', b'{}'))
        self.assertTrue(probe.s.sent.startswith(b'\x32'))

    def test_publish_qos1_returns_false_without_a_puback(self):
        probe = self._probe(b'')
        self.assertFalse(probe.publish_qos1('gateways/gw_b/telemetry/x', b'{}', timeout=0.05))

    def test_publish_frame_skips_a_pingresp_and_returns_the_publish(self):
        topic = 'gateways/gw_a/acks/00000000-0000-4000-8000-000000000000'
        payload = b'{"status":"committed"}'
        body = struct.pack('!H', len(topic)) + topic.encode() + payload
        publish = bytes([0x30, len(body)]) + body
        probe = self._probe(b'\xd0\x00' + publish)
        self.assertEqual(probe.publish_frame(0.5), (topic, payload))

    def test_publish_frame_returns_none_without_a_publish(self):
        probe = self._probe(b'')
        self.assertIsNone(probe.publish_frame(0.05))

    def test_probe_connects_with_keepalive_disabled(self):
        # A bounded probe can sit idle for a whole fault window; keepalive=0
        # stops the broker dropping it at 1.5x the default 5s CONNECT keepalive.
        captured = {}

        def fake_connect(port, ca, username, password, accepted=True, keepalive=5):
            captured.update(port=port, ca=ca, username=username, password=password, keepalive=keepalive)
            return _FakeSocket(b'')

        original = R.mqtt_connection
        R.mqtt_connection = fake_connect
        try:
            R.AclProbe(8883, Path('/tmp/ca.crt'), 'gw_a', 'secret')
        finally:
            R.mqtt_connection = original
        self.assertEqual(captured['keepalive'], 0)
        self.assertEqual(captured['username'], 'gw_a')


class ProbeBatchTests(unittest.TestCase):
    def test_synthetic_value_matches_the_simulator_wave(self):
        self.assertAlmostEqual(R.synthetic_value(0), 20.0)
        self.assertAlmostEqual(R.synthetic_value(99), 20.99)
        self.assertAlmostEqual(R.synthetic_value(1000), 30.0)
        self.assertAlmostEqual(R.synthetic_value(1001), 29.99)
        self.assertAlmostEqual(R.synthetic_value(1999), 20.01)

    def test_probe_measured_at_is_strictly_increasing_from_the_base(self):
        # Step 0 must equal the shipped constant so a first batch stays stable.
        self.assertEqual(R.probe_measured_at(0), R.PROBE_MEASURED_AT)
        # Successive steps must be strictly newer, or the reported_version
        # advance for a second committed batch would be absent.
        self.assertLess(R.probe_measured_at(0), R.probe_measured_at(1))
        self.assertLess(R.probe_measured_at(1), R.probe_measured_at(2))

    def test_probe_batch_is_valid_for_its_gateway(self):
        a, b = R.default_specs()
        payload = R.build_probe_batch(a, '00000000-0000-4000-8000-000000000010',
                                      '00000000-0000-4000-8000-000000000020', R.FIRST_SEQUENCE)
        self.assertTrue(R.probe_batch_is_valid(payload, a))
        # The same payload is not valid for another Gateway.
        self.assertFalse(R.probe_batch_is_valid(payload, b))

    def test_probe_batch_rejects_malformed_payloads(self):
        a, _ = R.default_specs()
        good = R.build_probe_batch(a, '00000000-0000-4000-8000-000000000010',
                                   '00000000-0000-4000-8000-000000000020', R.FIRST_SEQUENCE)
        obj = json.loads(good.decode())
        obj['sample_count'] = R.SAMPLE_COUNT + 1  # samples no longer match count
        self.assertFalse(R.probe_batch_is_valid(json.dumps(obj).encode(), a))
        obj = json.loads(good.decode())
        obj['message_id'] = 'not-a-uuid'
        self.assertFalse(R.probe_batch_is_valid(json.dumps(obj).encode(), a))


class FixtureParameterizationTests(unittest.TestCase):
    def test_two_gateways_share_one_sensor_with_distinct_sentinels(self):
        specs = R.default_specs()
        self.assertEqual(len(specs), 2)
        R.validate_specs(specs)  # shared sensor id is intentional and allowed
        self.assertEqual(len({s.sensor_id for s in specs}), 1)
        self.assertEqual(len({s.gateway_id for s in specs}), 2)
        # URN isolation: same sensor id, distinct per-Gateway URNs and topics.
        self.assertEqual(len({s.sensor_urn for s in specs}), 2)
        self.assertEqual(len({s.telemetry_topic for s in specs}), 2)
        # Distinct desired/Gateway sentinels must be preserved.
        self.assertNotEqual(specs[0].gateway_desired, specs[1].gateway_desired)
        self.assertNotEqual(specs[0].gateway_reported, specs[1].gateway_reported)
        self.assertNotEqual(specs[0].sensor_desired, specs[1].sensor_desired)

    def test_duplicate_gateway_id_is_rejected(self):
        specs = R.default_specs()
        with self.assertRaises(ValueError):
            R.validate_specs([specs[0], dataclasses.replace(specs[0])])

    def test_single_gateway_is_rejected(self):
        specs = R.default_specs()
        with self.assertRaises(ValueError):
            R.validate_specs([specs[0]])


class SqlScopingTests(unittest.TestCase):
    def test_assert_sql_is_scoped_to_one_gateway(self):
        a, b = R.default_specs()
        env = R.simulator_envelope()
        sql = R.build_gateway_assert_sql(a, env, 'GATEWAY_A_PASS')
        self.assertIn(a.gateway_id, sql)
        self.assertIn(a.sensor_urn, sql)
        self.assertIn(a.telemetry_topic, sql)
        self.assertNotIn(b.gateway_id, sql)
        self.assertNotIn(b.sensor_urn, sql)
        # The ledger/raw/temporal filters must be gateway-scoped.
        self.assertIn(f"gateway_id='{a.gateway_id}' AND sensor_id='{a.sensor_id}'", sql)

    def test_assert_sql_encodes_the_expected_envelope(self):
        a, _ = R.default_specs()
        env = R.simulator_envelope()
        sql = R.build_gateway_assert_sql(a, env, 'M')
        self.assertIn(f"<> {env.sample_count}", sql)
        self.assertIn('sample_interval_us', sql)
        self.assertIn(str(R.SIM_BASE), sql)
        self.assertIn(str(R.SIM_PERIOD), sql)


class CleanupContractTests(unittest.TestCase):
    def test_still_present_reports_only_missing_resources(self):
        registry = R.ResourceRegistry()
        registry.register('backend container', 'container', 'cid-a')
        registry.register('network', 'network', 'net-a')

        present = {'net-a'}
        remaining = registry.still_present(lambda kind, ident: ident not in present)
        self.assertEqual(remaining, ['network'])

    def test_still_present_empty_when_all_absent(self):
        registry = R.ResourceRegistry()
        registry.register('workdir', 'workdir', '/tmp/does-not-matter')
        self.assertEqual(registry.still_present(lambda kind, ident: True), [])

    def test_unknown_kind_is_rejected(self):
        with self.assertRaises(ValueError):
            R.absent('mystery', 'x')


class DbTimeoutHelperTests(unittest.TestCase):
    def test_parse_ingress_summary_text_slog(self):
        log_sample = (
            "2026/10/16 03:57:35 INFO backend listening port=8080 environment=smoke\n"
            "2026/10/16 03:57:40 INFO telemetry ingress summary accepted=2 queue_full=0 "
            "processed=2 validation_rejected=0 persist_failures=1 commit_unconfirmed=0 "
            "identity_mismatch=0 ack_build_failures=0 committed=1 duplicate_committed=0 "
            "samples_new=100 ack_published=1 ack_publish_failures=0 queue_depth=0 queue_capacity=256\n"
            "2026/10/16 03:57:40 INFO backend shutdown complete\n"
        )
        res = R.parse_ingress_summary(log_sample)
        self.assertEqual(res['persist_failures'], 1)
        self.assertEqual(res['committed'], 1)
        self.assertEqual(res['queue_full'], 0)
        self.assertEqual(res['accepted'], 2)
        self.assertEqual(res['samples_new'], 100)
        self.assertEqual(res['queue_capacity'], 256)

    def test_parse_ingress_summary_json(self):
        log_sample = (
            '{"time":"2026-10-16T03:57:40Z","level":"INFO","msg":"telemetry ingress summary",'
            '"accepted":5,"queue_full":0,"persist_failures":2,"committed":3}\n'
        )
        res = R.parse_ingress_summary(log_sample)
        self.assertEqual(res['persist_failures'], 2)
        self.assertEqual(res['committed'], 3)
        self.assertEqual(res['accepted'], 5)

    def test_parse_ingress_summary_absent(self):
        self.assertEqual(R.parse_ingress_summary("no summary here"), {})

    def test_parse_ingress_summary_with_queue_high_water(self):
        log_sample = (
            "2026/10/16 03:57:40 INFO telemetry ingress summary accepted=10 queue_full=1 "
            "queue_depth=0 queue_capacity=16 queue_high_water=16\n"
        )
        res = R.parse_ingress_summary(log_sample)
        self.assertEqual(res['queue_capacity'], 16)
        self.assertEqual(res['queue_high_water'], 16)
        self.assertEqual(res['queue_full'], 1)
        self.assertEqual(res['accepted'], 10)

    def test_sql_scalar_int_parses_table_output(self):
        class FakeFixture:
            def sql(self, data):
                return 0, b"\n count \n-------\n     42\n(1 row)\n"

        self.assertEqual(R.sql_scalar_int(FakeFixture(), "SELECT count(*) FROM table"), 42)

    def test_sql_scalar_int_fails_on_no_digits(self):
        class FakeFixture:
            def sql(self, data):
                return 0, b"\n count \n-------\n(0 rows)\n"

        with self.assertRaises(RuntimeError):
            R.sql_scalar_int(FakeFixture(), "SELECT count(*) FROM table")

    def test_wait_worker_blocked_on_lock_detects_lock_wait(self):
        class FakeFixture:
            def sql(self, data):
                return 0, b"\n  pid  | wait_event_type | wait_event | query \n-------+-----------------+------------+-------\n 99999 | Lock            | tuple      | SELECT 1\n(1 row)\n"

        res = R.wait_worker_blocked_on_lock(FakeFixture(), holder_pid=123, timeout=1.0)
        self.assertEqual(res['pid'], '99999')
        self.assertEqual(res['wait_event_type'], 'Lock')
        self.assertEqual(res['wait_event'], 'tuple')

    def test_assert_uncommitted_state_rejects_nonzero_ledger(self):
        class FakeFixture:
            def sql(self, data):
                query = data.decode('utf-8')
                if 'processed_messages' in query:
                    return 0, b"\n count \n-------\n     1\n(1 row)\n"
                return 0, b"\n count \n-------\n     0\n(1 row)\n"

        spec = R.default_specs()[0]
        with self.assertRaises(RuntimeError) as ctx:
            R.assert_uncommitted_state(FakeFixture(), spec)
        self.assertIn('expected 0 ledger rows', str(ctx.exception))

    def test_sql_row_lock_hold_release_is_idempotent(self):
        lock = R.SqlRowLockHold('dummy_cid', 'gw_a', 'sensor_a')
        self.assertFalse(lock.released)
        lock.release()
        self.assertTrue(lock.released)
        lock.release()


class QueueFullHelperTests(unittest.TestCase):
    def test_require_queue_full_summary_success(self):
        summary = {'queue_full': 2, 'accepted': 3, 'committed': 0}
        self.assertEqual(R.require_queue_full_summary(summary), 2)

    def test_require_queue_full_summary_zero_fails(self):
        summary = {'queue_full': 0, 'accepted': 3}
        with self.assertRaises(RuntimeError) as ctx:
            R.require_queue_full_summary(summary)
        self.assertIn('expected queue_full >= 1', str(ctx.exception))

    def test_require_queue_full_summary_missing_or_empty_fails(self):
        with self.assertRaises(RuntimeError):
            R.require_queue_full_summary({})
        with self.assertRaises(RuntimeError):
            R.require_queue_full_summary({'accepted': 3})

    def test_queue_full_profile_bounds(self):
        # Initial deliveries must exceed one worker plus one queued slot
        self.assertGreater(R.QUEUE_FULL_BATCHES, 2)
        # Sample rate must be a positive integer dividing 1,000,000
        self.assertEqual(1_000_000 % R.QUEUE_FULL_SAMPLE_RATE_HZ, 0)
        self.assertEqual(R.QUEUE_FULL_SAMPLE_INTERVAL_US, 1_000_000 // R.QUEUE_FULL_SAMPLE_RATE_HZ)
        env = R.gateway_env_queue_full('gw_test', 'secret')
        self.assertEqual(env['GATEWAY_SAMPLE_RATE_HZ'], '1000')
        self.assertEqual(env['GATEWAY_SAMPLES_PER_BATCH'], '100')
        self.assertEqual(env['GATEWAY_MAX_INFLIGHT'], '4')
        self.assertEqual(env['GATEWAY_OUTBOX_MAX_ROWS'], '10')

    def test_build_queue_full_assert_sql(self):
        spec = R.default_specs()[0]
        mids = ['mid-1', 'mid-2', 'mid-3']
        sql = R.build_queue_full_assert_sql(spec, mids, 100, 1000, 'TEST_MARKER')
        self.assertIn('TEST_MARKER', sql)
        self.assertIn(spec.gateway_id, sql)
        self.assertIn(spec.sensor_urn, sql)
        for m in mids:
            self.assertIn(f"'{m}'", sql)
        self.assertIn('l_cnt <> 3', sql)
        self.assertIn('r_cnt <> 300', sql)
        self.assertIn('t_cnt <> 3', sql)
        self.assertIn("property_name = 'sample_interval_us'", sql)
        self.assertIn('latest_obs', sql)
        self.assertIn('latest_seq', sql)


class GatewayReconnectTests(unittest.TestCase):
    """3.6.6 Step 1 pure unit tests for gateway-reconnect helpers and oracles."""

    def test_parse_gateway_transport_events(self):
        sample_logs = (
            'level=INFO msg="gateway simulator running" gateway=gw_001\n'
            'level=INFO msg="gatewaysim transport connected" event=connected epoch=1\n'
            'level=INFO msg="gatewaysim transport subscribed" event=subscribed epoch=1 filter=gateways/gw_001/acks/#\n'
            'level=INFO msg="gatewaysim transport connection lost" event=connection_lost epoch=1\n'
            'level=INFO msg="gatewaysim transport disconnected" event=disconnected epoch=1\n'
            'level=INFO msg="gatewaysim transport connected" event=connected epoch=2\n'
            'level=INFO msg="gatewaysim transport subscribed" event=subscribed epoch=2 filter=gateways/gw_001/acks/#\n'
        )
        evs = R.parse_gateway_transport_events(sample_logs)
        self.assertEqual(len(evs), 6)
        self.assertEqual(evs[0], {'event': 'connected', 'epoch': 1})
        self.assertEqual(evs[1], {'event': 'subscribed', 'epoch': 1, 'filter': 'gateways/gw_001/acks/#'})
        self.assertEqual(evs[2], {'event': 'connection_lost', 'epoch': 1})
        self.assertEqual(evs[3], {'event': 'disconnected', 'epoch': 1})
        self.assertEqual(evs[4], {'event': 'connected', 'epoch': 2})
        self.assertEqual(evs[5], {'event': 'subscribed', 'epoch': 2, 'filter': 'gateways/gw_001/acks/#'})

    def test_gateway_reconnect_events_ordering(self):
        # Disconnect on epoch 1 precedes subscribed on epoch 2 -> OK
        evs_good = [
            {'event': 'connected', 'epoch': 1},
            {'event': 'subscribed', 'epoch': 1},
            {'event': 'disconnected', 'epoch': 1},
            {'event': 'connected', 'epoch': 2},
            {'event': 'subscribed', 'epoch': 2},
        ]
        self.assertTrue(R.gateway_reconnect_events_ok(evs_good, min_epoch=2))

        # Missing disconnect on epoch 1 -> False
        evs_no_disc = [
            {'event': 'connected', 'epoch': 1},
            {'event': 'subscribed', 'epoch': 1},
            {'event': 'connected', 'epoch': 2},
            {'event': 'subscribed', 'epoch': 2},
        ]
        self.assertFalse(R.gateway_reconnect_events_ok(evs_no_disc, min_epoch=2))

        # Missing subscribed on epoch 2 -> False
        evs_no_sub = [
            {'event': 'connected', 'epoch': 1},
            {'event': 'subscribed', 'epoch': 1},
            {'event': 'disconnected', 'epoch': 1},
            {'event': 'connected', 'epoch': 2},
        ]
        self.assertFalse(R.gateway_reconnect_events_ok(evs_no_sub, min_epoch=2))

        # Inverted ordering: subscribed on epoch 2 appears before disconnect on epoch 1 -> False
        evs_inverted = [
            {'event': 'subscribed', 'epoch': 2},
            {'event': 'disconnected', 'epoch': 1},
        ]
        self.assertFalse(R.gateway_reconnect_events_ok(evs_inverted, min_epoch=2))

    def test_reconnect_pending_identity_unchanged(self):
        row = {
            'message_id': 'mid-001',
            'topic': 'gateways/gw_1/telemetry/degC',
            'payload': b'{"samples":[20.1]}',
            'boot_id': 'boot-123',
            'first_sequence': 0,
            'sample_count': 100,
            'sample_interval_us': 10000,
            'byte_size': 20,
        }
        # Identical row
        self.assertTrue(R.reconnect_pending_identity_unchanged(row, dict(row)))

        # Mutated message_id -> False
        mut_mid = dict(row, message_id='mid-002')
        self.assertFalse(R.reconnect_pending_identity_unchanged(row, mut_mid))

        # Mutated payload bytes -> False
        mut_payload = dict(row, payload=b'{"samples":[99.9]}')
        self.assertFalse(R.reconnect_pending_identity_unchanged(row, mut_payload))

        # Mutated boot_id -> False
        mut_boot = dict(row, boot_id='boot-456')
        self.assertFalse(R.reconnect_pending_identity_unchanged(row, mut_boot))

        # Mutated sequence range -> False
        mut_seq = dict(row, first_sequence=100)
        self.assertFalse(R.reconnect_pending_identity_unchanged(row, mut_seq))

    def test_gateway_reconnect_oracle(self):
        kwargs = dict(
            committed_init=True,
            pending_init=True,
            disconnect_observed=True,
            reconnect_subscribed=True,
            resend_observed=True,
            resend_bytes_match=True,
            pending_identity_match=True,
            db_unchanged_during=True,
            receive_restored=True,
            pending_after=0,
            cli_pending_rows=0,
            simulator_exit_code=0,
            db_unchanged_final=True,
        )
        self.assertTrue(R.gateway_reconnect_oracle(**kwargs))

        # Fails on nonzero exit code
        self.assertFalse(R.gateway_reconnect_oracle(**dict(kwargs, simulator_exit_code=1)))

        # Fails on remaining pending rows
        self.assertFalse(R.gateway_reconnect_oracle(**dict(kwargs, pending_after=1)))
        self.assertFalse(R.gateway_reconnect_oracle(**dict(kwargs, cli_pending_rows=1)))

        # Fails on missing disconnect observation
        self.assertFalse(R.gateway_reconnect_oracle(**dict(kwargs, disconnect_observed=False)))

        # Fails on missing subscription before resend
        self.assertFalse(R.gateway_reconnect_oracle(**dict(kwargs, reconnect_subscribed=False)))

        # Fails on resend mismatch
        self.assertFalse(R.gateway_reconnect_oracle(**dict(kwargs, resend_bytes_match=False)))

        # Fails on DB mutation during blocked resend
        self.assertFalse(R.gateway_reconnect_oracle(**dict(kwargs, db_unchanged_during=False)))

    def test_drain_publish_frames(self):
        class FakeReceiver:
            def __init__(self, frames):
                self._frames = list(frames)
            def publish_frame(self, timeout):
                if self._frames:
                    return self._frames.pop(0)
                return None

        fake = FakeReceiver([('top1', b'pay1'), ('top2', b'pay2')])
        drained = R.drain_publish_frames(fake, timeout=0.05)
        self.assertEqual(len(drained), 2)
        self.assertEqual(drained[0], ('top1', b'pay1'))
        self.assertEqual(drained[1], ('top2', b'pay2'))
        # Second drain should return empty list
        self.assertEqual(R.drain_publish_frames(fake, timeout=0.05), [])


class ProcessRestartTests(unittest.TestCase):
    """3.6.6 steps 2-3 pure unit tests for process restart helpers and oracles."""

    def test_backend_clean_shutdown(self):
        # Clean exit 0 with shutdown complete log -> True
        logs_ok = 'level=INFO msg="telemetry ingress summary" total=1\nlevel=INFO msg="backend shutdown complete"\n'
        self.assertTrue(R.backend_clean_shutdown_ok(0, logs_ok))
        self.assertTrue(R.backend_clean_shutdown_ok('0', logs_ok))

        # Non-zero exit code -> False
        self.assertFalse(R.backend_clean_shutdown_ok(1, logs_ok))
        self.assertFalse(R.backend_clean_shutdown_ok('143', logs_ok))

        # Incomplete teardown -> False
        logs_incomplete = 'level=ERROR msg="backend shutdown incomplete"\nlevel=INFO msg="backend shutdown complete"\n'
        self.assertFalse(R.backend_clean_shutdown_ok(0, logs_incomplete))

        # Panic -> False
        logs_panic = 'panic: runtime error: invalid memory address\nbackend shutdown complete\n'
        self.assertFalse(R.backend_clean_shutdown_ok(0, logs_panic))

        # Missing shutdown complete log -> False
        self.assertFalse(R.backend_clean_shutdown_ok(0, 'level=INFO msg="terminated"\n'))

    def test_gateway_stopped_pending_truthful(self):
        # Stopped with pending items, completed=false -> True (truthful non-completion)
        logs_pending = ('run summary: enqueued=1 publish_attempts=1 app_acknowledged=0 '
                        'pending_rows=1 pending_bytes=100 quarantined_rows=0 quarantined_bytes=0 completed=false\n')
        self.assertTrue(R.gateway_stopped_pending_ok(143, logs_pending, pending_count=1))
        self.assertTrue(R.gateway_stopped_pending_ok(1, logs_pending, pending_count=1))

        # Stopped with pending items, but claims completed=true -> False (false completion vetoed)
        logs_false_complete = ('run summary: enqueued=1 publish_attempts=1 app_acknowledged=0 '
                               'pending_rows=1 pending_bytes=100 quarantined_rows=0 quarantined_bytes=0 completed=true\n')
        self.assertFalse(R.gateway_stopped_pending_ok(0, logs_false_complete, pending_count=1))
        self.assertFalse(R.gateway_stopped_pending_ok(143, logs_false_complete, pending_count=1))

        # Zero pending items, exit 0, completed=true -> True (truthful completion)
        logs_done = ('run summary: enqueued=1 publish_attempts=1 app_acknowledged=1 '
                     'pending_rows=0 pending_bytes=0 quarantined_rows=0 quarantined_bytes=0 completed=true\n')
        self.assertTrue(R.gateway_stopped_pending_ok(0, logs_done, pending_count=0))

        # Zero pending items, exit non-zero -> False
        self.assertFalse(R.gateway_stopped_pending_ok(1, logs_done, pending_count=0))

    def test_gateway_restart_boot_distinction(self):
        old_row = {
            'message_id': '0195e18c-9fc1-7a42-9064-69ea49e63bf1',
            'boot_id': '7f2c45f7-0a76-4e28-8a37-7793d7d85b01',
            'first_sequence': 0,
            'topic': 'gateways/gw_b/telemetry/degC',
        }
        new_row = {
            'message_id': '0195e18c-9fc1-7a42-9064-69ea49e63bf2',
            'boot_id': '7f2c45f7-0a76-4e28-8a37-7793d7d85b02',
            'first_sequence': 0,
            'topic': 'gateways/gw_b/telemetry/degC',
        }
        # Distinct boot UUID, new sequence starts at 0 -> True
        self.assertTrue(R.gateway_restart_boot_distinction(old_row, new_row))

        # Same boot UUID -> False
        same_boot = dict(new_row, boot_id=old_row['boot_id'])
        self.assertFalse(R.gateway_restart_boot_distinction(old_row, same_boot))

        # New first_sequence non-zero -> False
        non_zero_seq = dict(new_row, first_sequence=100)
        self.assertFalse(R.gateway_restart_boot_distinction(old_row, non_zero_seq))

        # Same message_id -> False
        same_mid = dict(new_row, message_id=old_row['message_id'])
        self.assertFalse(R.gateway_restart_boot_distinction(old_row, same_mid))

        # Mismatched topic -> False
        diff_topic = dict(new_row, topic='gateways/gw_other/telemetry/degC')
        self.assertFalse(R.gateway_restart_boot_distinction(old_row, diff_topic))

    def test_same_file_binding(self):
        with tempfile.NamedTemporaryFile() as tf:
            path_str = tf.name
            path_obj = Path(tf.name)
            self.assertTrue(R.same_file_binding_ok(path_str, path_obj))
            self.assertTrue(R.same_file_binding_ok(path_obj, path_str))
            self.assertTrue(R.same_file_binding_ok(path_str, path_str))

        self.assertFalse(R.same_file_binding_ok('/tmp/a.db', '/tmp/b.db'))

    def test_process_restart_accounting(self):
        # Exact valid accounting: Subcase 1 = 1 ledger / 100 raw; Subcase 2 = 2 ledgers / 200 raw;
        # Total = 3 ledgers / 300 raw / 3 temporal -> True
        self.assertTrue(R.process_restart_accounting_ok(
            sub1_ledgers=1, sub2_ledgers=2,
            sub1_raw=100, sub2_raw=200,
            total_ledgers=3, total_raw=300, total_temporal=3,
        ))

        # Drop Subcase 2 (e.g. only 1 ledger recorded for Gateway B) -> False
        self.assertFalse(R.process_restart_accounting_ok(
            sub1_ledgers=1, sub2_ledgers=1,
            sub1_raw=100, sub2_raw=100,
            total_ledgers=2, total_raw=200, total_temporal=2,
        ))

        # Subcase 1 duplicate not deduplicated (2 ledgers for Subcase 1) -> False
        self.assertFalse(R.process_restart_accounting_ok(
            sub1_ledgers=2, sub2_ledgers=2,
            sub1_raw=200, sub2_raw=200,
            total_ledgers=4, total_raw=400, total_temporal=4,
        ))

        # Total ledgers mismatch -> False
        self.assertFalse(R.process_restart_accounting_ok(
            sub1_ledgers=1, sub2_ledgers=2,
            sub1_raw=100, sub2_raw=200,
            total_ledgers=4, total_raw=300, total_temporal=3,
        ))

    def test_mapped_port_refresh(self):
        # mapped_port parses docker port output lines and extracts integer port
        import stage3_mqtt_ingress_runner as MIR

        class _FakeDocker:
            def __init__(self, output: bytes):
                self.output = output
            def __call__(self, args, timeout=None):
                return self.output

        saved_docker = MIR.docker
        try:
            MIR.docker = _FakeDocker(b'0.0.0.0:49152\n:::49152\n')
            port = R.mapped_port('fake-cid', '8080/tcp')
            self.assertEqual(port, 49152)

            # Different port on second inspect / restart
            MIR.docker = _FakeDocker(b'0.0.0.0:49200\n')
            port_refreshed = R.mapped_port('fake-cid', '8080/tcp')
            self.assertEqual(port_refreshed, 49200)

            # Empty output raises RuntimeError
            MIR.docker = _FakeDocker(b'')
            with self.assertRaises(RuntimeError):
                R.mapped_port('fake-cid', '8080/tcp')
        finally:
            MIR.docker = saved_docker


class BrokerControlledRestartTests(unittest.TestCase):
    """3.6.7 pure unit tests for broker controlled restart helpers and predicates."""

    def test_dynsec_persistence_comparison_ok_passes_on_exact_match(self):
        pre_client = {'client': {'username': 'gw_a', 'disabled': False, 'roles': [{'rolename': 'r_uplink'}]}}
        post_client = {'client': {'username': 'gw_a', 'disabled': False, 'roles': [{'rolename': 'r_uplink'}, {'rolename': 'r_hook'}]}}
        pre_disabled = {'client': {'username': 'probe_dis', 'disabled': True}}
        post_disabled = {'client': {'username': 'probe_dis', 'disabled': True}}
        pre_role = {'role': {'rolename': 'r_uplink', 'acls': [
            {'acltype': 'publishClientSend', 'topic': 'gateways/gw_a/telemetry/#', 'allow': True, 'priority': 0},
            {'acltype': 'publishClientReceive', 'topic': 'gateways/gw_a/acks/#', 'allow': True, 'priority': 0},
        ]}}
        post_role = {'role': {'rolename': 'r_uplink', 'acls': [
            {'acltype': 'publishClientReceive', 'topic': 'gateways/gw_a/acks/#', 'allow': True, 'priority': 0},
            {'acltype': 'publishClientSend', 'topic': 'gateways/gw_a/telemetry/#', 'allow': True, 'priority': 0},
        ]}}
        self.assertTrue(R.dynsec_persistence_comparison_ok(
            pre_client, post_client, pre_disabled, post_disabled, pre_role, post_role))

    def test_dynsec_persistence_comparison_fails_on_disabled_mutation(self):
        pre_client = {'client': {'username': 'gw_a', 'disabled': False, 'roles': [{'rolename': 'r_uplink'}]}}
        post_client = {'client': {'username': 'gw_a', 'disabled': True, 'roles': [{'rolename': 'r_uplink'}]}}
        pre_disabled = {'client': {'username': 'probe_dis', 'disabled': True}}
        post_disabled = {'client': {'username': 'probe_dis', 'disabled': True}}
        role = {'role': {'rolename': 'r_uplink', 'acls': [{'acltype': 'publishClientSend', 'topic': 't/#', 'allow': True, 'priority': 0}]}}

        # Gateway A disabled becomes True -> False
        self.assertFalse(R.dynsec_persistence_comparison_ok(
            pre_client, post_client, pre_disabled, post_disabled, role, role))

        # Probe disabled becomes False -> False
        post_dis_mut = {'client': {'username': 'probe_dis', 'disabled': False}}
        post_cli_ok = {'client': {'username': 'gw_a', 'disabled': False, 'roles': [{'rolename': 'r_uplink'}]}}
        self.assertFalse(R.dynsec_persistence_comparison_ok(
            pre_client, post_cli_ok, pre_disabled, post_dis_mut, role, role))

    def test_dynsec_persistence_comparison_fails_on_acl_mutation(self):
        client_a = {'client': {'username': 'gw_a', 'disabled': False, 'roles': [{'rolename': 'r_uplink'}]}}
        client_dis = {'client': {'username': 'probe_dis', 'disabled': True}}
        pre_role = {'role': {'rolename': 'r_uplink', 'acls': [
            {'acltype': 'publishClientSend', 'topic': 't/#', 'allow': True, 'priority': 0}
        ]}}
        post_role_mut = {'role': {'rolename': 'r_uplink', 'acls': [
            {'acltype': 'publishClientSend', 'topic': 't/#', 'allow': False, 'priority': 0}
        ]}}
        self.assertFalse(R.dynsec_persistence_comparison_ok(
            client_a, client_a, client_dis, client_dis, pre_role, post_role_mut))

    def test_dynsec_persistence_comparison_fails_on_missing_role(self):
        pre_client = {'client': {'username': 'gw_a', 'disabled': False, 'roles': [{'rolename': 'r_uplink'}, {'rolename': 'r_other'}]}}
        post_client = {'client': {'username': 'gw_a', 'disabled': False, 'roles': [{'rolename': 'r_uplink'}]}}
        client_dis = {'client': {'username': 'probe_dis', 'disabled': True}}
        role = {'role': {'rolename': 'r_uplink', 'acls': [{'acltype': 'publishClientSend', 'topic': 't/#', 'allow': True, 'priority': 0}]}}
        self.assertFalse(R.dynsec_persistence_comparison_ok(
            pre_client, post_client, client_dis, client_dis, role, role))

    def test_broker_restart_login_outcomes(self):
        # All correct -> True
        self.assertTrue(R.broker_restart_login_outcomes_ok(
            new_pw_login=True, old_pw_login_denied=True,
            disabled_login_denied=True, positive_gw_login=True))

        # Any failure -> False
        self.assertFalse(R.broker_restart_login_outcomes_ok(
            new_pw_login=False, old_pw_login_denied=True,
            disabled_login_denied=True, positive_gw_login=True))
        self.assertFalse(R.broker_restart_login_outcomes_ok(
            new_pw_login=True, old_pw_login_denied=False,
            disabled_login_denied=True, positive_gw_login=True))
        self.assertFalse(R.broker_restart_login_outcomes_ok(
            new_pw_login=True, old_pw_login_denied=True,
            disabled_login_denied=False, positive_gw_login=True))
        self.assertFalse(R.broker_restart_login_outcomes_ok(
            new_pw_login=True, old_pw_login_denied=True,
            disabled_login_denied=True, positive_gw_login=False))

    def test_broker_same_store_constraints(self):
        # Matching container and volumes -> True
        self.assertTrue(R.broker_same_store_constraints_ok(
            cid_before='cid_1', cid_after='cid_1',
            dynsec_vol_before='vol_dyn', dynsec_vol_after='vol_dyn',
            data_vol_before='vol_data', data_vol_after='vol_data'))

        # Mutated container ID -> False
        self.assertFalse(R.broker_same_store_constraints_ok(
            cid_before='cid_1', cid_after='cid_2',
            dynsec_vol_before='vol_dyn', dynsec_vol_after='vol_dyn',
            data_vol_before='vol_data', data_vol_after='vol_data'))

        # Mutated dynsec volume -> False
        self.assertFalse(R.broker_same_store_constraints_ok(
            cid_before='cid_1', cid_after='cid_1',
            dynsec_vol_before='vol_dyn_1', dynsec_vol_after='vol_dyn_2',
            data_vol_before='vol_data', data_vol_after='vol_data'))

        # Mutated data volume -> False
        self.assertFalse(R.broker_same_store_constraints_ok(
            cid_before='cid_1', cid_after='cid_1',
            dynsec_vol_before='vol_dyn', dynsec_vol_after='vol_dyn',
            data_vol_before='vol_data_1', data_vol_after='vol_data_2'))

        # Empty / None values -> False
        self.assertFalse(R.broker_same_store_constraints_ok(
            cid_before='', cid_after='',
            dynsec_vol_before='vol_dyn', dynsec_vol_after='vol_dyn',
            data_vol_before='vol_data', data_vol_after='vol_data'))

    def test_broker_stopped_state_predicates(self):
        # /healthz 200, /readyz non-200 (503), db unchanged, pending unchanged -> True
        self.assertTrue(R.broker_stopped_state_ok(
            health_status_stopped=200, ready_status_stopped=503, db_unchanged=True, pending_unchanged=True))

        # /readyz None (connection refused / stale port / backend dead) -> False
        self.assertFalse(R.broker_stopped_state_ok(
            health_status_stopped=200, ready_status_stopped=None, db_unchanged=True, pending_unchanged=True))

        # /healthz not 200 (backend dead / unhealthy) -> False
        self.assertFalse(R.broker_stopped_state_ok(
            health_status_stopped=None, ready_status_stopped=503, db_unchanged=True, pending_unchanged=True))
        self.assertFalse(R.broker_stopped_state_ok(
            health_status_stopped=500, ready_status_stopped=503, db_unchanged=True, pending_unchanged=True))

        # /readyz 200 while stopped -> False
        self.assertFalse(R.broker_stopped_state_ok(
            health_status_stopped=200, ready_status_stopped=200, db_unchanged=True, pending_unchanged=True))

        # db mutated -> False
        self.assertFalse(R.broker_stopped_state_ok(
            health_status_stopped=200, ready_status_stopped=503, db_unchanged=False, pending_unchanged=True))

        # pending mutated or lost -> False
        self.assertFalse(R.broker_stopped_state_ok(
            health_status_stopped=200, ready_status_stopped=503, db_unchanged=True, pending_unchanged=False))

    def test_probe_http_status_catches_http_error_code(self):
        from unittest.mock import patch
        import urllib.error

        # HTTP 503 from urllib raises HTTPError; must return 503, not None.
        with patch('urllib.request.urlopen') as mock_url:
            mock_url.side_effect = urllib.error.HTTPError('http://127.0.0.1:8080/readyz', 503, 'Service Unavailable', {}, None)
            self.assertEqual(R.probe_http_status(8080, '/readyz'), 503)

        # Connection refused / network error raises OSError; must return None.
        with patch('urllib.request.urlopen') as mock_url:
            mock_url.side_effect = ConnectionRefusedError('Connection refused')
            self.assertIsNone(R.probe_http_status(8080, '/readyz'))

    def test_post_restart_denial_log_scoping_ignores_old_logs(self):
        old_log = "1726000000: Denied PUBLISH from gw_a (d0, q1, r0, m1, 'gateways/gw_b/telemetry/degC', ...)\n"
        # Before new publish, logs_after == logs_before -> False (no new denial)
        self.assertFalse(R.denied_publish_logged(old_log, old_log, 'gateways/gw_b/telemetry/degC'))

        # After new publish, new line appeared -> True
        new_log = old_log + "1726000010: Denied PUBLISH from gw_a (d0, q1, r0, m2, 'gateways/gw_b/telemetry/degC', ...)\n"
        self.assertTrue(R.denied_publish_logged(old_log, new_log, 'gateways/gw_b/telemetry/degC'))


if __name__ == '__main__':
    unittest.main()
