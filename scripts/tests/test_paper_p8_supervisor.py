import json
import os
import signal
import tempfile
import time
import unittest
import shutil
from pathlib import Path
from unittest import mock

import paper_p8_supervisor as supervisor


class SupervisorStateTests(unittest.TestCase):
    def test_e4_expiry_phase_plan_is_last_and_excludes_cold(self):
        with tempfile.TemporaryDirectory(dir='/var/tmp/opencode/p8-remote-step1') as directory:
            root = Path(directory); root.chmod(0o700)
            state = supervisor.initialize(root, experiment='E4', arm='warm', seed=89,
                deadline_seconds=900, receipts=[], preparation_status='E4_PREPARED',
                expiry_replay_enabled=True,
                e4_phase_plan=['unique', 'replay', 'expiry-replay'])
            self.assertEqual(state['phase_id'], 'unique')
        with tempfile.TemporaryDirectory(dir='/var/tmp/opencode/p8-remote-step1') as bad:
            with self.assertRaisesRegex(ValueError, 'session_e4_phase_plan_rejected'):
                supervisor.initialize(Path(bad), experiment='E4', arm='warm', seed=89,
                    deadline_seconds=900, receipts=[], preparation_status='E4_PREPARED',
                    expiry_replay_enabled=True,
                    e4_phase_plan=['unique', 'replay', 'cold-replay', 'expiry-replay'])

    def private_root(self):
        path = Path(tempfile.mkdtemp(dir='/var/tmp/opencode/p8-remote-step1'))
        path.chmod(0o700)
        self.addCleanup(shutil.rmtree, path)
        return path

    def test_profile_metadata_defaults_to_pilot_and_is_portable(self):
        root = self.private_root()
        before = time.monotonic()
        state = supervisor.initialize(root, experiment='E1', arm='baseline', seed=7,
            deadline_seconds=10, receipts=[])
        after = time.monotonic()
        self.assertEqual(state['profile'], 'pilot')
        self.assertGreaterEqual(state['deadline_monotonic'], before + 10)
        self.assertLessEqual(state['deadline_monotonic'], after + 10)
        self.assertNotIn(str(root), json.dumps(state))

    def test_official_profile_requires_explicit_opt_in(self):
        root = self.private_root()
        with self.assertRaisesRegex(ValueError, 'official_profile_opt_in_required'):
            supervisor.initialize(root, experiment='E1', arm='baseline', seed=7,
                deadline_seconds=10, receipts=[], profile='official')

    def test_invalid_profile_rejected_before_session_file(self):
        root = self.private_root()
        with self.assertRaisesRegex(ValueError, 'session_metadata_rejected'):
            supervisor.initialize(root, experiment='E1', arm='baseline', seed=7,
                deadline_seconds=10, receipts=[], profile='ignored')
        self.assertFalse((root / 'session.json').exists())

    def test_refresh_owned_receipts_requires_exact_registry_receipt_set(self):
        root = self.private_root()
        state = {'experiment': 'E4', 'project': 'ncs_0123456789ab'}
        row = {'kind': 'container', 'name': 'ncs_0123456789ab-backend-new',
            'receipt': {'id': 'b' * 64}, 'task': 'native-compose-allocation',
            'run': state['project'], 'allocation_attempted': True}
        path = root / 'resource-new.json'
        path.write_text(json.dumps(row)); path.chmod(0o600)
        supervisor.refresh_owned_receipts(root, state, [row])
        self.assertEqual(state['receipts'], [row])
        with self.assertRaisesRegex(ValueError, 'receipt_set_mismatch'):
            supervisor.refresh_owned_receipts(root, state, [])

    def test_hold_accepts_finalize_and_marks_closed_not_pass(self):
        with tempfile.TemporaryDirectory(dir='/var/tmp/opencode/p8-remote-step1') as directory:
            root = Path(directory); root.chmod(0o700)
            supervisor.initialize(root, experiment='E1', arm='baseline', seed=7,
                                  deadline_seconds=10, receipts=[])
            supervisor.write_request(root, {'session_id': supervisor.read_state(root)['session_id'],
                'command': 'finalize', 'phase': 'READY'})
            result = supervisor.hold(root, poll_seconds=0)
            self.assertEqual(result['phase'], 'FINALIZING')
            self.assertEqual(result['workload_outcome'], 'NOT_RUN')
            closed = supervisor.finalize(root, result, cleanup_verified=True)
            self.assertEqual(closed['phase'], 'CLOSED')
            self.assertEqual(closed['cleanup_outcome'], 'VERIFIED')

    def test_owned_guard_preflight_threshold_aborts_and_preserves_cleanup_outcome(self):
        root = self.private_root()
        state = supervisor.initialize(root, experiment='E2', arm='official', seed=84,
            deadline_seconds=30, receipts=[])
        guard = lambda: {'ok': False, 'reason': 'tiny_growth_limit',
            'report': {'status': 'threshold', 'growth_limit_bytes': 1, 'terminal': {'growth_bytes': 1}}}
        stopped = supervisor.hold(root, poll_seconds=0, owned_guard=guard)
        self.assertEqual(stopped['phase'], 'FINALIZING')
        self.assertEqual(stopped['primary_stop_reason'], 'tiny_growth_limit')
        self.assertEqual(stopped['workload_outcome'], 'ABORTED')
        self.assertEqual(stopped['owned_storage_pending_status'], 'NOT_VERIFIED')
        self.assertEqual(stopped['remote_b_stop_instruction'], 'STOP_LOCALLY_ON_HOST_B')
        closed = supervisor.finalize(root, stopped, cleanup_verified=False,
            cleanup_error='owned fixture cleanup failed')
        self.assertEqual(closed['primary_stop_reason'], 'tiny_growth_limit')
        self.assertEqual(closed['cleanup_outcome'], 'FAILED')
        self.assertEqual(closed['cleanup_error'], 'owned fixture cleanup failed')

    def test_owned_guard_threshold_during_hold_aborts_active_measurement(self):
        root = self.private_root()
        state = supervisor.initialize(root, experiment='E2', arm='pilot', seed=84,
            deadline_seconds=30, receipts=[])
        calls = {'count': 0}
        def guard():
            calls['count'] += 1
            return {'ok': calls['count'] < 3, 'reason': 'owned_db_growth_limit',
                'report': {'status': 'sampling' if calls['count'] < 3 else 'threshold'}}
        supervisor.write_request(root, {'session_id': state['session_id'],
            'command': 'begin', 'phase': 'READY', 'phase_id': 'samples-1'})
        with mock.patch.object(supervisor.time, 'sleep', return_value=None):
            stopped = supervisor.hold(root, poll_seconds=0, on_begin=lambda _: {}, owned_guard=guard)
        self.assertEqual(stopped['phase'], 'FINALIZING')
        self.assertEqual(stopped['primary_stop_reason'], 'owned_db_growth_limit')
        self.assertEqual(stopped['phase_id'], 'samples-1')

    def test_owned_guard_measurement_failure_fails_closed_before_load(self):
        root = self.private_root()
        supervisor.initialize(root, experiment='E1', arm='pilot', seed=86,
            deadline_seconds=30, receipts=[])
        stopped = supervisor.hold(root, poll_seconds=0,
            owned_guard=lambda: (_ for _ in ()).throw(OSError('reader down')))
        self.assertEqual(stopped['primary_stop_reason'], 'OWNED_STORAGE_MEASUREMENT_UNAVAILABLE')
        self.assertEqual(stopped['owned_storage_guard']['status'], 'measurement_unavailable')
        self.assertEqual(stopped['workload_outcome'], 'ABORTED')

    def test_e3_finalize_after_collector_started_marks_unfinished_workload_aborted(self):
        with tempfile.TemporaryDirectory(dir='/var/tmp/opencode/p8-remote-step1') as directory:
            root = Path(directory); root.chmod(0o700)
            state = supervisor.initialize(root, experiment='E3', arm='polling', seed=89,
                deadline_seconds=10, receipts=[], preparation_status='E3_PREPARED')
            supervisor.write_request(root, {'session_id':state['session_id'],
                'command':'begin', 'phase':'READY', 'phase_id':'e3-low_load-polling'})
            def finalize_while_measuring(_):
                current = supervisor.read_state(root)
                if current['phase'] == 'MEASURING' and not (root / 'request.json').exists():
                    supervisor.write_request(root, {'session_id':state['session_id'],
                        'command':'finalize', 'phase':'MEASURING'})
            with mock.patch.object(supervisor.time, 'sleep', side_effect=finalize_while_measuring):
                finalizing = supervisor.hold(root, poll_seconds=0.01, on_begin=lambda _: {})
            self.assertEqual(finalizing['workload_outcome'], 'ABORTED')

    def test_e3_preparation_only_finalize_keeps_not_run(self):
        with tempfile.TemporaryDirectory(dir='/var/tmp/opencode/p8-remote-step1') as directory:
            root = Path(directory); root.chmod(0o700)
            state = supervisor.initialize(root, experiment='E3', arm='polling', seed=89,
                deadline_seconds=10, receipts=[], preparation_status='E3_PREPARED')
            supervisor.write_request(root, {'session_id':state['session_id'],
                'command':'finalize', 'phase':'READY'})
            finalizing = supervisor.hold(root, poll_seconds=0)
            self.assertEqual(finalizing['workload_outcome'], 'NOT_RUN')

    def test_e4_requires_a_validated_unique_result_before_replay_begin(self):
        with tempfile.TemporaryDirectory(dir='/var/tmp/opencode/p8-remote-step1') as directory:
            root = Path(directory); root.chmod(0o700)
            state = supervisor.initialize(root, experiment='E4', arm='warm', seed=89,
                deadline_seconds=30, receipts=[], preparation_status='E4_PREPARED',
                cold_replay_enabled=True)
            sid = state['session_id']
            supervisor.write_request(root, {'session_id': sid, 'command': 'begin',
                'phase': 'READY', 'phase_id': 'unique'})
            expected = [('begin', 'unique'), ('finish-phase', 'unique'),
                        ('next-phase', 'replay'), ('begin', 'replay'),
                        ('finish-phase', 'replay'), ('next-phase', 'cold-replay'),
                        ('begin', 'cold-replay'), ('finish-phase', 'cold-replay'),
                        ('finalize', None)]
            sent = {0}  # The initial unique begin request is already published.

            def publish_after_step(_seconds):
                current = supervisor.read_state(root)
                index = len(sent)
                if index >= len(expected) or current.get('phase') not in ('MEASURING', 'VERIFYING', 'READY'):
                    return
                command, phase_id = expected[index]
                if command == 'begin' and current.get('phase') == 'READY' and current.get('phase_id') == phase_id:
                    supervisor.write_request(root, {'session_id': sid, 'command': command,
                        'phase': 'READY', 'phase_id': phase_id})
                    sent.add(index)
                elif command == 'finish-phase' and current.get('phase') == 'MEASURING' and current.get('phase_id') == phase_id:
                    supervisor.write_request(root, {'session_id': sid, 'command': command,
                        'phase': 'MEASURING', 'phase_id': phase_id, 'results': '/portable/results.json'})
                    sent.add(index)
                elif command == 'next-phase' and current.get('phase') == 'VERIFYING' and current.get('phase_id') == 'unique':
                    supervisor.write_request(root, {'session_id': sid, 'command': command,
                        'phase': 'VERIFYING', 'phase_id': 'replay'})
                    sent.add(index)
                elif command == 'next-phase' and current.get('phase') == 'VERIFYING' and current.get('phase_id') == 'replay':
                    supervisor.write_request(root, {'session_id': sid, 'command': command,
                        'phase': 'VERIFYING', 'phase_id': 'cold-replay'})
                    sent.add(index)
                elif command == 'finalize' and current.get('phase') == 'VERIFYING' and current.get('phase_id') == 'cold-replay':
                    supervisor.write_request(root, {'session_id': sid, 'command': command,
                        'phase': 'VERIFYING'})
                    sent.add(index)

            def finish(request):
                return {'workload_outcome': 'VALIDATED', 'validation_valid': True,
                    'phase_id': request['phase_id'],
                    'children_stopped_verified': True}

            def replace(request, current):
                self.assertEqual(request['phase_id'], 'cold-replay')
                self.assertEqual(current['phase'], 'VERIFYING')
                return {'replacement_verified': True, 'receipts': [{'name': 'new-backend'}],
                    'replacement_proof': {'metrics_readback': True}}

            with mock.patch.object(supervisor.time, 'sleep', side_effect=publish_after_step):
                finalizing = supervisor.hold(root, poll_seconds=0.01,
                    on_begin=lambda req: {'phase': req['phase_id']}, on_finish=finish,
                    on_next=replace)
            self.assertEqual(finalizing['phase'], 'FINALIZING')
            self.assertEqual(finalizing['workload_outcome'], 'VALIDATED')
            self.assertEqual(finalizing['phase_history'], ['unique', 'replay', 'cold-replay'])
            self.assertEqual(finalizing['receipts'], [{'name': 'new-backend'}])
            self.assertTrue(finalizing['replacement_proof']['metrics_readback'])

    def test_rejects_wrong_session_and_phase_without_consuming_lifecycle(self):
        with tempfile.TemporaryDirectory(dir='/var/tmp/opencode/p8-remote-step1') as directory:
            root = Path(directory); root.chmod(0o700)
            supervisor.initialize(root, experiment='E1', arm='baseline', seed=7,
                                  deadline_seconds=10, receipts=[])
            supervisor.write_request(root, {'session_id': 'wrong', 'command': 'finalize', 'phase': 'READY'})
            session_id = supervisor.read_state(root)['session_id']
            def stop_after_rejection(_seconds):
                if not (root / 'request.json').exists():
                    supervisor.write_request(root, {'session_id': session_id,
                        'command': 'abort', 'phase': 'READY'})
            with mock.patch.object(supervisor.time, 'sleep', side_effect=stop_after_rejection):
                state = supervisor.hold(root, poll_seconds=0.01)
            self.assertEqual(state['last_request_error'], 'request_session_mismatch')

    def test_rejects_wrong_phase(self):
        with tempfile.TemporaryDirectory(dir='/var/tmp/opencode/p8-remote-step1') as directory:
            root = Path(directory); root.chmod(0o700)
            supervisor.initialize(root, experiment='E1', arm='baseline', seed=7,
                                  deadline_seconds=10, receipts=[])
            supervisor.write_request(root, {'session_id': supervisor.read_state(root)['session_id'],
                'command': 'finalize', 'phase': 'MEASURING'})
            session_id = supervisor.read_state(root)['session_id']
            def stop_after_rejection(_seconds):
                if not (root / 'request.json').exists():
                    supervisor.write_request(root, {'session_id': session_id,
                        'command': 'abort', 'phase': 'READY'})
            with mock.patch.object(supervisor.time, 'sleep', side_effect=stop_after_rejection):
                state = supervisor.hold(root, poll_seconds=0.01)
            self.assertEqual(state['last_request_error'], 'request_phase_mismatch')

    def test_repeat_request_is_rejected_and_state_file_is_private(self):
        with tempfile.TemporaryDirectory(dir='/var/tmp/opencode/p8-remote-step1') as directory:
            root = Path(directory); root.chmod(0o700)
            supervisor.initialize(root, experiment='E1', arm='baseline', seed=7,
                                  deadline_seconds=10, receipts=[])
            self.assertEqual(os.stat(root / 'session.json').st_mode & 0o777, 0o600)
            supervisor.write_request(root, {'session_id': supervisor.read_state(root)['session_id'],
                'command': 'finalize', 'phase': 'READY'})
            supervisor.hold(root, poll_seconds=0)
            with self.assertRaisesRegex(ValueError, 'session_not_ready'):
                supervisor.hold(root, poll_seconds=0)

    def test_invalid_transition_and_expired_deadline_close_cleanly(self):
        with tempfile.TemporaryDirectory(dir='/var/tmp/opencode/p8-remote-step1') as directory:
            root = Path(directory); root.chmod(0o700)
            state = supervisor.initialize(root, experiment='E1', arm='baseline', seed=7,
                                           deadline_seconds=0, receipts=[])
            self.assertEqual(supervisor.hold(root, poll_seconds=0)['phase'], 'FINALIZING')
            final = supervisor.finalize(root, supervisor.read_state(root), cleanup_verified=False)
            self.assertEqual(final['workload_outcome'], 'DEADLINE')
            self.assertEqual(final['cleanup_outcome'], 'FAILED')

    def test_signal_closes_state_and_restores_previous_handler(self):
        with tempfile.TemporaryDirectory(dir='/var/tmp/opencode/p8-remote-step1') as directory:
            root = Path(directory); root.chmod(0o700)
            supervisor.initialize(root, experiment='E1', arm='baseline', seed=7,
                                  deadline_seconds=10, receipts=[])
            old = signal.getsignal(signal.SIGTERM)
            def deliver(_seconds):
                signal.getsignal(signal.SIGTERM)(signal.SIGTERM, None)
            with mock.patch.object(supervisor.time, 'sleep', side_effect=deliver):
                state = supervisor.hold(root, poll_seconds=0.01)
            self.assertEqual(state['workload_outcome'], 'INTERRUPTED')
            self.assertIs(signal.getsignal(signal.SIGTERM), old)

    def test_exclusive_supervisor_lock_and_atomic_pending_request(self):
        root = self.private_root()
        first = supervisor.acquire_lock(root)
        self.addCleanup(first.close)
        with self.assertRaisesRegex(ValueError, 'session_active'):
            supervisor.acquire_lock(root)
        supervisor.write_request(root, {'session_id': 's', 'command': 'finalize', 'phase': 'READY'})
        with self.assertRaisesRegex(ValueError, 'request_pending'):
            supervisor.write_request(root, {'session_id': 'other'})
        self.assertTrue((root / 'request.json').read_text().endswith('\n'))

    def test_begin_collects_before_ack_finish_imports_then_finalize_preserves_outcome(self):
        root = self.private_root()
        state = supervisor.initialize(root, experiment='E1', arm='baseline', seed=7,
            deadline_seconds=10, receipts=[], preparation_status='E1_PREPARED')
        supervisor.write_request(root, {'session_id':state['session_id'], 'command':'begin',
            'phase':'READY', 'phase_id':'pilot'})
        events=[]
        def begin(request):
            events.append(('collector_started', supervisor.read_state(root)['phase']))
            return {'baseline':'captured'}
        def finish(request):
            events.append(('result_imported', request['phase_id']))
            return {'workload_outcome':'VALIDATED','validation_valid':True}
        def drive(_seconds):
            current=supervisor.read_state(root)
            if current['phase']=='MEASURING' and not (root/'request.json').exists():
                supervisor.write_request(root, {'session_id':state['session_id'], 'command':'finish-phase',
                    'phase':'MEASURING', 'phase_id':'pilot', 'results':'/owned/received/results'})
            elif current['phase']=='VERIFYING' and not (root/'request.json').exists():
                supervisor.write_request(root, {'session_id':state['session_id'], 'command':'finalize',
                    'phase':'VERIFYING'})
        with mock.patch.object(supervisor.time, 'sleep', side_effect=drive):
            finalizing=supervisor.hold(root, poll_seconds=0.01, on_begin=begin, on_finish=finish)
        self.assertEqual(events, [('collector_started','READY'),('result_imported','pilot')])
        self.assertEqual(finalizing['phase'], 'FINALIZING')
        self.assertEqual(finalizing['workload_outcome'], 'VALIDATED')
        self.assertTrue(finalizing['collector_ready'])

    def test_import_failure_leaves_measurement_open_for_abort(self):
        root=self.private_root()
        state=supervisor.initialize(root,experiment='E1',arm='baseline',seed=7,
            deadline_seconds=10,receipts=[],preparation_status='E1_PREPARED')
        supervisor.write_request(root,{'session_id':state['session_id'],'command':'begin',
            'phase':'READY','phase_id':'pilot'})
        submitted=[False]
        def drive(_):
            current=supervisor.read_state(root)
            if current['phase']=='MEASURING' and not submitted[0]:
                submitted[0]=True
                supervisor.write_request(root,{'session_id':state['session_id'],
                    'command':'finish-phase','phase':'MEASURING','phase_id':'pilot',
                    'results':'/invalid/result-root'})
            elif current['phase']=='MEASURING' and submitted[0] and not (root/'request.json').exists():
                supervisor.write_request(root,{'session_id':state['session_id'],
                    'command':'abort','phase':'MEASURING'})
        with mock.patch.object(supervisor.time,'sleep',side_effect=drive):
            result=supervisor.hold(root,poll_seconds=0.01,on_begin=lambda _: {},
                on_finish=lambda _: (_ for _ in ()).throw(ValueError('result_bundle_correlation_rejected')))
        self.assertEqual(result['workload_outcome'],'ABORTED')
        self.assertEqual(result['last_request_error'],'result_bundle_correlation_rejected')
        self.assertEqual(result['phase'],'FINALIZING')

    def test_hold_can_read_completed_request_while_publisher_temp_link_still_exists(self):
        root = self.private_root()
        state = supervisor.initialize(root, experiment='E1', arm='baseline', seed=7,
                                       deadline_seconds=10, receipts=[])
        real_link = supervisor.os.link
        observed = []
        def publish_then_read(source, destination, **kwargs):
            real_link(source, destination, **kwargs)
            observed.append(supervisor._consume_request(root, state))
        with mock.patch.object(supervisor.os, 'link', side_effect=publish_then_read):
            supervisor.write_request(root, {'session_id': state['session_id'],
                'command': 'finalize', 'phase': 'READY'})
        self.assertEqual([item['command'] for item in observed], ['finalize'])

    def test_invalid_request_is_consumed_and_recorded_without_ending_hold(self):
        root = self.private_root()
        supervisor.initialize(root, experiment='E1', arm='baseline', seed=7,
                              deadline_seconds=10, receipts=[])
        state = supervisor.read_state(root)
        supervisor.write_request(root, {'session_id': state['session_id'],
            'command': 'not-a-command', 'phase': 'READY'})
        # A valid stop follows the rejected request; invalid input must not kill the supervisor.
        def after_rejection(_seconds):
            if not (root / 'request.json').exists():
                supervisor.write_request(root, {'session_id': state['session_id'],
                    'command': 'abort', 'phase': 'READY'})
        with mock.patch.object(supervisor.time, 'sleep', side_effect=after_rejection):
            finalizing = supervisor.hold(root, poll_seconds=0.01)
        self.assertEqual(finalizing['phase'], 'FINALIZING')
        self.assertEqual(finalizing['last_request_error'], 'request_command_rejected')
        self.assertEqual(finalizing['workload_outcome'], 'ABORTED')

    def test_request_bounds_and_incomplete_or_nonregular_file_fail_closed(self):
        root = self.private_root()
        with self.assertRaisesRegex(ValueError, 'request_contract_rejected'):
            supervisor.write_request(root, [])
        with self.assertRaisesRegex(ValueError, 'request_size_rejected'):
            supervisor.write_request(root, {'padding': 'x' * supervisor.MAX_REQUEST_BYTES})
        supervisor.initialize(root, experiment='E1', arm='baseline', seed=7,
                              deadline_seconds=10, receipts=[])
        state = supervisor.read_state(root)
        (root / 'request.json').write_text('{')
        (root / 'request.json').chmod(0o600)
        def stop_after_rejection(_seconds):
            if not (root / 'request.json').exists():
                supervisor.write_request(root, {'session_id': state['session_id'],
                    'command': 'finalize', 'phase': 'READY'})
        with mock.patch.object(supervisor.time, 'sleep', side_effect=stop_after_rejection):
            finalizing = supervisor.hold(root, poll_seconds=0.01)
        self.assertEqual(finalizing['last_request_error'], 'request_file_rejected')
        self.assertEqual(finalizing['phase'], 'FINALIZING')

    def test_initialize_rejects_nonfinite_deadline_and_duplicate_state(self):
        root = self.private_root()
        with self.assertRaisesRegex(ValueError, 'session_metadata_rejected'):
            supervisor.initialize(root, experiment='E1', arm='baseline', seed=7,
                                  deadline_seconds=float('nan'), receipts=[])
        supervisor.initialize(root, experiment='E1', arm='baseline', seed=7,
                              deadline_seconds=10, receipts=[])
        with self.assertRaisesRegex(ValueError, 'session_already_initialized'):
            supervisor.initialize(root, experiment='E1', arm='baseline', seed=7,
                                  deadline_seconds=10, receipts=[])

    def test_stale_recovery_uses_immutable_receipt_refuses_replacement_and_is_idempotent(self):
        root = self.private_root()
        project = 'ncs_0123456789ab'
        labels = {'test.task': 'native-compose-allocation', 'test.run': project,
                  'bootstrap.managed': 'true'}
        item = {'kind': 'volume', 'name': project + '_data', 'image': None, 'labels': labels,
                'task': 'native-compose-allocation', 'run': project, 'nonce': '0123',
                'receipt': {'id': project + '_data', 'created': '2026-10-30T00:00:00Z', 'labels': labels},
                'allocation_attempted': True}
        supervisor.initialize(root, experiment='E1', arm='baseline', seed=7,
                              deadline_seconds=10, receipts=[{key: item[key] for key in ('kind', 'name', 'receipt')}], project=project)
        supervisor._atomic_json(root / 'resource-0001.json', item)
        active = supervisor.acquire_lock(root)
        try:
            with self.assertRaisesRegex(ValueError, 'session_active'):
                supervisor.recover_cleanup(root, project=project, runner=mock.Mock())
        finally:
            active.close()

        class DockerRunner:
            def __init__(self, replaced=False):
                self.exists = True
                self.replaced = replaced
            def __call__(self, command, **kwargs):
                if command[1:3] == ['volume', 'inspect']:
                    if not self.exists:
                        return mock.Mock(returncode=1, stdout=b'', stderr=b'No such volume')
                    obj_labels = dict(labels)
                    if self.replaced:
                        obj_labels['test.run'] = 'replacement'
                    row = {'Name': item['name'], 'CreatedAt': item['receipt']['created'],
                           'Labels': obj_labels, 'Driver': 'local', 'Options': None}
                    return mock.Mock(returncode=0, stdout=json.dumps([row]).encode(), stderr=b'')
                if command[1:3] == ['volume', 'rm']:
                    self.exists = False
                    return mock.Mock(returncode=0, stdout=b'', stderr=b'')
                raise AssertionError(command)

        replaced = DockerRunner(replaced=True)
        with self.assertRaisesRegex(ValueError, 'resource_ownership_refused'):
            supervisor.recover_cleanup(root, project=project, runner=replaced)
        self.assertEqual(supervisor.read_state(root)['cleanup_outcome'], 'FAILED')
        state = supervisor.read_state(root)
        state.update(phase='CLOSED', cleanup_outcome='VERIFIED', workload_outcome='NOT_RUN')
        supervisor._atomic_json(root / 'session.json', state)
        owned = DockerRunner()
        self.assertTrue(supervisor.recover_cleanup(root, project=project, runner=owned)['cleanup_verified'])
        self.assertEqual(supervisor.read_state(root)['cleanup_outcome'], 'VERIFIED')
        self.assertEqual(supervisor.read_state(root)['workload_outcome'], 'NOT_RUN')
        self.assertTrue(supervisor.recover_cleanup(root, project=project, runner=owned)['cleanup_verified'])
        self.assertEqual(supervisor.read_state(root)['workload_outcome'], 'NOT_RUN')

    def test_recovery_refuses_resource_without_immutable_receipt(self):
        root = self.private_root()
        project = 'ncs_0123456789ab'
        supervisor.initialize(root, experiment='E1', arm='baseline', seed=7,
                              deadline_seconds=10, receipts=[], project=project)
        supervisor._atomic_json(root / 'resource-0001.json', {
            'kind': 'container', 'name': project + '-backend-1', 'task': 'native-compose-allocation',
            'run': project, 'labels': {}, 'allocation_attempted': True, 'receipt': None})
        with self.assertRaisesRegex(ValueError, 'recovery_receipt_incomplete'):
            supervisor.recover_cleanup(root, project=project, runner=mock.Mock())

    def test_initialize_stores_and_reads_e2_workload(self):
        root = self.private_root()
        state = supervisor.initialize(root, experiment='E2', arm='E2-pilot', seed=84,
                                      deadline_seconds=1800, receipts=[],
                                      workload='equal_samples')
        self.assertEqual(state.get('workload'), 'equal_samples')
        read = supervisor.read_state(root)
        self.assertEqual(read.get('workload'), 'equal_samples')

        root2 = self.private_root()
        with self.assertRaisesRegex(ValueError, 'session_metadata_rejected'):
            supervisor.initialize(root2, experiment='E2', arm='E2-pilot', seed=84,
                                  deadline_seconds=1800, receipts=[],
                                  workload='invalid workload!')



if __name__ == '__main__':
    unittest.main()
