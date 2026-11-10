"""E5 staged per-arm input preparation and runtime fixture."""
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import traceback
import time
import uuid
import stat
from datetime import datetime, timezone

_HERE = Path(__file__).resolve().parent
for _path in (str(_HERE), str(_HERE.parent)):
    if _path not in sys.path:
        sys.path.insert(0, _path)

import paper_p8_e3_support as e3
import paper_p8_e5_support as e5
import paper_p8_e4_support as e4
import paper_p8_support as p8_support
from paper_p8 import GATEWAY_ENV_KEYS
from stage3_uplink_runner import gateway_env
from paper_p8_e3_fixture import (
    _executor_environment, _inspect_executor_sqlite, _verify_control_receipts,
    _stop_collector_and_process,
)


def _validate_reference(smoke, path, context):
    if not path:
        raise ValueError('e5_reference_required')
    artifact_path = Path(path)
    try:
        st = artifact_path.lstat()
        if (artifact_path.is_symlink() or not artifact_path.is_file() or st.st_uid != os.getuid()
                or stat.S_IMODE(st.st_mode) != 0o600 or st.st_size > 2_000_000):
            raise ValueError('e5_reference_file_rejected')
        report = json.loads(artifact_path.read_text(encoding='utf-8'))
    except (OSError, json.JSONDecodeError):
        raise ValueError('e5_reference_artifact_invalid') from None
    if (report.get('status') != 'PASS' or report.get('arm') != 'optimized'
            or report.get('cleanup_verified') is not True or report.get('full_p8_accepted') is not False
            or not smoke.paper_p8_e5_acceptance(report, {'arm': 'optimized'}, True)):
        raise ValueError('e5_reference_artifact_rejected')
    old_source, source = report.get('source_fingerprint'), e4._support._source_fingerprint(smoke.ROOT)
    if (not isinstance(old_source, dict) or not old_source.get('revision') or not old_source.get('dirty_source_sha256')
            or old_source.get('revision') != source.get('revision')
            or old_source.get('dirty_source_sha256') != source.get('dirty_source_sha256')):
        raise ValueError('e5_reference_source_mismatch')
    old_context = report.get('context')
    metadata = report.get('metadata') if isinstance(report.get('metadata'), dict) else {}
    workload = metadata.get('workload') if isinstance(metadata.get('workload'), dict) else {}
    context_matches = (all(old_context.get(k) == context.get(k) for k in
        ('sha256', 'seed', 'anchor', 'workload_selection_sha256'))
        if isinstance(old_context, dict) else workload.get('journal_sha256') == context.get('sha256')
        and metadata.get('seed') == context.get('seed')
        and (workload.get('telemetry') or {}).get('dataset_anchor') == context.get('anchor'))
    if not context_matches:
        raise ValueError('e5_reference_context_mismatch')
    ids = report.get('image_ids') or (report.get('metadata') or {}).get('image_ids')
    if not isinstance(ids, dict) or any(not isinstance(ids.get(k), str)
            or not re.fullmatch(r'sha256:[0-9a-f]{64}', ids[k]) for k in ('backend', 'gateway')):
        raise ValueError('e5_reference_images_invalid')
    return {'image_ids': ids, 'report': report}


def run(smoke, fixture, endpoint, registry, containers, images, state,
        paperload_binary, gateway_binary, arm, dataset_context_path, reference_path=None,
        recovery_context_out=None):
    """Execute one staged E5 arm. Runtime output remains private until fixture cleanup."""
    if arm not in e5.ARMS:
        raise ValueError('e5_arm_rejected')
    # The repeat context is shared read-only by all arms.  All mutable runtime
    # state belongs to this arm's private fixture directory.
    root = Path(fixture.private) / 'p8-e5'
    root.mkdir(mode=0o700, exist_ok=False)
    root.chmod(0o700)
    root = _private_root(root)
    report = {'status': 'FAIL', 'experiment': 'E5', 'arm': arm,
              'full_p8_accepted': False, 'primary_error': None, 'cleanup_errors': [],
              'validation': {'valid': False, 'failures': []}, 'processes_stopped': False,
              'runner_stopped': False, 'actual_stop_confirmed': False,
              'generated_at': datetime.now(timezone.utc).isoformat()}
    procs, collectors, viewer_proc = [], [], None
    collector_pids = {}
    stopped_collectors = set()
    current_phase = 'preflight'
    try:
        # The context is small, private, non-symlinked, and binds the common workload.
        context_path = Path(dataset_context_path)
        st = context_path.lstat()
        if context_path.is_symlink() or not context_path.is_file() or st.st_uid != os.getuid() or st.st_mode & 0o077 or st.st_size > 16384:
            raise ValueError('e5_dataset_context_not_private')
        context = json.loads(context_path.read_text(encoding='utf-8'))
        if arm != 'optimized':
            reference = _validate_reference(smoke, reference_path, context)
            if any(images.get(k) != reference.get('image_ids', {}).get(k) for k in ('backend', 'gateway')):
                raise ValueError('e5_reference_image_mismatch')
        required = ('dataset_path', 'sha256', 'bytes', 'seed', 'anchor', 'sensor_ids',
                    'gateway_ids', 'command_keys', 'viewer_subscriptions', 'workload_selection',
                    'workload_selection_sha256')
        if not isinstance(context, dict) or any(k not in context for k in required):
            raise ValueError('e5_dataset_context_invalid')
        dataset = Path(context['dataset_path'])
        ds = dataset.lstat()
        raw = dataset.read_bytes()
        if dataset.is_symlink() or not dataset.is_file() or ds.st_uid != os.getuid() or ds.st_mode & 0o077 or len(raw) > e5.MAX_JOURNAL_BYTES or len(raw) != context['bytes'] or hashlib.sha256(raw).hexdigest() != context['sha256']:
            raise ValueError('e5_dataset_context_hash_mismatch')
        pg, quota, effective, resources, storage, tokens, specs, passwords, _ = smoke.prepare_paper_workload(
            fixture, registry, endpoint, containers, images, state, gateway_ids=list(GATEWAYS))
        expected = e5.e5_arm_profile(arm)
        services = effective.get('services') if isinstance(effective, dict) else None
        backend = services.get('backend') if isinstance(services, dict) else None
        actual_env = backend.get('environment') if isinstance(backend, dict) else None
        if not isinstance(actual_env, dict) or any(str(actual_env.get(k, '')).lower() != str(v).lower() for k, v in expected.items()):
            raise ValueError('e5_effective_arm_profile_mismatch')
        schema = smoke.db_psql(registry, pg,
            "SELECT version::text || ':' || name FROM schema_migrations ORDER BY version DESC LIMIT 1;").strip()
        if schema not in ('26:drop_telemetry_redundant_indexes', '25:drop_telemetry_sensor_fk', '24:telemetry_ledger_timestamp'):
            raise ValueError('p8_schema_terminal_rejected')
        broker = registry.resolve(containers['mosquitto'])
        mappings = broker.get('NetworkSettings', {}).get('Ports', {}).get('8883/tcp') or []
        if len(mappings) != 1 or not mappings[0].get('HostPort'):
            raise ValueError('e5_mqtt_port_readback_rejected')
        p8_support.validate_mqtt_readback({'service': 'mosquitto',
            'host_ip': mappings[0].get('HostIp'), 'target': 8883,
            'published': int(mappings[0]['HostPort'])}, owned_service='mosquitto')
        journal_context = {'sha256': context['sha256'], 'bytes': context['bytes'],
            'gateway_ids': context['gateway_ids'], 'sensor_ids': context['sensor_ids'],
            'seed': context['seed'], 'anchor': context['anchor'],
            'workload_selection_sha256': context['workload_selection_sha256']}
        mqtt_broker_url = f"ssl://127.0.0.1:{mappings[0]['HostPort']}"
        mqtt_manifest_endpoint = f"mqtts://127.0.0.1:{mappings[0]['HostPort']}"
        prepared = prepare_inputs(smoke=smoke, fixture=fixture, endpoint=endpoint,
            specs=specs, passwords=passwords, tokens=tokens, mqtt_broker_url=mqtt_broker_url,
            mqtt_manifest_endpoint=mqtt_manifest_endpoint,
            mqtt_ca_file=fixture.private / 'certs/ca.crt', journal_path=dataset,
            journal_context=journal_context, seed=context['seed'], anchor=context['anchor'], root=root,
            command_keys=context['command_keys'], viewer_subscriptions=context['viewer_subscriptions'],
            workload_selection=context['workload_selection'],
            recovery_mode=recovery_context_out is not None)
        if recovery_context_out is not None:
            if not isinstance(recovery_context_out, dict) or recovery_context_out:
                raise ValueError('e5_recovery_context_invalid')
            # Deliberately memory-only: no token/password/spec payload is written to artifacts.
            recovery_context_out.update({'tokens': tokens, 'specs': specs, 'prepared': prepared,
                'context': context, 'root': root, 'pg': pg, 'sqlite_paths': [
                    Path(json.loads(Path(row['env_file']).read_text())['GATEWAY_OUTBOX_PATH'])
                    for row in json.loads(Path(prepared['manifests']['run_spec']).read_text()).get('gateways', [])]
                    + ([Path(prepared['manifests']['recovery_aux_db_path'])]
                       if 'recovery_aux_db_path' in prepared['manifests'] else []),
                'recovery_aux_env_file': prepared['manifests']['recovery_aux_env_file'],
                'recovery_aux_db_path': prepared['manifests']['recovery_aux_db_path'],
                'recovery_aux_sensor_id': RECOVERY_AUX_SENSOR})
        # Start actual, preflighted viewers before taking any telemetry baseline.
        viewer_out = root / 'viewer-output'
        viewer_log = root / 'viewer.log'
        viewer_proc = e4.start_e4_viewer(paperload_binary=paperload_binary,
            manifest_path=prepared['manifests']['viewer'], output_dir=viewer_out,
            log_path=viewer_log, expected_viewers=2)
        admin_before = e4._support._admin_metrics(smoke, endpoint, tokens['admin'])
        phases, windows, resources_by_phase, histograms = {}, {}, {}, {}
        phase_db_before = {}
        initial_metrics_before = None
        initial_metrics_after = None
        runtime_logs = root / 'logs'
        runtime_logs.mkdir(mode=0o700)
        for phase, key in (('initial', 'initial'), ('warm_replay', 'warm_replay')):
            current_phase = phase
            manifest = prepared['manifests'][key]
            baseline = e4._support._admin_metrics(smoke, endpoint, tokens['admin'])
            if phase == 'initial': initial_metrics_before = baseline
            phase_db_before[key] = [e4._counts(smoke, registry, pg, s) for s in specs]
            server_base = smoke.paper_p0_stats(registry, containers, 'e5_' + phase + '_start')
            collector = e4.BoundedCollector(scrape=lambda: e4._support._admin_metrics(smoke, endpoint, tokens['admin']),
                interval=.5, max_snapshots=16, deadline_seconds=30,
                baseline_metrics=baseline, baseline_server=server_base)
            collectors.append(collector)
            out = root / (phase + '-output')
            log = runtime_logs / f'p810c-runtime-{fixture.project}-{arm}-{phase}.log'
            with log.open('xb') as stream:
                log.chmod(0o600)
                proc = subprocess.Popen(['python3', str(smoke.ROOT / 'scripts/paper_p8.py'), '--run',
                    '--binary', str(paperload_binary), '--manifest', str(manifest), '--output-dir', str(out),
                    '--run-artifact', str(root / (phase + '-run.json'))], cwd=smoke.ROOT,
                    stdout=stream, stderr=subprocess.STDOUT, env=smoke.process_env(fixture), close_fds=True)
                procs.append(proc); collector.start(proc.pid); collector_pids[collector] = proc.pid
                try:
                    proc.wait(timeout=45)
                finally:
                    collector_error, cleanup_failure = _stop_collector_and_process(collector, proc)
                    stopped_collectors.add(collector)
                    if collector_error:
                        report['cleanup_errors'].append('collector_' + type(collector_error).__name__)
                    if cleanup_failure:
                        report['cleanup_errors'].append('process_' + type(cleanup_failure).__name__)
            if proc.returncode != 0:
                raise ValueError('e5_runner_nonzero_' + phase)
            result = json.loads((out / 'workload.json').read_text(encoding='utf-8'))
            if result.get('status') != 'COMPLETE' or result.get('cleanup_errors'):
                raise ValueError('e5_workload_incomplete_' + phase)
            after = e4._support._admin_metrics(smoke, endpoint, tokens['admin'])
            if phase == 'initial': initial_metrics_after = after
            db = [e4._counts(smoke, registry, pg, s) for s in specs]
            phase_before = phase_db_before[key]
            delta = e4._database_delta(phase_before, db)
            phases[phase] = e4._safe_result(result)
            phases[phase]['database_counts'] = db
            phases[phase]['database_delta'] = delta
            phases[phase]['generator_samples'] = list(collector.generator_samples)
            windows[phase] = {'admin_before': baseline, 'admin_after': after,
                'ingress_delta': e4._support.calculate_ingress_delta(baseline, after)}
            resources_by_phase[phase] = collector.resources + collector.generator_samples
            before_durations = e4._ingress_duration_snapshot(baseline)
            after_durations = e4._ingress_duration_snapshot(after)
            phase_histograms = {}
            for name, value in (after_durations.get('ingress') or {}).items():
                if isinstance(value, dict) and 'buckets' in value:
                    phase_histograms['ingress.' + name] = e4._support.metrics_window(
                        before_durations, after_durations, 'ingress', name,
                        smoke.paper_p1_histogram_window)
            histograms[phase] = phase_histograms or {
                'status': 'INCONCLUSIVE', 'reason': 'ingress_duration_histograms_missing'}
            if collector.errors:
                raise ValueError('e5_collector_incomplete_' + phase)
        # Control runner is deliberately after replay ACK baseline; E3 owns eligibility and receipt checks.
        current_phase = 'control'
        from paper_p8_e3_fixture import _eligible_post_stop_drain, _drain_executor_outboxes
        spec = json.loads(Path(prepared['manifests']['run_spec']).read_text())
        commands = json.loads(Path(prepared['manifests']['control']).read_text()).get('commands', [])
        declared_keys = context['command_keys']
        if [c.get('idempotency_key') for c in commands] != declared_keys:
            raise ValueError('e5_command_manifest_key_mismatch')
        for key in declared_keys:
            if smoke.db_psql(registry, pg, "SELECT count(*) FROM twin_command_requests WHERE idempotency_key=:'key';", {'key': key}).strip() != '0':
                raise ValueError('e5_command_key_not_fresh')
        control_log = runtime_logs / f'p810c-runtime-{fixture.project}-{arm}-control.log'
        control_run = root / 'control-run.json'
        control_metrics_before = e4._support._admin_metrics(smoke, endpoint, tokens['admin'])
        with control_log.open('xb') as stream:
            control_log.chmod(0o600)
            cp = subprocess.Popen(['python3', str(smoke.ROOT / 'scripts/paper_p8.py'), '--run', '--binary', str(paperload_binary),
                '--gateway-binary', str(gateway_binary),
                '--run-spec', prepared['manifests']['run_spec'], '--run-artifact', str(control_run)], cwd=smoke.ROOT,
                stdout=stream, stderr=subprocess.STDOUT, env=smoke.process_env(fixture), close_fds=True)
            procs.append(cp)
            try: cp.wait(timeout=45)
            except subprocess.TimeoutExpired: raise ValueError('e5_control_runner_timeout') from None
        artifact = json.loads(control_run.read_text(encoding='utf-8'))
        if cp.returncode != 0 and artifact.get('status') != 'PARTIAL':
            raise ValueError('e5_control_runner_nonzero')
        report['control_run'] = artifact
        if artifact.get('status') == 'PARTIAL':
            output_paths = [root / row['output_dir'] / 'workload.json'
                            for row in spec.get('paperload', []) if row.get('role') == 'control']
            outputs = []
            try:
                outputs = [json.loads(path.read_text(encoding='utf-8')) for path in output_paths]
            except (OSError, ValueError, json.JSONDecodeError):
                outputs = []
            env_paths, db_paths, sqlite = [], [], []
            for row in spec.get('gateways', []):
                env_path = Path(row['env_file'])
                executor_env = json.loads(env_path.read_text(encoding='utf-8'))
                env_paths.append(env_path)
                db_paths.append(Path(executor_env['GATEWAY_OUTBOX_PATH']))
                sqlite.append(_inspect_executor_sqlite(db_paths[-1]))
            if not _eligible_post_stop_drain(artifact, outputs, sqlite):
                raise ValueError('e5_partial_control_not_drain_eligible')
            drain = _drain_executor_outboxes(smoke, fixture, registry, pg, gateway_binary,
                [str(p) for p in env_paths], db_paths)
            if not drain or any(row.get('pending_after') for row in drain):
                raise ValueError('e5_post_stop_drain_not_verified')
            report['post_stop_drain'] = drain
        if artifact.get('status') not in ('COMPLETE', 'PARTIAL'):
            raise ValueError('e5_control_run_status_rejected')
        if artifact.get('status') == 'PARTIAL' and not report.get('post_stop_drain'):
            raise ValueError('e5_control_partial_not_recovered')
        control_metrics_after = e4._support._admin_metrics(smoke, endpoint, tokens['admin'])
        control_outputs = [json.loads((root / row['output_dir'] / 'workload.json').read_text())
                           for row in spec.get('paperload', []) if row.get('role') == 'control']
        if len(control_outputs) != 1 or control_outputs[0].get('status') != 'COMPLETE':
            raise ValueError('e5_control_workload_incomplete')
        actual_commands = control_outputs[0].get('commands')
        if (not isinstance(actual_commands, list) or len(actual_commands) != 4
                or any(not isinstance(row, dict) or row.get('classification') != 'succeeded'
                       or row.get('idempotency_key') not in declared_keys
                       for row in actual_commands)
                or {row.get('idempotency_key') for row in actual_commands} != set(declared_keys)):
            raise ValueError('e5_control_command_classification_invalid')
        pg_receipts, rest_configs, db_paths = {}, {}, []
        for key, command in zip(declared_keys, commands):
            sid = command['sensor_id']; urn = f'urn:ngsi-ld:Sensor:{GATEWAYS[0]}:{sid}'
            sql = """SELECT json_build_object('idempotency_key',r.idempotency_key,'status_code',r.status_code,
                'command_id',r.command_id::text,'status',c.status,'result_hash',c.result_hash,
                'desired_version',c.desired_version::text,'sample_interval_us',(c.parameters->>'sample_interval_us')::bigint)::text
                FROM twin_command_requests r JOIN twin_commands c ON c.command_id=r.command_id
                JOIN twin_entities e ON e.id=c.entity_id WHERE r.idempotency_key=:'key' AND e.entity_id=:'urn';"""
            text = smoke.db_psql(registry, pg, sql, {'key': key, 'urn': urn}).strip()
            if not text: raise ValueError('e5_command_receipt_missing')
            receipt = json.loads(text)
            if receipt.get('status_code') != 202 or receipt.get('status') != 'succeeded' or not receipt.get('result_hash'):
                raise ValueError('e5_command_receipt_invalid')
            pg_receipts[key] = receipt
        for row in spec.get('gateways', []):
            env_data = json.loads(Path(row['env_file']).read_text())
            db_paths.append(Path(env_data['GATEWAY_OUTBOX_PATH']))
        sqlite_summaries = [_inspect_executor_sqlite(path) for path in db_paths]
        for sid in COMMAND_SENSORS:
            urn = f'urn:ngsi-ld:Sensor:{GATEWAYS[0]}:{sid}'
            status, _, body = smoke.http_request(endpoint, 'GET', f'/v1/digital-twins/{urn}?format=simplified', token=tokens['a'])
            if status != 200 or not isinstance(body, dict): raise ValueError('e5_command_rest_read_failed')
            rest_configs[sid] = (body.get('reported_state') or {}).get('configuration')
        if not _verify_control_receipts(sqlite_summaries, commands, pg_receipts, rest_configs, COMMAND_SENSORS):
            raise ValueError('e5_command_receipts_unverified')
        current_phase = 'viewer'
        before_raw = [e4._counts(smoke, registry, pg, s) for s in specs]
        # Capture the fanout activation window with both viewers alive; command aux sensors are excluded.
        admin_after_control = e4._support._admin_metrics(smoke, endpoint, tokens['admin'])
        viewer_result = e4.finish_e4_viewer(viewer_proc, viewer_out, timeout=50); viewer_proc = None
        viewer_end_metrics = e4._support._admin_metrics(smoke, endpoint, tokens['admin'])
        after_raw = [e4._counts(smoke, registry, pg, s) for s in specs]
        raw_ids = [r.get('message_id') for r in phases['initial'].get('e4', {}).get('safe_header_samples', []) if isinstance(r, dict)]
        raw_proof = [e4.verify_pilot_raw(smoke, registry, pg, s, message_ids=raw_ids) for s in specs]
        mechanisms = e5.adapt_mechanism_counters(initial_metrics_before, initial_metrics_after, arm=arm,
            viewer_readiness=[True, True])
        warm_before = windows['warm_replay']['admin_before']
        warm_after = windows['warm_replay']['admin_after']
        mechanisms['ram_dedup'] = e5.adapt_mechanism_counters(warm_before, warm_after,
            arm=arm)['ram_dedup']
        mechanisms['command'] = e5.adapt_mechanism_counters(control_metrics_before, control_metrics_after,
            arm=arm, verified_commands=len(declared_keys))['command']
        command_timeline = e5.control_timeline_window(control_metrics_before, control_metrics_after,
            expected_commands=len(declared_keys))
        mechanisms['command']['timeline_window'] = command_timeline
        mechanisms['fanout']['window'] = e4._e4_ws_window(initial_metrics_before, initial_metrics_after)
        mechanisms['command']['window'] = e4._e4_ws_window(control_metrics_before, control_metrics_after)
        mechanisms['activation_status'] = 'AVAILABLE' if all(isinstance(mechanisms.get(k), dict)
            and 'status' not in mechanisms[k] for k in ('microbatch','ram_dedup','command','fanout','shaper')) else 'INCONCLUSIVE'
        validation_failures = []
        selection = context['workload_selection']
        unique_count = selection['telemetry_messages']
        warm_count = selection['warm_replay_prefix']
        if len(raw_ids) != unique_count: validation_failures.append('initial_header_count')
        for phase_name, expected_count in (('initial', selection['initial_replay_count']), ('warm_replay', warm_count)):
            workload = phases[phase_name].get('e4') or {}
            telemetry = phases[phase_name].get('telemetry') or {}
            if not e5.replay_accounting_matches(workload, telemetry, expected_count):
                validation_failures.append(phase_name + '_workload_accounting')
        if (phases['warm_replay'].get('database_counts') != phases['initial'].get('database_counts')
                or any(before.get('fingerprint') != after.get('fingerprint')
                       for before, after in zip(phases['initial']['database_counts'], phases['warm_replay']['database_counts']))):
            validation_failures.append('warm_replay_changed_main_fingerprint')
        initial_delta = phases['initial'].get('database_delta', {})
        if any(initial_delta.get(k) != v for k, v in {'ledger':unique_count,
                'raw':unique_count * selection['samples_per_message'],
                'temporal':unique_count,'configuration':0}.items()):
            validation_failures.append('initial_database_delta')
        if any(row.get('verified_exact') is not True for row in raw_proof): validation_failures.append('main_raw_fingerprint')
        if before_raw != after_raw: validation_failures.append('main_raw_fingerprint_changed')
        viewer_validation = _validate_e5_viewer(viewer_result, context, phases['initial'],
            e4._e4_ws_window(initial_metrics_before, initial_metrics_after),
            e4._e4_ws_window(windows['warm_replay']['admin_before'], windows['warm_replay']['admin_after']),
            viewer_end_metrics)
        validation_failures.extend(viewer_validation['failures'])
        current_phase = 'validation'
        source_fingerprint = e4._support._source_fingerprint(smoke.ROOT)
        if not isinstance(source_fingerprint, dict) or not source_fingerprint.get('valid', True):
            validation_failures.append('source_fingerprint_invalid')
        validation = {'valid': not validation_failures, 'failures': validation_failures,
             'main_raw_fingerprint_stable': before_raw == after_raw, 'viewer_status': viewer_result.get('status'),
             'mechanism_eligibility': mechanisms.get('activation_status'),
             'viewer_validation': viewer_validation}
        report.update({'status': 'WORKLOAD_COMPLETE_PENDING_CLEANUP' if validation['valid'] else 'FAIL',
            'lane_marker': 'PAPER_P8_E5_ARM_PILOT',
            'validation': validation, 'schema': schema, 'quota': quota, 'storage_status': storage,
            'effective_flags': {k: str(actual_env[k]).lower() for k in expected}, 'resources': {'fixture': resources, 'phases': resources_by_phase},
            'histogram_windows': histograms, 'phase_windows': windows, 'phases': phases,
            'mechanisms': mechanisms, 'viewer': {'ready_servers': 2, 'subscriptions': len(context['viewer_subscriptions']), 'sockets': 2,
                'result': _safe_e5_viewer(viewer_result), 'validation': viewer_validation}, 'raw_verification': raw_proof,
            'main_raw_before': before_raw, 'main_raw_after': after_raw, 'source_revision': smoke.paper_p0_git(fixture, 'rev-parse', 'HEAD'),
            'source_fingerprint': source_fingerprint,
            'paperload_binary_sha256': hashlib.sha256(Path(paperload_binary).read_bytes()).hexdigest(),
            'gateway_binary_sha256': hashlib.sha256(Path(gateway_binary).read_bytes()).hexdigest(),
            'image_ids': images, 'workload': prepared['workload'], 'workload_sha256': prepared['workload_sha256'],
             'journal_sha256': prepared['journal_sha256'], 'manifest_paths_private': prepared['manifests'],
             'context': {'sha256': context['sha256'], 'seed': context['seed'], 'anchor': context['anchor'],
                'workload_selection_sha256': context['workload_selection_sha256']},
            'runner_logs_private': [str(runtime_logs / f'p810c-runtime-{fixture.project}-{arm}-{phase}.log') for phase in ('initial', 'warm_replay', 'control')] + [str(viewer_log)],
            'runner_stopped': all(p.poll() is not None for p in procs), 'actual_stop_confirmed': True,
            'processes_stopped': all(p.poll() is not None for p in procs)})
        report['mechanism_metrics'] = mechanisms
        report['accounting'] = {'ingress_delta': windows['initial']['ingress_delta'],
            'imported_input': unique_count, 'warm_replay': warm_count,
            'main_dataset_delta': {'ledger': initial_delta.get('ledger'),
                'raw': initial_delta.get('raw'), 'temporal': initial_delta.get('temporal')},
            'command_receipts_verified': len(pg_receipts),
            'command_configurations_verified': len(rest_configs)}
        report['control_reconciliation'] = {'status': 'PASS' if len(pg_receipts) == 4 and len(rest_configs) == 2 else 'INCONCLUSIVE',
            'verified_receipts': len(pg_receipts), 'verified_configurations': len(rest_configs)}
        source_fp = report['source_fingerprint']
        metadata = {'source_revision': source_fp['revision'], 'dirty_worktree': source_fp['dirty'],
            'dirty_tree_hash': source_fp['dirty_source_sha256'],
            'binary_sha256': hashlib.sha256((Path(paperload_binary).read_bytes() + Path(gateway_binary).read_bytes())).hexdigest(),
            'paperload_source_sha256': _paperload_source_hash(smoke.ROOT),
            'image_ids': {k: str(v) for k, v in images.items() if v is not None}, 'schema': schema,
            'security_mode': {'fixture_jwt_mode': _jwt_algorithm(tokens['admin']),
                'endpoint_tls': str(getattr(endpoint, 'url', '')).lower().startswith('https://'),
                'mqtt_tls': str(mqtt_broker_url if 'mqtt_broker_url' in locals() else '').lower().startswith('ssl://'),
             'mqtt_authentication_readback': 'not_performed'}, 'quota': quota,
            'workers': {'paperload': 2, 'gateway_executors': 2,
                **{key.lower(): int(actual_env[key]) for key in ('MQTT_WORKER_COUNT', 'DATABASE_MAX_CONNS', 'STAGE5_PUBLISH_WORKERS', 'STAGE5_RESULT_WORKERS')}},
            'durability': 'synthetic_journal_background+sqlite_command_executors',
            'topology': {'gateways': 2, 'sensors_per_gateway': 1, 'viewers': 2},
            'seed': context['seed'], 'phase_seconds': {'initial': 1, 'warm_replay': 2, 'viewer': 40},
            'cohort': {'sensor_ids': context['sensor_ids'], 'gateway_ids': context['gateway_ids']},
            'workload_sha256': prepared['workload_sha256'], 'workload': prepared['workload'],
            'effective_flags': {k: str(actual_env[k]).lower() for k in expected}}
        metadata['comparison_metadata'] = e5.comparison_metadata(workload=prepared['workload'],
            journal_sha256=prepared['journal_sha256'])
        if metadata['comparison_metadata']['workload_sha256'] != prepared['workload_sha256']:
            raise ValueError('e5_workload_fingerprint_mismatch')
        report['metadata'] = metadata
        if not validation['valid']: raise ValueError('e5_validation_failed')
        report['status'] = 'WORKLOAD_COMPLETE_PENDING_CLEANUP'
    except Exception as exc:
        safe_code = str(exc) if isinstance(exc, ValueError) and re.fullmatch(r'(?:e5|p8)_[-a-zA-Z0-9_]+', str(exc)) else None
        report['primary_error'] = {'phase': current_phase, 'code': safe_code,
            'exception': type(exc).__name__,
            'location': _safe_exception_location(exc) if safe_code is None else None}
    finally:
        for collector in collectors:
            if collector in stopped_collectors:
                continue
            try: collector.stop(collector_pids.get(collector))
            except Exception as exc: report['cleanup_errors'].append('collector_' + type(exc).__name__)
        if viewer_proc is not None:
            try: e4._stop_viewer(viewer_proc)
            except Exception as exc: report['cleanup_errors'].append('viewer_' + type(exc).__name__)
        for proc in procs:
            if proc.poll() is None:
                try: proc.terminate(); proc.wait(timeout=3)
                except Exception:
                    try: proc.kill(); proc.wait(timeout=3)
                    except Exception as exc: report['cleanup_errors'].append('process_' + type(exc).__name__)
        report['runner_stopped'] = all(p.poll() is not None for p in procs)
        viewer_stopped = viewer_proc is None or viewer_proc.poll() is not None
        report['processes_stopped'] = all(p.poll() is not None for p in procs) and viewer_stopped
        if report['status'] == 'WORKLOAD_COMPLETE_PENDING_CLEANUP' and report['cleanup_errors']:
            report['status'] = 'FAIL'
        # Keep helper output staged under the private fixture until the native
        # lifecycle has completed its registry-owned cleanup check.
        report_path = Path(fixture.private) / 'p8-e5-result.json'
        try:
            smoke.durable_json(report_path, report)
        except Exception:
            return {'status': 'FAIL', 'experiment': 'E5', 'arm': arm,
                    'primary_error': report.get('primary_error'),
                    'cleanup_errors': report.get('cleanup_errors', []),
                    'artifact_error': 'durable_artifact_write_failed'}
    return report

def _safe_e5_viewer(result):
    keys = ('status', 'connected_distinct_viewers', 'subscribed_distinct_viewers', 'receiving_distinct_viewers',
            'unique_events', 'viewers', 'duplicates', 'wrong_namespace',
            'identity_samples', 'phase_events', 'cleanup_errors')
    return {key: result.get(key) for key in keys if key in result}


def _safe_exception_location(exc):
    """Keep only a source basename/line for unexpected failures, never its message."""
    frames = traceback.extract_tb(exc.__traceback__)
    if not frames:
        return None
    frame = frames[-1]
    return {'file': Path(frame.filename).name, 'line': frame.lineno}


def _validate_e5_viewer(result, context, initial, initial_ws, warm_ws, end_metrics):
    failures = []
    if not isinstance(result, dict) or result.get('status') != 'COMPLETE':
        failures.append('viewer_incomplete')
        result = result if isinstance(result, dict) else {}
    if (result.get('connected_distinct_viewers') != 2 or result.get('subscribed_distinct_viewers') != 2
            or result.get('receiving_distinct_viewers') != 2):
        failures.append('viewer_readiness')
    selection = context.get('workload_selection') or e5.workload_preset('pilot')
    unique_count = selection['telemetry_messages']
    if result.get('unique_events') != unique_count or result.get('duplicates') != 0 or result.get('wrong_namespace') != 0:
        failures.append('viewer_event_accounting')
    viewers = result.get('viewers')
    subscriptions = context.get('viewer_subscriptions') or []
    expected_ids = {row.get('viewer_id') for row in subscriptions}
    counts = {row.get('id'): row.get('unique_events') for row in viewers if isinstance(row, dict)} if isinstance(viewers, list) else {}
    if set(counts) != expected_ids or any(value != unique_count // len(expected_ids) for value in counts.values()):
        failures.append('viewer_per_identity_accounting')
    headers = (initial.get('e4') or {}).get('safe_header_samples', [])
    expected = {row.get('message_id') for row in headers if isinstance(row, dict)}
    observed = {row.get('message_id') for row in result.get('identity_samples', []) if isinstance(row, dict)}
    if not expected or not observed or not observed.issubset(expected):
        failures.append('viewer_header_provenance')
    for name, window, expected_new in (('initial', initial_ws, unique_count), ('warm', warm_ws, 0)):
        if not isinstance(window, dict) or any(window.get(k) != v for k, v in {
                'new_events': expected_new, 'telemetry_writes_completed': expected_new,
                'deliveries_enqueued': expected_new, 'drops': 0}.items()):
            failures.append('viewer_ws_' + name + '_window')
    ws = end_metrics.get('websocket_metrics') if isinstance(end_metrics, dict) else None
    if not isinstance(ws, dict) or ws.get('global_queued_work') != 0 or ws.get('global_queued_bytes') != 0:
        failures.append('viewer_end_gauges')
    return {'valid': not failures, 'failures': failures}


def _paperload_source_hash(root):
    source_root = Path(root) / 'src'
    paths = sorted((source_root / 'internal/paperload').rglob('*.go'))
    paths.extend(sorted((source_root / 'cmd/paperload').rglob('*.go')))
    if not paths:
        raise ValueError('e5_paperload_source_missing')
    digest = hashlib.sha256()
    for path in paths:
        digest.update(path.relative_to(source_root).as_posix().encode() + b'\0')
        digest.update(path.read_bytes())
    return digest.hexdigest()


def _jwt_algorithm(token):
    try:
        import base64
        encoded = token.split('.')[0]
        payload = json.loads(base64.urlsafe_b64decode(encoded + '=' * (-len(encoded) % 4)))
        algorithm = payload.get('alg')
        return algorithm if isinstance(algorithm, str) else 'unavailable'
    except (AttributeError, ValueError, json.JSONDecodeError):
        return 'unavailable'


GATEWAYS = ('p8-e5-a', 'p8-e5-b')
COMMAND_SENSORS = ('p8-e5-command-a', 'p8-e5-command-b')
RECOVERY_AUX_SENSOR = 'p8-e5-recovery-aux'


def recovery_aux_outbox_limits():
    return {'GATEWAY_MAX_INFLIGHT': '4', 'GATEWAY_OUTBOX_MAX_ROWS': '4',
        'GATEWAY_OUTBOX_MAX_BYTES': str(1 << 16)}
_UUID4 = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$')


def _private_root(path):
    root = Path(path)
    try:
        stat = root.lstat()
    except OSError:
        raise ValueError('e5_root_must_exist_private') from None
    if root.is_symlink() or not root.is_dir() or stat.st_uid != os.getuid() or stat.st_mode & 0o777 != 0o700:
        raise ValueError('e5_root_must_exist_private')
    return root


def _write(path, data):
    path = Path(path)
    with path.open('x', encoding='utf-8') as stream:
        json.dump(data, stream, sort_keys=True, separators=(',', ':'))
    path.chmod(0o600)
    return str(path)


def prepare_inputs(*, smoke, fixture, endpoint, specs, passwords, tokens, mqtt_broker_url,
                   mqtt_manifest_endpoint, mqtt_ca_file, journal_path, journal_context,
                   seed, anchor, root, command_keys, viewer_subscriptions, command_versions=None,
                   workload_selection=None, recovery_mode=False):
    """Create byte-preserving journal copy, four private role inputs, and run-spec.

    ``journal_context`` is the one shared repeat context: ``sha256``, ``bytes``,
    ``sensor_ids`` and ``gateway_ids``. The caller must create it once before arms.
    """
    root = _private_root(root)
    if len(specs) != 2 or [s.gateway_id for s in specs] != list(GATEWAYS):
        raise ValueError('e5_gateway_identity_mismatch')
    sensor_ids = [s.sensor_id for s in specs]
    workload_selection = workload_selection or e5.workload_preset('pilot')
    if (e5.selection_sha256(workload_selection) != e5.selection_sha256(e5.workload_preset(workload_selection.get('profile')))
            or len(passwords) != 2 or len(command_keys) != workload_selection['command_count']
            or len(set(command_keys)) != len(command_keys) or any(not _UUID4.fullmatch(k) for k in command_keys)):
        raise ValueError('e5_repeat_inputs_invalid')
    if (journal_context.get('gateway_ids') != list(GATEWAYS) or
            journal_context.get('sensor_ids') != sensor_ids or
            type(seed) is not int or seed < 0 or journal_context.get('seed') != seed or
            journal_context.get('anchor') != anchor):
        raise ValueError('e5_shared_context_mismatch')
    source = Path(journal_path)
    try:
        parent_stat = source.parent.lstat()
    except OSError:
        raise ValueError('e5_source_journal_not_private') from None
    if (source.is_symlink() or not source.is_file() or source.stat().st_mode & 0o077 or
            source.parent.is_symlink() or parent_stat.st_uid != os.getuid() or parent_stat.st_mode & 0o077):
        raise ValueError('e5_source_journal_not_private')
    raw = source.read_bytes()
    digest = hashlib.sha256(raw).hexdigest()
    selection_digest = e5.selection_sha256(workload_selection)
    if (digest != journal_context.get('sha256') or len(raw) != journal_context.get('bytes')
            or journal_context.get('workload_selection_sha256', selection_digest) != selection_digest):
        raise ValueError('e5_journal_context_mismatch')
    copied = root / 'dataset.jsonl'
    fd = os.open(copied, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'wb') as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())

    identity_files, empty_maps = {}, {}
    for spec, password, suffix in zip(specs, passwords, ('a', 'b')):
        identity_files[spec.gateway_id] = _write(root / f'gateway-{suffix}.json',
            {'username': spec.gateway_id, 'password': password})
        empty_maps[suffix.upper()] = _write(root / f'telemetry-{suffix}-identities.json', {})

    # Use the same owner-authorized provisioning and authoritative REST readback as E3.
    observed_versions = {}
    for sid in COMMAND_SENSORS:
        urn = f'urn:ngsi-ld:Sensor:{GATEWAYS[0]}:{sid}'
        status, _, body = smoke.http_request(endpoint, 'PUT',
            f'/v1/gateways/{GATEWAYS[0]}/sensors/{sid}', token=tokens['a'],
            body={'name': f'P8 E5 Command Sensor {sid}', 'unit': specs[0].sensor_unit})
        if status != 201 or body.get('entity_id') != urn:
            raise ValueError('e5_command_sensor_provision_failed')
        status, _, detail = smoke.http_request(endpoint, 'GET',
            f'/v1/digital-twins/{urn}?format=simplified', token=tokens['a'])
        if status != 200 or not isinstance(detail, dict):
            raise ValueError('e5_command_sensor_get_failed')
        version = detail.get('desired_version')
        if version is None and isinstance(detail.get('desired_state'), dict):
            version = detail['desired_state'].get('desired_version')
        if version is None:
            raise ValueError('e5_command_sensor_version_missing')
        observed_versions[sid] = str(version)
    if recovery_mode:
        recovery_urn = f'urn:ngsi-ld:Sensor:{GATEWAYS[0]}:{RECOVERY_AUX_SENSOR}'
        status, _, body = smoke.http_request(endpoint, 'PUT',
            f'/v1/gateways/{GATEWAYS[0]}/sensors/{RECOVERY_AUX_SENSOR}', token=tokens['a'],
            body={'name': 'P8 E5 Recovery Auxiliary Sensor', 'unit': specs[0].sensor_unit})
        if status != 201 or body.get('entity_id') != recovery_urn:
            raise ValueError('e5_recovery_sensor_provision_failed')
        status, _, detail = smoke.http_request(endpoint, 'GET',
            f'/v1/digital-twins/{recovery_urn}?format=simplified', token=tokens['a'])
        if status != 200 or not isinstance(detail, dict):
            raise ValueError('e5_recovery_sensor_readback_failed')
    if command_versions is not None and {k: str(v) for k, v in command_versions.items()} != observed_versions:
        raise ValueError('e5_command_version_readback_mismatch')

    common = {'schema_version': 1, 'mode': 'local', 'run_id': f'{fixture.project}-e5', 'seed': seed, 'arm': 'warm',
              'bundle_path': str(root), 'publish_timeout_ms': 3000, 'retry_interval_ms': 500,
              'profile': 'pilot', 'samples_per_message': workload_selection['samples_per_message'],
              'topology': {'gateways': 2, 'sensors_per_gateway': 1, 'users': 0, 'viewers': 0},
              'tls': {'https_endpoint': endpoint.url, 'https_ca_file': str(endpoint.ca_file),
                      'mqtt_tls_endpoint': mqtt_manifest_endpoint,
                      'mqtt_tls_ca_file': str(mqtt_ca_file), 'mqtt_tls_server_name': 'localhost'}}
    if set(sensor_ids) & set(COMMAND_SENSORS):
        raise ValueError('e5_command_sensor_overlaps_main_sensor')
    telemetry = {**common, 'schema_version': 1, 'experiment': 'E4', 'arm': 'warm',
            'roles': ['telemetry'],
            'targets': [{'gateway_id': spec.gateway_id, 'sensor_id': spec.sensor_id} for spec in specs],
            'subscriptions': [], 'e4': {'mode': 'replay', 'journal_path': str(copied),
                'replay_count': workload_selection['initial_replay_count'], 'ignore_first_valid_acks': 0,
                'max_messages': workload_selection['initial_replay_count'], 'max_bytes': 1 << 20},
            'traffic': {'rate_per_second': workload_selection['initial_replay_rate_per_second'], 'ramp_seconds': 0,
                'burst_messages': max(0, workload_selection['initial_replay_count'] - 1)},
            # paperload requires a positive measurement window even for E4 replay;
            # the replay count, not the generated-offer estimate, bounds its input.
            'phases': {'warmup_seconds': 0, 'measure_seconds': 1, 'drain_seconds': 12, 'cooldown_seconds': 0},
            'limits': {'workers': 2, 'connections': 2, 'pending_messages': 64,
                'pending_bytes': 1 << 20, 'wall_seconds': 30, 'retries': 16,
                'disk_growth_bytes': 1 << 20}, 'retry_interval_ms': 500,
            'durability_mode': 'synthetic-load',
            'gateway_identity_files': identity_files,
            'identity_files': {'telemetry': _write(root / 'telemetry-identities.json', {})},
            }
    # A separate bounded warm-replay input references the exact same private bytes.
    warm_measure_seconds = 2
    warm_rate_per_second = workload_selection['warm_replay_prefix'] // warm_measure_seconds
    warm = dict(telemetry, e4={'mode': 'replay', 'journal_path': str(copied),
        'replay_count': workload_selection['warm_replay_prefix'],
        'ignore_first_valid_acks': 0, 'max_messages': workload_selection['warm_replay_prefix'], 'max_bytes': 1 << 20},
        traffic={'rate_per_second': warm_rate_per_second, 'ramp_seconds': 0, 'burst_messages': 0},
        phases={'warmup_seconds': 0, 'measure_seconds': warm_measure_seconds,
                'drain_seconds': 12, 'cooldown_seconds': 0})
    viewer_identity = _write(root / 'viewer-identities.json',
                              {'user-a': tokens['a'], 'user-b': tokens['b']})
    viewer_common = {**common, 'topology': {**common['topology'], 'users': 2, 'viewers': 2},
                     'identity_files': {'viewer': viewer_identity},
                     'targets': [{'gateway_id': spec.gateway_id, 'sensor_id': spec.sensor_id} for spec in specs],
                     'traffic': {'rate_per_second': 5, 'ramp_seconds': 0, 'burst_messages': 0}}
    viewer = e4.e4_viewer_manifest(viewer_common, subscriptions=viewer_subscriptions,
        viewer_identity=viewer_identity, root=root)
    if len(viewer_subscriptions) == 3:
        viewer['subscriptions'] = [dict(row) for row in viewer_subscriptions]
    viewer['phases'] = {'warmup_seconds': 0, 'measure_seconds': 40, 'drain_seconds': 0, 'cooldown_seconds': 0}
    viewer['limits']['wall_seconds'] = 70
    # Reuse E3 command contract and caller-provided UUIDv4 keys; omit all MQTT fields.
    control_targets = [{'gateway_id': GATEWAYS[0], 'sensor_id': sid} for sid in COMMAND_SENSORS]
    bundle = e3.e3_pilot_manifests(common=common,
        background_targets=[{'gateway_id': s.gateway_id, 'sensor_id': s.sensor_id} for s in specs],
        background_gateway_identity_files=identity_files, telemetry_identity_files=empty_maps,
        control_targets=control_targets, initial_versions=observed_versions,
            control_identity_file=_write(root / 'control-tokens.json', {'e3-operator': tokens['a']}),
            tls=common['tls'],
        idempotency_keys=command_keys)
    control = bundle['control']
    control['seed'] = seed
    control['tls'] = {'https_endpoint': endpoint.url, 'https_ca_file': str(endpoint.ca_file)}
    env_paths = []
    for i, sid in enumerate(COMMAND_SENSORS):
        env = gateway_env(GATEWAYS[0], passwords[0])
        env.update({'GATEWAY_CONTROL_ENABLED': 'true', 'GATEWAY_MQTT_BROKER_URL': mqtt_broker_url,
            'GATEWAY_MQTT_CA_FILE': str(mqtt_ca_file), 'GATEWAY_MQTT_SERVER_NAME': 'localhost',
            'GATEWAY_SAMPLE_RATE_HZ': '1', 'GATEWAY_SAMPLES_PER_BATCH': '100',
            'GATEWAY_OUTBOX_PATH': str(root / f'executor-{i+1}.sqlite'),
            'GATEWAY_OUTBOX_MAX_ROWS': '64', 'GATEWAY_OUTBOX_MAX_BYTES': str(1 << 20),
            'GATEWAY_SHUTDOWN_TIMEOUT': '10s'})
        env = _executor_environment(env, sid, f'{GATEWAYS[0]}_p8_e5_executor_{i+1}')
        env_paths.append(_write(root / f'executor-{i+1}.env.json', env))
    aux_env_path = aux_db_path = None
    if recovery_mode:
        aux_env = gateway_env(GATEWAYS[0], passwords[0])
        aux_env.update({'GATEWAY_CONTROL_ENABLED': 'false', 'GATEWAY_MQTT_BROKER_URL': mqtt_broker_url,
            'GATEWAY_MQTT_CA_FILE': str(mqtt_ca_file), 'GATEWAY_MQTT_SERVER_NAME': 'localhost',
            'GATEWAY_SAMPLE_RATE_HZ': '1', 'GATEWAY_SAMPLES_PER_BATCH': '1',
            'GATEWAY_OUTBOX_PATH': str(root / 'recovery-aux.sqlite'),
            **recovery_aux_outbox_limits(),
            'GATEWAY_SHUTDOWN_TIMEOUT': '10s'})
        aux_env = _executor_environment(aux_env, RECOVERY_AUX_SENSOR,
                                        f'{GATEWAYS[0]}_p8_e5_recovery_aux')
        aux_env_path = _write(root / 'recovery-aux.env.json', aux_env)
        aux_db_path = aux_env['GATEWAY_OUTBOX_PATH']
    _write(root / 'initial-replay.manifest.json', telemetry)
    _write(root / 'warm-replay.manifest.json', warm)
    _write(root / 'viewer.manifest.json', viewer)
    _write(root / 'control.manifest.json', control)
    # Control-only runner specification. Invocation belongs to a later runtime slice.
    try:
        from scripts.paper_p8_resources import disk_guard_config
    except ImportError:  # direct script execution puts scripts/ on sys.path
        from paper_p8_resources import disk_guard_config
    run_spec = {'paperload': [{'manifest': str(root / 'control.manifest.json'),
        'output_dir': str(root / 'control-output'), 'role': 'control', 'ready_timeout_seconds': 90}],
        'gateways': [{'env_file': p, 'role': 'executor', 'ready_timeout_seconds': 30} for p in env_paths],
        'stop_timeout_seconds': 30,
        'disk_guard': disk_guard_config([telemetry, warm, viewer, control])}
    run_spec_path = _write(root / 'control-run-spec.json', run_spec)
    workload = e5.workload_manifest(journal_sha256=digest, journal_bytes=len(raw),
        command_ids=command_keys, sensor_ids=sensor_ids, viewer_targets=viewer_subscriptions,
        anchor=anchor, selection=workload_selection)
    return {'root': str(root), 'dataset_path': str(copied), 'journal_sha256': digest,
        'workload': workload, 'workload_sha256': e5.comparison_metadata(workload=workload,
            journal_sha256=digest)['workload_sha256'], 'manifests': {'initial': str(root/'initial-replay.manifest.json'),
            'warm_replay': str(root/'warm-replay.manifest.json'), 'viewer': str(root/'viewer.manifest.json'),
            'control': str(root/'control.manifest.json'), 'gateway_env': env_paths, 'run_spec': run_spec_path,
            **({'recovery_aux_env_file': str(aux_env_path), 'recovery_aux_db_path': aux_db_path}
               if recovery_mode else {})},
        'expected': {'main_messages': workload_selection['telemetry_messages'],
            'raw_samples': workload_selection['telemetry_messages'] * workload_selection['samples_per_message'],
            'temporal': workload_selection['telemetry_messages'],
            'initial_schedule': {'rate_per_second': workload_selection['initial_replay_rate_per_second'],
                'measure_seconds': 1, 'burst_messages': max(0, workload_selection['initial_replay_count'] - 1)},
            'viewers': workload_selection['viewer_count'], 'commands': workload_selection['command_count']},
        'execution': 'NOT RUN'}
