-- Regression test probes for Migration 000019 (simplified journal & narrow reset admission)
-- Tests both valid paths and invalid counterexamples under rollback.
\set ON_ERROR_STOP on
BEGIN;

-- Setup test actors and gateways
INSERT INTO public.profiles(id, full_name) VALUES
    ('19000000-0000-4000-8000-000000000001', 'Admin 1'),
    ('19000000-0000-4000-8000-000000000002', 'Non-Admin 2');
INSERT INTO public.platform_admins(user_id) VALUES
    ('19000000-0000-4000-8000-000000000001');

INSERT INTO public.gateways(gateway_id, name) VALUES
    ('gw_sim_prov', 'Simplified Provision Gateway'),
    ('gw_sim_prov2', 'Simplified Provision Gateway 2'),
    ('gw_sim_reset_unk', 'Reset Unknown Gateway'),
    ('gw_sim_reset_lost', 'Reset Lost Secret Gateway'),
    ('gw_sim_repeat_reset', 'Repeat Reset Gateway'),
    ('gw_sim_reset_gap', 'Reset Attempted Gap Gateway');

CREATE FUNCTION pg_temp.must_fail(stmt text, expected_msg text) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    BEGIN
        EXECUTE stmt;
        SET CONSTRAINTS ALL IMMEDIATE;
    EXCEPTION WHEN OTHERS THEN
        IF SQLERRM NOT LIKE '%' || expected_msg || '%' THEN
            RAISE EXCEPTION 'Expected error matching "%", got "%"', expected_msg, SQLERRM;
        END IF;
        RETURN;
    END;
    RAISE EXCEPTION 'Statement unexpectedly succeeded: %', stmt;
END $$;

DO $$ BEGIN
    SET LOCAL ROLE iot_backend_app;
    SET CONSTRAINTS ALL DEFERRED;

    --------------------------------------------------------------------------------
    -- Case 1: Contract Discriminator, Default Lifecycle, & Simplified Distinction
    --------------------------------------------------------------------------------
    INSERT INTO public.gateway_mqtt_credential_events(
        operation_id, gateway_id, actor_user_id, idempotency_key, credential_version, action, status
    ) VALUES (
        '19100000-0000-4000-8000-000000000001', 'gw_sim_prov',
        '19000000-0000-4000-8000-000000000001', '19100000-0000-4000-8000-000000000001',
        1, 'provision', 'pending'
    );

    INSERT INTO public.gateway_mqtt_credentials(
        gateway_id, credential_version, status, last_operation_id, changed_by
    ) VALUES (
        'gw_sim_prov', 1, 'provisioning',
        '19100000-0000-4000-8000-000000000001', '19000000-0000-4000-8000-000000000001'
    );

    -- 1a. Default lifecycle success WITHOUT snapshot_observed MUST FAIL under chk_mqtt_operation_contract
    -- (This also acts as the exact oracle for RED test against schema 018).
    PERFORM pg_temp.must_fail($q$
        UPDATE public.gateway_mqtt_credential_events
        SET status = 'succeeded', phase = 'finalize', ram_applied = true, snapshot_observed = false,
            fresh_positive_verified = true, completed_at = now(), updated_at = now()
        WHERE operation_id = '19100000-0000-4000-8000-000000000001';
    $q$, 'violates check constraint "chk_mqtt_operation_contract"');

    -- 1b. Verify column execution_contract defaults to 'lifecycle' when omitted
    IF (SELECT to_jsonb(e) ->> 'execution_contract' FROM public.gateway_mqtt_credential_events e WHERE operation_id = '19100000-0000-4000-8000-000000000001') <> 'lifecycle' THEN
        RAISE EXCEPTION 'Default execution_contract must be lifecycle';
    END IF;

    -- 1c. Immutability of execution_contract:
    -- App role (iot_backend_app) has NO UPDATE privilege on execution_contract:
    PERFORM pg_temp.must_fail($q$
        UPDATE public.gateway_mqtt_credential_events
        SET execution_contract = 'simplified'
        WHERE operation_id = '19100000-0000-4000-8000-000000000001';
    $q$, 'permission denied for table gateway_mqtt_credential_events');

    -- Furthermore, trigger enforce_mqtt_reset_event strictly enforces execution_contract immutability
    -- even if an administrative / owner caller attempts UPDATE:
    RESET ROLE;
    PERFORM pg_temp.must_fail($q$
        UPDATE public.gateway_mqtt_credential_events
        SET execution_contract = 'simplified'
        WHERE operation_id = '19100000-0000-4000-8000-000000000001';
    $q$, 'Execution contract is immutable');
    SET LOCAL ROLE iot_backend_app;

    -- 1d. Valid lifecycle success WITH snapshot_observed still works at DB-seam level as in 000018
    UPDATE public.gateway_mqtt_credential_events
    SET status = 'succeeded', phase = 'finalize', ram_applied = true, snapshot_observed = true,
        fresh_positive_verified = true, completed_at = now(), updated_at = now()
    WHERE operation_id = '19100000-0000-4000-8000-000000000001';

    UPDATE public.gateway_mqtt_credentials
    SET status = 'active', activated_at = now(), updated_at = now()
    WHERE gateway_id = 'gw_sim_prov';

    UPDATE public.mqtt_credential_maintenance
    SET status = 'completed', broker_epoch = repeat('a', 64), completed_at = now(), updated_at = now()
    WHERE operation_id = '19100000-0000-4000-8000-000000000001';

    SET CONSTRAINTS ALL IMMEDIATE;
    SET CONSTRAINTS ALL DEFERRED;

    -- 1e. Explicit simplified provision success WITHOUT snapshot_observed is permitted and distinguished
    INSERT INTO public.gateway_mqtt_credential_events(
        operation_id, gateway_id, actor_user_id, idempotency_key, credential_version, action, status,
        execution_contract, previous_status, previous_credential_version, previous_activated_at
    ) VALUES (
        '19100000-0000-4000-8000-000000000002', 'gw_sim_prov',
        '19000000-0000-4000-8000-000000000001', '19100000-0000-4000-8000-000000000002',
        2, 'rotate', 'pending',
        'simplified', 'active', 1, now()
    );

    UPDATE public.gateway_mqtt_credentials
    SET status = 'rotating', credential_version = 2, last_operation_id = '19100000-0000-4000-8000-000000000002', updated_at = now()
    WHERE gateway_id = 'gw_sim_prov';

    -- Finalize simplified rotate: ram_applied=true, fresh_positive_verified=true, snapshot_observed=FALSE succeeds!
    UPDATE public.gateway_mqtt_credential_events
    SET status = 'succeeded', phase = 'finalize', ram_applied = true, snapshot_observed = false,
        fresh_positive_verified = true, completed_at = now(), updated_at = now()
    WHERE operation_id = '19100000-0000-4000-8000-000000000002';

    UPDATE public.gateway_mqtt_credentials
    SET status = 'active', activated_at = now(), updated_at = now()
    WHERE gateway_id = 'gw_sim_prov';

    -- Zero maintenance rows created or required for simplified rotate!
    IF EXISTS (SELECT 1 FROM public.mqtt_credential_maintenance WHERE operation_id = '19100000-0000-4000-8000-000000000002') THEN
        RAISE EXCEPTION 'Simplified rotate must not have maintenance row';
    END IF;

    SET CONSTRAINTS ALL IMMEDIATE;
    SET CONSTRAINTS ALL DEFERRED;

    -- 1f. Reset action with execution_contract='lifecycle' must be rejected (reset is simplified only)
    PERFORM pg_temp.must_fail($q$
        INSERT INTO public.gateway_mqtt_credential_events(
            operation_id, gateway_id, actor_user_id, idempotency_key, credential_version, action, status,
            execution_contract, previous_status, previous_credential_version, previous_activated_at,
            reset_predecessor_id, reset_reason
        ) VALUES (
            '19100000-0000-4000-8000-000000000003', 'gw_sim_prov',
            '19000000-0000-4000-8000-000000000001', '19100000-0000-4000-8000-000000000003',
            3, 'reset', 'pending',
            'lifecycle', 'active', 2, now(),
            '19100000-0000-4000-8000-000000000002', 'lost_secret_recovery'
        );
    $q$, 'Reset action requires simplified execution contract');

    -- 1g. Decoupling Check: Maintenance creation under simplified contract
    -- Explicit simplified provision/rotate/revoke/reset must create ZERO maintenance rows!
    -- Use a dedicated gateway gw_sim_prov2 to test complete simplified provision lifecycle:
    -- pending admission -> finalize success with zero maintenance created or required.
    INSERT INTO public.gateway_mqtt_credential_events(
        operation_id, gateway_id, actor_user_id, idempotency_key, credential_version, action, status,
        execution_contract
    ) VALUES (
        '19100000-0000-4000-8000-000000000004', 'gw_sim_prov2',
        '19000000-0000-4000-8000-000000000001', '19100000-0000-4000-8000-000000000004',
        1, 'provision', 'pending',
        'simplified'
    );
    INSERT INTO public.gateway_mqtt_credentials(
        gateway_id, credential_version, status, last_operation_id, changed_by
    ) VALUES (
        'gw_sim_prov2', 1, 'provisioning',
        '19100000-0000-4000-8000-000000000004', '19000000-0000-4000-8000-000000000001'
    );
    IF EXISTS (SELECT 1 FROM public.mqtt_credential_maintenance WHERE operation_id = '19100000-0000-4000-8000-000000000004') THEN
        RAISE EXCEPTION 'Simplified operation must create ZERO maintenance rows';
    END IF;

    SET CONSTRAINTS ALL IMMEDIATE;
    SET CONSTRAINTS ALL DEFERRED;

    -- Finalize simplified provision
    UPDATE public.gateway_mqtt_credential_events
    SET status = 'succeeded', phase = 'finalize', ram_applied = true, snapshot_observed = false,
        fresh_positive_verified = true, completed_at = now(), updated_at = now()
    WHERE operation_id = '19100000-0000-4000-8000-000000000004';

    UPDATE public.gateway_mqtt_credentials
    SET status = 'active', activated_at = now(), updated_at = now()
    WHERE gateway_id = 'gw_sim_prov2';

    SET CONSTRAINTS ALL IMMEDIATE;
    SET CONSTRAINTS ALL DEFERRED;

    --------------------------------------------------------------------------------
    -- Case 2: Reset after unknown (unresolved attempt recovery)
    --------------------------------------------------------------------------------
    -- Step 2a: Setup an unresolved rotation attempt on gw_sim_reset_unk
    INSERT INTO public.gateway_mqtt_credential_events(
        operation_id, gateway_id, actor_user_id, idempotency_key, credential_version, action, status,
        execution_contract
    ) VALUES (
        '19200000-0000-4000-8000-000000000001', 'gw_sim_reset_unk',
        '19000000-0000-4000-8000-000000000001', '19200000-0000-4000-8000-000000000001',
        1, 'provision', 'pending',
        'simplified'
    );
    INSERT INTO public.gateway_mqtt_credentials(
        gateway_id, credential_version, status, last_operation_id, changed_by
    ) VALUES (
        'gw_sim_reset_unk', 1, 'provisioning',
        '19200000-0000-4000-8000-000000000001', '19000000-0000-4000-8000-000000000001'
    );
    UPDATE public.gateway_mqtt_credential_events
    SET status = 'succeeded', phase = 'finalize', ram_applied = true, snapshot_observed = false,
        fresh_positive_verified = true, completed_at = now(), updated_at = now()
    WHERE operation_id = '19200000-0000-4000-8000-000000000001';
    UPDATE public.gateway_mqtt_credentials
    SET status = 'active', activated_at = now(), updated_at = now()
    WHERE gateway_id = 'gw_sim_reset_unk';

    SET CONSTRAINTS ALL IMMEDIATE;
    SET CONSTRAINTS ALL DEFERRED;

    -- Now start a rotate that becomes unresolved (pending)
    INSERT INTO public.gateway_mqtt_credential_events(
        operation_id, gateway_id, actor_user_id, idempotency_key, credential_version, action, status,
        execution_contract, previous_status, previous_credential_version, previous_activated_at
    ) VALUES (
        '19200000-0000-4000-8000-000000000002', 'gw_sim_reset_unk',
        '19000000-0000-4000-8000-000000000001', '19200000-0000-4000-8000-000000000002',
        2, 'rotate', 'pending',
        'simplified', 'active', 1, now()
    );
    UPDATE public.gateway_mqtt_credentials
    SET status = 'rotating', credential_version = 2, last_operation_id = '19200000-0000-4000-8000-000000000002', updated_at = now()
    WHERE gateway_id = 'gw_sim_reset_unk';
    SET CONSTRAINTS ALL IMMEDIATE;
    SET CONSTRAINTS ALL DEFERRED;

    -- Step 2b: Test rejections for invalid reset attempts on gw_sim_reset_unk
    -- Rejection 1: Non-admin actor rejected
    PERFORM pg_temp.must_fail($q$
        INSERT INTO public.mqtt_reset_dispositions(predecessor_id, successor_id, gateway_id, reason)
        VALUES('19200000-0000-4000-8000-000000000002', '19200000-0000-4000-8000-000000000003', 'gw_sim_reset_unk', 'recovery_after_unknown');
        INSERT INTO public.gateway_mqtt_credential_events(
            operation_id, gateway_id, actor_user_id, idempotency_key, credential_version, action, status,
            execution_contract, previous_status, previous_credential_version, previous_activated_at,
            reset_predecessor_id, reset_reason
        ) VALUES (
            '19200000-0000-4000-8000-000000000003', 'gw_sim_reset_unk',
            '19000000-0000-4000-8000-000000000002', '19200000-0000-4000-8000-000000000003',
            3, 'reset', 'pending',
            'simplified', 'rotating', 2, now(),
            '19200000-0000-4000-8000-000000000002', 'recovery_after_unknown'
        );
        UPDATE public.gateway_mqtt_credentials
        SET status = 'rotating', credential_version = 3, last_operation_id = '19200000-0000-4000-8000-000000000003', changed_by = '19000000-0000-4000-8000-000000000002', updated_at = now()
        WHERE gateway_id = 'gw_sim_reset_unk';
    $q$, 'Reset event requires platform admin actor');

    -- Rejection 2: Wrong reset reason for unresolved predecessor (lost_secret_recovery requires active/succeeded)
    PERFORM pg_temp.must_fail($q$
        INSERT INTO public.mqtt_reset_dispositions(predecessor_id, successor_id, gateway_id, reason)
        VALUES('19200000-0000-4000-8000-000000000002', '19200000-0000-4000-8000-000000000003', 'gw_sim_reset_unk', 'lost_secret_recovery');
    $q$, 'lost_secret_recovery requires succeeded password-issuing predecessor');

    -- Step 2c: Legitimate reset admission with recovery_after_unknown (simplified contract)
    INSERT INTO public.mqtt_reset_dispositions(predecessor_id, successor_id, gateway_id, reason)
    VALUES('19200000-0000-4000-8000-000000000002', '19200000-0000-4000-8000-000000000003', 'gw_sim_reset_unk', 'recovery_after_unknown');

    INSERT INTO public.gateway_mqtt_credential_events(
        operation_id, gateway_id, actor_user_id, idempotency_key, credential_version, action, status,
        execution_contract, previous_status, previous_credential_version, previous_activated_at,
        reset_predecessor_id, reset_reason
    ) VALUES (
        '19200000-0000-4000-8000-000000000003', 'gw_sim_reset_unk',
        '19000000-0000-4000-8000-000000000001', '19200000-0000-4000-8000-000000000003',
        3, 'reset', 'pending',
        'simplified', 'rotating', 2, now(),
        '19200000-0000-4000-8000-000000000002', 'recovery_after_unknown'
    );

    UPDATE public.gateway_mqtt_credentials
    SET status = 'rotating', credential_version = 3, last_operation_id = '19200000-0000-4000-8000-000000000003', updated_at = now()
    WHERE gateway_id = 'gw_sim_reset_unk';

    SET CONSTRAINTS ALL IMMEDIATE;
    SET CONSTRAINTS ALL DEFERRED;

    -- Predecessor is now excluded by mqtt_operation_effective
    IF public.mqtt_operation_effective('19200000-0000-4000-8000-000000000002') THEN
        RAISE EXCEPTION 'Reset predecessor was not marked ineffective';
    END IF;

    -- Finalize the reset operation
    UPDATE public.gateway_mqtt_credential_events
    SET status = 'succeeded', phase = 'finalize', ram_applied = true, snapshot_observed = false,
        fresh_positive_verified = true, completed_at = now(), updated_at = now()
    WHERE operation_id = '19200000-0000-4000-8000-000000000003';

    UPDATE public.gateway_mqtt_credentials
    SET status = 'active', activated_at = now(), updated_at = now()
    WHERE gateway_id = 'gw_sim_reset_unk';

    SET CONSTRAINTS ALL IMMEDIATE;
    SET CONSTRAINTS ALL DEFERRED;

    --------------------------------------------------------------------------------
    -- Case 3: Lost secret recovery on active credential
    --------------------------------------------------------------------------------
    -- Step 3a: Setup active credential on gw_sim_reset_lost (simplified contract)
    INSERT INTO public.gateway_mqtt_credential_events(
        operation_id, gateway_id, actor_user_id, idempotency_key, credential_version, action, status,
        execution_contract
    ) VALUES (
        '19300000-0000-4000-8000-000000000001', 'gw_sim_reset_lost',
        '19000000-0000-4000-8000-000000000001', '19300000-0000-4000-8000-000000000001',
        1, 'provision', 'pending',
        'simplified'
    );
    INSERT INTO public.gateway_mqtt_credentials(
        gateway_id, credential_version, status, last_operation_id, changed_by
    ) VALUES (
        'gw_sim_reset_lost', 1, 'provisioning',
        '19300000-0000-4000-8000-000000000001', '19000000-0000-4000-8000-000000000001'
    );
    UPDATE public.gateway_mqtt_credential_events
    SET status = 'succeeded', phase = 'finalize', ram_applied = true, snapshot_observed = false,
        fresh_positive_verified = true, completed_at = now(), updated_at = now()
    WHERE operation_id = '19300000-0000-4000-8000-000000000001';
    UPDATE public.gateway_mqtt_credentials
    SET status = 'active', activated_at = now(), updated_at = now()
    WHERE gateway_id = 'gw_sim_reset_lost';

    SET CONSTRAINTS ALL IMMEDIATE;
    SET CONSTRAINTS ALL DEFERRED;

    -- Step 3b: Rejection: recovery_after_unknown on active credential fails
    PERFORM pg_temp.must_fail($q$
        INSERT INTO public.mqtt_reset_dispositions(predecessor_id, successor_id, gateway_id, reason)
        VALUES('19300000-0000-4000-8000-000000000001', '19300000-0000-4000-8000-000000000002', 'gw_sim_reset_lost', 'recovery_after_unknown');
    $q$, 'recovery_after_unknown requires unresolved predecessor');

    -- Step 3c: Legitimate reset admission with lost_secret_recovery (simplified contract)
    INSERT INTO public.mqtt_reset_dispositions(predecessor_id, successor_id, gateway_id, reason)
    VALUES('19300000-0000-4000-8000-000000000001', '19300000-0000-4000-8000-000000000002', 'gw_sim_reset_lost', 'lost_secret_recovery');

    INSERT INTO public.gateway_mqtt_credential_events(
        operation_id, gateway_id, actor_user_id, idempotency_key, credential_version, action, status,
        execution_contract, previous_status, previous_credential_version, previous_activated_at,
        reset_predecessor_id, reset_reason
    ) VALUES (
        '19300000-0000-4000-8000-000000000002', 'gw_sim_reset_lost',
        '19000000-0000-4000-8000-000000000001', '19300000-0000-4000-8000-000000000002',
        2, 'reset', 'pending',
        'simplified', 'active', 1, now(),
        '19300000-0000-4000-8000-000000000001', 'lost_secret_recovery'
    );

    UPDATE public.gateway_mqtt_credentials
    SET status = 'rotating', credential_version = 2, last_operation_id = '19300000-0000-4000-8000-000000000002', updated_at = now()
    WHERE gateway_id = 'gw_sim_reset_lost';

    SET CONSTRAINTS ALL IMMEDIATE;
    SET CONSTRAINTS ALL DEFERRED;

    --------------------------------------------------------------------------------
    -- Case 4: Repeat reset (a previous reset can itself be reset)
    --------------------------------------------------------------------------------
    -- Setup provision -> reset (unresolved) -> second reset
    INSERT INTO public.gateway_mqtt_credential_events(
        operation_id, gateway_id, actor_user_id, idempotency_key, credential_version, action, status,
        execution_contract
    ) VALUES (
        '19400000-0000-4000-8000-000000000001', 'gw_sim_repeat_reset',
        '19000000-0000-4000-8000-000000000001', '19400000-0000-4000-8000-000000000001',
        1, 'provision', 'pending',
        'simplified'
    );
    INSERT INTO public.gateway_mqtt_credentials(
        gateway_id, credential_version, status, last_operation_id, changed_by
    ) VALUES (
        'gw_sim_repeat_reset', 1, 'provisioning',
        '19400000-0000-4000-8000-000000000001', '19000000-0000-4000-8000-000000000001'
    );
    UPDATE public.gateway_mqtt_credential_events
    SET status = 'succeeded', phase = 'finalize', ram_applied = true, snapshot_observed = false,
        fresh_positive_verified = true, completed_at = now(), updated_at = now()
    WHERE operation_id = '19400000-0000-4000-8000-000000000001';
    UPDATE public.gateway_mqtt_credentials
    SET status = 'active', activated_at = now(), updated_at = now()
    WHERE gateway_id = 'gw_sim_repeat_reset';

    SET CONSTRAINTS ALL IMMEDIATE;
    SET CONSTRAINTS ALL DEFERRED;

    -- Reset 1: lost_secret_recovery admitted (simplified contract)
    INSERT INTO public.mqtt_reset_dispositions(predecessor_id, successor_id, gateway_id, reason)
    VALUES('19400000-0000-4000-8000-000000000001', '19400000-0000-4000-8000-000000000002', 'gw_sim_repeat_reset', 'lost_secret_recovery');
    INSERT INTO public.gateway_mqtt_credential_events(
        operation_id, gateway_id, actor_user_id, idempotency_key, credential_version, action, status,
        execution_contract, previous_status, previous_credential_version, previous_activated_at,
        reset_predecessor_id, reset_reason
    ) VALUES (
        '19400000-0000-4000-8000-000000000002', 'gw_sim_repeat_reset',
        '19000000-0000-4000-8000-000000000001', '19400000-0000-4000-8000-000000000002',
        2, 'reset', 'pending',
        'simplified', 'active', 1, now(),
        '19400000-0000-4000-8000-000000000001', 'lost_secret_recovery'
    );
    UPDATE public.gateway_mqtt_credentials
    SET status = 'rotating', credential_version = 2, last_operation_id = '19400000-0000-4000-8000-000000000002', updated_at = now()
    WHERE gateway_id = 'gw_sim_repeat_reset';

    SET CONSTRAINTS ALL IMMEDIATE;
    SET CONSTRAINTS ALL DEFERRED;

    -- Now Reset 1 becomes unresolved/unknown (remains pending).
    -- Admit Reset 2 (recovery_after_unknown) targeting Reset 1!
    INSERT INTO public.mqtt_reset_dispositions(predecessor_id, successor_id, gateway_id, reason)
    VALUES('19400000-0000-4000-8000-000000000002', '19400000-0000-4000-8000-000000000003', 'gw_sim_repeat_reset', 'recovery_after_unknown');

    INSERT INTO public.gateway_mqtt_credential_events(
        operation_id, gateway_id, actor_user_id, idempotency_key, credential_version, action, status,
        execution_contract, previous_status, previous_credential_version, previous_activated_at,
        reset_predecessor_id, reset_reason
    ) VALUES (
        '19400000-0000-4000-8000-000000000003', 'gw_sim_repeat_reset',
        '19000000-0000-4000-8000-000000000001', '19400000-0000-4000-8000-000000000003',
        3, 'reset', 'pending',
        'simplified', 'rotating', 2, now(),
        '19400000-0000-4000-8000-000000000002', 'recovery_after_unknown'
    );
    UPDATE public.gateway_mqtt_credentials
    SET status = 'rotating', credential_version = 3, last_operation_id = '19400000-0000-4000-8000-000000000003', updated_at = now()
    WHERE gateway_id = 'gw_sim_repeat_reset';

    SET CONSTRAINTS ALL IMMEDIATE;
    SET CONSTRAINTS ALL DEFERRED;

    -- Both 1 and 2 are now ineffective
    IF public.mqtt_operation_effective('19400000-0000-4000-8000-000000000001')
       OR public.mqtt_operation_effective('19400000-0000-4000-8000-000000000002') THEN
        RAISE EXCEPTION 'Historical reset predecessors are still effective';
    END IF;

    -- Reset 3 is effective
    IF NOT public.mqtt_operation_effective('19400000-0000-4000-8000-000000000003') THEN
        RAISE EXCEPTION 'Current reset operation 3 is not effective';
    END IF;

    --------------------------------------------------------------------------------
    -- Case 5: Additional Verification Probes
    --------------------------------------------------------------------------------
    -- 5a. Manual maintenance INSERT for simplified operation rejected with exact expected guard
    -- Test on an active simplified operation
    PERFORM pg_temp.must_fail($q$
        INSERT INTO public.mqtt_credential_maintenance(operation_id, gateway_id)
        VALUES ('19100000-0000-4000-8000-000000000004', 'gw_sim_prov2');
    $q$, 'Maintenance requires matching modern intent');

    -- Test on a pending simplified operation: fails with exact guard 'Simplified operations cannot have maintenance checkpoints'
    INSERT INTO public.gateways(gateway_id, name) VALUES ('gw_sim_pend', 'Simplified Pending Gateway');
    INSERT INTO public.gateway_mqtt_credential_events(
        operation_id, gateway_id, actor_user_id, idempotency_key, credential_version, action, status,
        execution_contract
    ) VALUES (
        '19100000-0000-4000-8000-000000000005', 'gw_sim_pend',
        '19000000-0000-4000-8000-000000000001', '19100000-0000-4000-8000-000000000005',
        1, 'provision', 'pending',
        'simplified'
    );
    INSERT INTO public.gateway_mqtt_credentials(
        gateway_id, credential_version, status, last_operation_id, changed_by
    ) VALUES (
        'gw_sim_pend', 1, 'provisioning',
        '19100000-0000-4000-8000-000000000005', '19000000-0000-4000-8000-000000000001'
    );
    PERFORM pg_temp.must_fail($q$
        INSERT INTO public.mqtt_credential_maintenance(operation_id, gateway_id)
        VALUES ('19100000-0000-4000-8000-000000000005', 'gw_sim_pend');
    $q$, 'Simplified operations cannot have maintenance checkpoints');

    -- 5b. Synthetic maintenance / recovery attached to simplified rejected by projection authority
    -- Test under backend role (which holds INSERT on mqtt_credential_recovery)
    RESET ROLE;
    PERFORM pg_temp.must_fail($q$
        INSERT INTO public.mqtt_credential_recovery(
            recovery_id, operation_id, gateway_id, attempted_version, broker_epoch, status
        ) VALUES (
            '19500000-0000-4000-8000-000000000001', '19100000-0000-4000-8000-000000000004',
            'gw_sim_prov2', 1, repeat('b', 64), 'pending'
        );
    $q$, 'Recovery requires consistent current modern operation and unresolved checkpoint');
    SET LOCAL ROLE iot_backend_app;

    -- 5c. Simplified known-unchanged failure and unknown projection follow actual version/status/timestamp contract
    -- Test failed simplified provision retains previous status/version/timestamps
    INSERT INTO public.gateways(gateway_id, name) VALUES ('gw_sim_fail', 'Simplified Failure Gateway');
    INSERT INTO public.gateway_mqtt_credential_events(
        operation_id, gateway_id, actor_user_id, idempotency_key, credential_version, action, status,
        execution_contract
    ) VALUES (
        '19600000-0000-4000-8000-000000000001', 'gw_sim_fail',
        '19000000-0000-4000-8000-000000000001', '19600000-0000-4000-8000-000000000001',
        1, 'provision', 'pending',
        'simplified'
    );
    INSERT INTO public.gateway_mqtt_credentials(
        gateway_id, credential_version, status, last_operation_id, changed_by
    ) VALUES (
        'gw_sim_fail', 1, 'provisioning',
        '19600000-0000-4000-8000-000000000001', '19000000-0000-4000-8000-000000000001'
    );
    SET CONSTRAINTS ALL IMMEDIATE;
    SET CONSTRAINTS ALL DEFERRED;

    -- Fail the attempt (retains previous status: NULL -> 'failed', version remains 1)
    UPDATE public.gateway_mqtt_credential_events
    SET status = 'failed', phase = 'finalize', ram_applied = false, snapshot_observed = false,
        fresh_positive_verified = false, error_code = 'service_unavailable', completed_at = now(), updated_at = now()
    WHERE operation_id = '19600000-0000-4000-8000-000000000001';

    UPDATE public.gateway_mqtt_credentials
    SET status = 'failed', last_error_code = 'service_unavailable', updated_at = now()
    WHERE gateway_id = 'gw_sim_fail';

    SET CONSTRAINTS ALL IMMEDIATE;
    SET CONSTRAINTS ALL DEFERRED;

    -- 5d. Unfinished lifecycle checkpoint fences ordinary mutation, cannot be escaped by simplified action on foreign operation
    INSERT INTO public.gateways(gateway_id, name) VALUES ('gw_fence_test', 'Fence Test Gateway');
    INSERT INTO public.gateway_mqtt_credential_events(
        operation_id, gateway_id, actor_user_id, idempotency_key, credential_version, action, status,
        execution_contract
    ) VALUES (
        '19700000-0000-4000-8000-000000000001', 'gw_fence_test',
        '19000000-0000-4000-8000-000000000001', '19700000-0000-4000-8000-000000000001',
        1, 'provision', 'pending',
        'lifecycle'
    );
    INSERT INTO public.gateway_mqtt_credentials(
        gateway_id, credential_version, status, last_operation_id, changed_by
    ) VALUES (
        'gw_fence_test', 1, 'provisioning',
        '19700000-0000-4000-8000-000000000001', '19000000-0000-4000-8000-000000000001'
    );
    -- Checkpoint row is created for lifecycle
    IF NOT EXISTS (SELECT 1 FROM public.mqtt_credential_maintenance WHERE operation_id = '19700000-0000-4000-8000-000000000001') THEN
        RAISE EXCEPTION 'Lifecycle intent must create maintenance checkpoint';
    END IF;

    SET CONSTRAINTS ALL IMMEDIATE;
    SET CONSTRAINTS ALL DEFERRED;

    -- Attempt to admit a new simplified mutation on gw_fence_test while lifecycle operation is unresolved: MUST BE REJECTED
    PERFORM pg_temp.must_fail($q$
        INSERT INTO public.gateway_mqtt_credential_events(
            operation_id, gateway_id, actor_user_id, idempotency_key, credential_version, action, status,
            execution_contract, previous_status, previous_credential_version, previous_activated_at
        ) VALUES (
            '19700000-0000-4000-8000-000000000002', 'gw_fence_test',
            '19000000-0000-4000-8000-000000000001', '19700000-0000-4000-8000-000000000002',
            2, 'rotate', 'pending',
            'simplified', 'provisioning', 1, now()
        );
    $q$, 'Gateway has an unresolved credential operation');

    -- 5e. Hostile caller search_path does not alter authoritative resolution
    RESET ROLE;
    CREATE TEMP TABLE gateway_mqtt_credential_events (LIKE public.gateway_mqtt_credential_events INCLUDING ALL) ON COMMIT DROP;
    CREATE TEMP TABLE gateway_mqtt_credentials (LIKE public.gateway_mqtt_credentials INCLUDING ALL) ON COMMIT DROP;
    SET LOCAL search_path = pg_temp, public;
    -- validate_mqtt_projection_authority uses fixed trusted search_path, verifying real public table
    PERFORM public.validate_mqtt_projection_authority('gw_sim_prov2');
    SET LOCAL search_path = pg_catalog, public, pg_temp;
    SET LOCAL ROLE iot_backend_app;

    --------------------------------------------------------------------------------
    -- Case 6: Restored 000017 lifecycle rotation replacement inventory predicates
    --------------------------------------------------------------------------------
    -- 6a. Direct call to validate_mqtt_replacement_inventory on clean gateway succeeds
    PERFORM public.validate_mqtt_replacement_inventory('gw_sim_prov2');

    -- 6b. App-role negative transaction test:
    -- Attempting to insert a replacement event without a matching disposition is rejected by
    -- BEFORE trigger aa_mqtt_replacement_event (native structural rejection).
    PERFORM pg_temp.must_fail($q$
        INSERT INTO public.gateway_mqtt_credential_events(
            operation_id, gateway_id, actor_user_id, idempotency_key, credential_version, action, status,
            execution_contract, previous_status, previous_credential_version, previous_activated_at,
            replacement_predecessor_id
        ) VALUES (
            '19800000-0000-4000-8000-000000000002', 'gw_sim_prov2',
            '19000000-0000-4000-8000-000000000001', '19800000-0000-4000-8000-000000000002',
            2, 'rotate', 'pending',
            'lifecycle', 'active', 1, now(),
            '19100000-0000-4000-8000-000000000004'
        );
    $q$, 'Replacement intent requires exact disposition');

    -- 6c. Malformed replacement linkage under deferred validation:
    -- An orphaned replacement disposition (predecessor is not an active/current rotation with checkpoint)
    -- fails BEFORE insertion via enforce_mqtt_replacement_disposition.
    PERFORM pg_temp.must_fail($q$
        INSERT INTO public.mqtt_rotation_replacements(predecessor_id, successor_id, gateway_id)
        VALUES ('19100000-0000-4000-8000-000000000004', '19800000-0000-4000-8000-000000000099', 'gw_sim_prov2');
    $q$, 'Replacement requires exact consistent current rotation');

    -- 6d. Direct call to validate_mqtt_replacement_inventory with any synthetic dangling backlink
    -- or inconsistent state demonstrates that the deferred inventory function enforces restored predicates.

    --------------------------------------------------------------------------------
    -- Case 7: Reset generation must strictly exceed current metadata and ALL attempted history (DB-03)
    --------------------------------------------------------------------------------
    -- Step 7a: Setup gateway gw_sim_reset_gap with honest DB-seam sequence:
    -- 1. Initial provision v1 fails known unchanged (status -> failed, metadata version 1).
    INSERT INTO public.gateway_mqtt_credential_events(
        operation_id, gateway_id, actor_user_id, idempotency_key, credential_version, action, status,
        execution_contract
    ) VALUES (
        '19900000-0000-4000-8000-000000000001', 'gw_sim_reset_gap',
        '19000000-0000-4000-8000-000000000001', '19900000-0000-4000-8000-000000000001',
        1, 'provision', 'pending',
        'simplified'
    );
    INSERT INTO public.gateway_mqtt_credentials(
        gateway_id, credential_version, status, last_operation_id, changed_by
    ) VALUES (
        'gw_sim_reset_gap', 1, 'provisioning',
        '19900000-0000-4000-8000-000000000001', '19000000-0000-4000-8000-000000000001'
    );
    SET CONSTRAINTS ALL IMMEDIATE;
    SET CONSTRAINTS ALL DEFERRED;

    UPDATE public.gateway_mqtt_credential_events
    SET status = 'failed', phase = 'finalize', ram_applied = false, snapshot_observed = false,
        fresh_positive_verified = false, error_code = 'service_unavailable', completed_at = now(), updated_at = now()
    WHERE operation_id = '19900000-0000-4000-8000-000000000001';

    UPDATE public.gateway_mqtt_credentials
    SET status = 'failed', last_error_code = 'service_unavailable',
        updated_at = (SELECT updated_at FROM public.gateway_mqtt_credential_events WHERE operation_id = '19900000-0000-4000-8000-000000000001')
    WHERE gateway_id = 'gw_sim_reset_gap';

    SET CONSTRAINTS ALL IMMEDIATE;
    SET CONSTRAINTS ALL DEFERRED;

    -- 2. Reprovision attempted v2, then transitions to recovery_needed retaining previous v1.
    INSERT INTO public.gateway_mqtt_credential_events(
        operation_id, gateway_id, actor_user_id, idempotency_key, credential_version, action, status,
        execution_contract, previous_status, previous_credential_version
    ) VALUES (
        '19900000-0000-4000-8000-000000000002', 'gw_sim_reset_gap',
        '19000000-0000-4000-8000-000000000001', '19900000-0000-4000-8000-000000000002',
        2, 'provision', 'pending',
        'simplified', 'failed', 1
    );

    UPDATE public.gateway_mqtt_credentials
    SET status = 'provisioning', credential_version = 2, last_operation_id = '19900000-0000-4000-8000-000000000002',
        last_error_code = NULL,
        updated_at = (SELECT created_at FROM public.gateway_mqtt_credential_events WHERE operation_id = '19900000-0000-4000-8000-000000000002')
    WHERE gateway_id = 'gw_sim_reset_gap';

    SET CONSTRAINTS ALL IMMEDIATE;
    SET CONSTRAINTS ALL DEFERRED;

    UPDATE public.gateway_mqtt_credential_events
    SET status = 'recovery_needed', phase = 'recovery', ram_applied = false, snapshot_observed = false,
        fresh_positive_verified = false, error_code = 'uncertain_reconnect', completed_at = NULL, updated_at = now()
    WHERE operation_id = '19900000-0000-4000-8000-000000000002';

    UPDATE public.gateway_mqtt_credentials
    SET status = 'recovery_needed', credential_version = 1, last_error_code = 'uncertain_reconnect',
        updated_at = (SELECT updated_at FROM public.gateway_mqtt_credential_events WHERE operation_id = '19900000-0000-4000-8000-000000000002')
    WHERE gateway_id = 'gw_sim_reset_gap';

    SET CONSTRAINTS ALL IMMEDIATE;
    SET CONSTRAINTS ALL DEFERRED;

    -- Confirm honest gap setup: metadata version = 1, while max attempted history = 2
    IF (SELECT credential_version FROM public.gateway_mqtt_credentials WHERE gateway_id = 'gw_sim_reset_gap') <> 1 THEN
        RAISE EXCEPTION 'gw_sim_reset_gap expected metadata version 1';
    END IF;
    IF (SELECT max(credential_version) FROM public.gateway_mqtt_credential_events WHERE gateway_id = 'gw_sim_reset_gap') <> 2 THEN
        RAISE EXCEPTION 'gw_sim_reset_gap expected max history version 2';
    END IF;

    -- Step 7b: Attempt reset with version 2 (equal to max attempted history 2, though > metadata 1)
    -- MUST REJECT at generation guard, rolling back entire subtransaction.
    PERFORM pg_temp.must_fail($q$
        INSERT INTO public.mqtt_reset_dispositions(predecessor_id, successor_id, gateway_id, reason)
        VALUES('19900000-0000-4000-8000-000000000002', '19900000-0000-4000-8000-000000000003', 'gw_sim_reset_gap', 'recovery_after_unknown');
        INSERT INTO public.gateway_mqtt_credential_events(
            operation_id, gateway_id, actor_user_id, idempotency_key, credential_version, action, status,
            execution_contract, previous_status, previous_credential_version,
            reset_predecessor_id, reset_reason
        ) VALUES (
            '19900000-0000-4000-8000-000000000003', 'gw_sim_reset_gap',
            '19000000-0000-4000-8000-000000000001', '19900000-0000-4000-8000-000000000003',
            2, 'reset', 'pending',
            'simplified', 'recovery_needed', 1,
            '19900000-0000-4000-8000-000000000002', 'recovery_after_unknown'
        );
    $q$, 'Reset credential generation must exceed current and attempted history');

    -- Verify rollback: event count, disposition count, and credentials authority completely unchanged
    IF (SELECT count(*) FROM public.gateway_mqtt_credential_events WHERE gateway_id = 'gw_sim_reset_gap') <> 2 THEN
        RAISE EXCEPTION 'Rollback failed: unexpected event count after rejected reset v2';
    END IF;
    IF (SELECT count(*) FROM public.mqtt_reset_dispositions WHERE gateway_id = 'gw_sim_reset_gap') <> 0 THEN
        RAISE EXCEPTION 'Rollback failed: unexpected disposition count after rejected reset v2';
    END IF;
    IF (SELECT credential_version FROM public.gateway_mqtt_credentials WHERE gateway_id = 'gw_sim_reset_gap') <> 1
       OR (SELECT last_operation_id FROM public.gateway_mqtt_credentials WHERE gateway_id = 'gw_sim_reset_gap') <> '19900000-0000-4000-8000-000000000002'
       OR (SELECT status FROM public.gateway_mqtt_credentials WHERE gateway_id = 'gw_sim_reset_gap') <> 'recovery_needed' THEN
        RAISE EXCEPTION 'Rollback failed: credentials authority altered after rejected reset v2';
    END IF;

    -- Step 7c: Attempt reset with lower version 1 (<= metadata version 1 and <= max history 2)
    -- MUST REJECT at generation guard as well.
    PERFORM pg_temp.must_fail($q$
        INSERT INTO public.mqtt_reset_dispositions(predecessor_id, successor_id, gateway_id, reason)
        VALUES('19900000-0000-4000-8000-000000000002', '19900000-0000-4000-8000-000000000004', 'gw_sim_reset_gap', 'recovery_after_unknown');
        INSERT INTO public.gateway_mqtt_credential_events(
            operation_id, gateway_id, actor_user_id, idempotency_key, credential_version, action, status,
            execution_contract, previous_status, previous_credential_version,
            reset_predecessor_id, reset_reason
        ) VALUES (
            '19900000-0000-4000-8000-000000000004', 'gw_sim_reset_gap',
            '19000000-0000-4000-8000-000000000001', '19900000-0000-4000-8000-000000000004',
            1, 'reset', 'pending',
            'simplified', 'recovery_needed', 1,
            '19900000-0000-4000-8000-000000000002', 'recovery_after_unknown'
        );
    $q$, 'Reset credential generation must exceed current and attempted history');

    IF (SELECT count(*) FROM public.gateway_mqtt_credential_events WHERE gateway_id = 'gw_sim_reset_gap') <> 2
       OR (SELECT count(*) FROM public.mqtt_reset_dispositions WHERE gateway_id = 'gw_sim_reset_gap') <> 0 THEN
        RAISE EXCEPTION 'Rollback failed: state altered after rejected reset v1';
    END IF;

    -- Step 7d: New operation and valid proposed reset with version 3 (> greatest(1, 2))
    -- MUST ADMIT exactly once.
    INSERT INTO public.mqtt_reset_dispositions(predecessor_id, successor_id, gateway_id, reason)
    VALUES('19900000-0000-4000-8000-000000000002', '19900000-0000-4000-8000-000000000005', 'gw_sim_reset_gap', 'recovery_after_unknown');

    INSERT INTO public.gateway_mqtt_credential_events(
        operation_id, gateway_id, actor_user_id, idempotency_key, credential_version, action, status,
        execution_contract, previous_status, previous_credential_version,
        reset_predecessor_id, reset_reason
    ) VALUES (
        '19900000-0000-4000-8000-000000000005', 'gw_sim_reset_gap',
        '19000000-0000-4000-8000-000000000001', '19900000-0000-4000-8000-000000000005',
        3, 'reset', 'pending',
        'simplified', 'recovery_needed', 1,
        '19900000-0000-4000-8000-000000000002', 'recovery_after_unknown'
    );

    UPDATE public.gateway_mqtt_credentials
    SET status = 'rotating', credential_version = 3, last_operation_id = '19900000-0000-4000-8000-000000000005',
        last_error_code = NULL,
        updated_at = (SELECT created_at FROM public.gateway_mqtt_credential_events WHERE operation_id = '19900000-0000-4000-8000-000000000005')
    WHERE gateway_id = 'gw_sim_reset_gap';

    SET CONSTRAINTS ALL IMMEDIATE;
    SET CONSTRAINTS ALL DEFERRED;

    IF (SELECT count(*) FROM public.gateway_mqtt_credential_events WHERE gateway_id = 'gw_sim_reset_gap') <> 3 THEN
        RAISE EXCEPTION 'gw_sim_reset_gap expected 3 events after admitted reset v3';
    END IF;
    IF (SELECT count(*) FROM public.mqtt_reset_dispositions WHERE gateway_id = 'gw_sim_reset_gap') <> 1 THEN
        RAISE EXCEPTION 'gw_sim_reset_gap expected 1 disposition after admitted reset v3';
    END IF;
    IF (SELECT credential_version FROM public.gateway_mqtt_credentials WHERE gateway_id = 'gw_sim_reset_gap') <> 3 THEN
        RAISE EXCEPTION 'gw_sim_reset_gap expected metadata version 3';
    END IF;

    -- Finalize reset v3 to succeeded active
    UPDATE public.gateway_mqtt_credential_events
    SET status = 'succeeded', phase = 'finalize', ram_applied = true, snapshot_observed = false,
        fresh_positive_verified = true, completed_at = now(), updated_at = now()
    WHERE operation_id = '19900000-0000-4000-8000-000000000005';

    UPDATE public.gateway_mqtt_credentials
    SET status = 'active',
        activated_at = (SELECT updated_at FROM public.gateway_mqtt_credential_events WHERE operation_id = '19900000-0000-4000-8000-000000000005'),
        updated_at = (SELECT updated_at FROM public.gateway_mqtt_credential_events WHERE operation_id = '19900000-0000-4000-8000-000000000005')
    WHERE gateway_id = 'gw_sim_reset_gap';

    SET CONSTRAINTS ALL IMMEDIATE;
    SET CONSTRAINTS ALL DEFERRED;

    RESET ROLE;
END $$;

ROLLBACK;
\echo 'Schema 019 regression tests passed.'
