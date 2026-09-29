\set ON_ERROR_STOP on

BEGIN TRANSACTION READ ONLY;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM schema_migrations
        WHERE version = 10 AND name = 'stage2_auth_and_provisioning'
    ) THEN
        RAISE EXCEPTION 'Verification failed: migration 000010 is not recorded';
    END IF;

    IF to_regclass('public.platform_admins') IS NULL
       OR to_regclass('public.gateway_mqtt_credentials') IS NULL
       OR to_regclass('public.gateway_mqtt_credential_events') IS NULL THEN
        RAISE EXCEPTION 'Verification failed: a Stage 2 administration table is missing';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'chk_gateways_identifier'
          AND conrelid = 'gateways'::regclass
    ) OR NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'chk_sensors_identifier'
          AND conrelid = 'sensors'::regclass
    ) THEN
        RAISE EXCEPTION 'Verification failed: identifier constraints are missing';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'enforce_gateway_mqtt_event_transition'
          AND tgrelid = 'gateway_mqtt_credential_events'::regclass
          AND NOT tgisinternal
    ) THEN
        RAISE EXCEPTION 'Verification failed: credential event transition trigger is missing';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'fk_gateway_mqtt_credentials_last_operation'
          AND conrelid = 'gateway_mqtt_credentials'::regclass
    ) OR NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'chk_gateway_mqtt_credentials_timestamps'
          AND conrelid = 'gateway_mqtt_credentials'::regclass
    ) OR NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'chk_gateway_mqtt_credential_events_completion'
          AND conrelid = 'gateway_mqtt_credential_events'::regclass
    ) THEN
        RAISE EXCEPTION 'Verification failed: credential integrity constraints are missing';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE schemaname = 'public'
          AND indexname = 'idx_user_gateways_gateway_user'
    ) THEN
        RAISE EXCEPTION 'Verification failed: reverse authorization index is missing';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_roles
        WHERE rolname = 'iot_backend'
          AND rolbypassrls
          AND NOT rolsuper
          AND NOT rolcreatedb
          AND NOT rolcreaterole
    ) OR NOT EXISTS (
        SELECT 1 FROM pg_roles
        WHERE rolname = 'iot_backend_app'
          AND rolcanlogin
          AND rolbypassrls
          AND NOT rolsuper
          AND pg_has_role('iot_backend_app', 'iot_backend', 'MEMBER')
    ) THEN
        RAISE EXCEPTION 'Verification failed: backend database roles are not least privilege';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_auth_members AS membership
        JOIN pg_roles AS member_role ON member_role.oid = membership.member
        JOIN pg_roles AS granted_role ON granted_role.oid = membership.roleid
        WHERE (
            member_role.rolname IN ('iot_backend', 'iot_backend_app')
            OR granted_role.rolname IN ('iot_backend', 'iot_backend_app')
        )
          AND NOT (
              member_role.rolname = 'iot_backend_app'
              AND granted_role.rolname = 'iot_backend'
          )
    ) THEN
        RAISE EXCEPTION 'Verification failed: backend role has unexpected membership';
    END IF;

    IF has_database_privilege('iot_backend_app', current_database(), 'CREATE')
       OR has_schema_privilege('iot_backend_app', 'public', 'CREATE') THEN
        RAISE EXCEPTION 'Verification failed: backend login role has DDL privilege';
    END IF;

    IF has_table_privilege('iot_backend', 'schema_migrations', 'INSERT')
       OR has_schema_privilege('iot_backend', 'public', 'CREATE')
       OR NOT has_table_privilege('iot_backend', 'gateways', 'SELECT')
       OR NOT has_table_privilege('iot_backend', 'gateway_mqtt_credentials', 'INSERT')
       OR NOT has_table_privilege('iot_backend', 'platform_admins', 'SELECT')
       OR has_table_privilege('iot_backend', 'platform_admins', 'INSERT')
       OR has_table_privilege('iot_backend', 'platform_admins', 'DELETE')
       OR has_table_privilege('iot_backend', 'gateway_mqtt_credential_events', 'DELETE')
       OR has_table_privilege('iot_backend', 'telemetry', 'UPDATE')
       OR has_table_privilege('iot_backend', 'processed_messages', 'DELETE') THEN
        RAISE EXCEPTION 'Verification failed: backend role grants are incorrect';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_class
        WHERE oid IN (
            'platform_admins'::regclass,
            'gateway_mqtt_credentials'::regclass,
            'gateway_mqtt_credential_events'::regclass
        )
          AND NOT relrowsecurity
    ) THEN
        RAISE EXCEPTION 'Verification failed: RLS is not enabled on a Stage 2 table';
    END IF;
END
$$;

ROLLBACK;

\echo 'Migration 000010 verification passed.'
