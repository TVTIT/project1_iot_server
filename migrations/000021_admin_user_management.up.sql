-- Migration 000021: Admin User Management
-- Grants least-privilege permissions to iot_backend for admin user management,
-- and creates security-definer function for bounded allowlisted user projection.
\set ON_ERROR_STOP on
SELECT pg_advisory_lock(3290, 2);
SET search_path = pg_catalog, public, pg_temp;

SELECT NOT EXISTS (SELECT 1 FROM public.schema_migrations WHERE version = 21) AS apply_admin_user_management \gset
\if :apply_admin_user_management

SELECT to_regclass('auth.users') IS NOT NULL AS auth_ready \gset
\if :auth_ready

BEGIN;
SET LOCAL lock_timeout = '10s';

-- 1. Assert predecessor migration version 20 exists and check prerequisites
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM public.schema_migrations WHERE version = 20 AND name = 'stage3_telemetry_ingestion'
    ) THEN
        RAISE EXCEPTION 'Predecessor migration version 20 stage3_telemetry_ingestion must be applied before migration 000021';
    END IF;

    IF EXISTS (
        SELECT 1 FROM public.schema_migrations WHERE version = 21 AND name <> 'admin_user_management'
    ) THEN
        RAISE EXCEPTION 'Migration version 21 has an unexpected name';
    END IF;

    -- public schema must remain trusted; fail if unsafe CREATE privileges exist
    IF has_schema_privilege('iot_backend', 'public', 'CREATE')
       OR has_schema_privilege('iot_backend_app', 'public', 'CREATE')
       OR EXISTS (SELECT 1 FROM pg_namespace n, LATERAL aclexplode(COALESCE(n.nspacl, acldefault('n', n.nspowner))) a
                  WHERE n.nspname = 'public' AND a.grantee = 0 AND a.privilege_type = 'CREATE') THEN
        RAISE EXCEPTION 'Migration requires trusted public schema';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'iot_backend') THEN
        RAISE EXCEPTION 'Role iot_backend must exist before migration 000021';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'iot_backend_app') THEN
        RAISE EXCEPTION 'Role iot_backend_app must exist before migration 000021';
    END IF;

    -- Prerequisites: profiles, and trigger on_auth_user_created must exist
    IF to_regclass('public.profiles') IS NULL THEN
        RAISE EXCEPTION 'Prerequisite public.profiles table is missing';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger t
        JOIN pg_class c ON t.tgrelid = c.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE n.nspname = 'auth' AND c.relname = 'users' AND t.tgname = 'on_auth_user_created'
    ) THEN
        RAISE EXCEPTION 'Prerequisite trigger on_auth_user_created on auth.users is missing';
    END IF;
END
$$;

-- 2. Security-definer read function for allowlisted user projection
CREATE OR REPLACE FUNCTION public.list_admin_users_projection(
    p_actor_id UUID,
    p_limit INT
)
RETURNS TABLE (
    user_id UUID,
    email TEXT,
    full_name TEXT,
    platform_role TEXT,
    created_at TIMESTAMPTZ,
    profile_exists BOOLEAN
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $$
BEGIN
    IF p_actor_id IS NULL THEN
        RAISE EXCEPTION 'actor_id is required';
    END IF;
    IF p_limit IS NOT NULL AND p_limit < 0 THEN
        RAISE EXCEPTION 'limit must be non-negative';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM public.platform_admins pa WHERE pa.user_id = p_actor_id
    ) THEN
        RAISE EXCEPTION 'access denied: actor is not a platform admin';
    END IF;

    RETURN QUERY
    SELECT
        u.id AS user_id,
        u.email::TEXT AS email,
        p.full_name AS full_name,
        CASE WHEN pa.user_id IS NOT NULL THEN 'platform_admin' ELSE 'user' END AS platform_role,
        u.created_at AS created_at,
        (p.id IS NOT NULL) AS profile_exists
    FROM auth.users u
    LEFT JOIN public.profiles p ON p.id = u.id
    LEFT JOIN public.platform_admins pa ON pa.user_id = u.id
    ORDER BY u.created_at ASC, u.id ASC
    LIMIT p_limit;
END;
$$;

-- Revoke execute from public/anon/authenticated and grant only to iot_backend
REVOKE ALL ON FUNCTION public.list_admin_users_projection(UUID, INT) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.list_admin_users_projection(UUID, INT) FROM anon;
REVOKE ALL ON FUNCTION public.list_admin_users_projection(UUID, INT) FROM authenticated;
GRANT EXECUTE ON FUNCTION public.list_admin_users_projection(UUID, INT) TO iot_backend;

-- 3. Grant least-privilege mutations to iot_backend
-- Profiles: only update full_name
GRANT UPDATE (full_name) ON TABLE public.profiles TO iot_backend;

-- Platform admins: insert and delete for role management (SELECT already granted in migration 10)
GRANT INSERT, DELETE ON TABLE public.platform_admins TO iot_backend;

-- 4. Record migration in schema_migrations
INSERT INTO public.schema_migrations (version, name)
VALUES (21, 'admin_user_management');

COMMIT;

\else
\echo 'Supabase auth.users table not found. Skipping migration 000021_admin_user_management.'
\echo 'This migration must be applied after Supabase Auth is initialized.'
\endif

\else
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM public.schema_migrations WHERE version = 21 AND name = 'admin_user_management') THEN
        RAISE EXCEPTION 'Unexpected migration version 21';
    END IF;
END $$;
\endif

SELECT pg_advisory_unlock(3290, 2);
