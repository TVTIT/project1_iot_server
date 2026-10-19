\set ON_ERROR_STOP on
BEGIN;
DO $$
BEGIN
  -- 1. Migration tracking assertion
  IF NOT EXISTS (SELECT 1 FROM public.schema_migrations WHERE version=21 AND name='admin_user_management')
     OR (SELECT max(version) FROM public.schema_migrations) <> 21 THEN
    RAISE EXCEPTION 'migration 21 tracking is missing or unexpected';
  END IF;

  -- 2. Security definer function verification
  IF NOT EXISTS (
    SELECT 1 FROM pg_proc p
    JOIN pg_namespace n ON p.pronamespace = n.oid
    WHERE n.nspname = 'public'
      AND p.proname = 'list_admin_users_projection'
      AND p.prosecdef = true
      AND p.proconfig @> ARRAY['search_path=""']
  ) THEN
    RAISE EXCEPTION 'list_admin_users_projection must exist as SECURITY DEFINER with search_path locked';
  END IF;

  -- 3. Execute permissions verification
  IF NOT has_function_privilege('iot_backend', 'public.list_admin_users_projection(uuid, int)', 'EXECUTE')
     OR NOT has_function_privilege('iot_backend_app', 'public.list_admin_users_projection(uuid, int)', 'EXECUTE') THEN
    RAISE EXCEPTION 'iot_backend / iot_backend_app must have EXECUTE on list_admin_users_projection';
  END IF;

  IF has_function_privilege('anon', 'public.list_admin_users_projection(uuid, int)', 'EXECUTE')
     OR has_function_privilege('authenticated', 'public.list_admin_users_projection(uuid, int)', 'EXECUTE') THEN
    RAISE EXCEPTION 'human roles anon / authenticated must NOT have EXECUTE on list_admin_users_projection';
  END IF;

  -- 4. Profiles least-privilege verification
  IF NOT has_column_privilege('iot_backend', 'public.profiles', 'full_name', 'UPDATE')
     OR NOT has_column_privilege('iot_backend_app', 'public.profiles', 'full_name', 'UPDATE') THEN
    RAISE EXCEPTION 'iot_backend must have UPDATE on profiles.full_name';
  END IF;

  IF has_column_privilege('iot_backend_app', 'public.profiles', 'id', 'UPDATE') THEN
    RAISE EXCEPTION 'iot_backend_app must NOT have UPDATE on profiles.id';
  END IF;

  -- 5. Platform admins least-privilege verification
  IF NOT has_table_privilege('iot_backend_app', 'public.platform_admins', 'INSERT')
     OR NOT has_table_privilege('iot_backend_app', 'public.platform_admins', 'DELETE') THEN
    RAISE EXCEPTION 'iot_backend_app must have INSERT and DELETE on platform_admins';
  END IF;

  -- 6. Direct auth.users write restriction
  IF has_table_privilege('iot_backend_app', 'auth.users', 'INSERT')
     OR has_table_privilege('iot_backend_app', 'auth.users', 'UPDATE')
     OR has_table_privilege('iot_backend_app', 'auth.users', 'DELETE') THEN
    RAISE EXCEPTION 'iot_backend_app must NOT have write privileges on auth.users';
  END IF;

  -- 7. Schema create restriction
  IF has_schema_privilege('iot_backend_app', 'public', 'CREATE') THEN
    RAISE EXCEPTION 'iot_backend_app must NOT have CREATE on public schema';
  END IF;

END $$;
COMMIT;
SELECT 'Migration 000021 verification passed.';
