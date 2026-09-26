-- Supabase Database Roles Setup
-- These roles are required by Supabase Auth (GoTrue) and Supabase Storage API

DO $$
BEGIN
    -- 1. Create client roles
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'anon') THEN
        CREATE ROLE anon NOLOGIN NOINHERIT;
    END IF;

    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'authenticated') THEN
        CREATE ROLE authenticated NOLOGIN NOINHERIT;
    END IF;

    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'service_role') THEN
        CREATE ROLE service_role NOLOGIN NOINHERIT BYPASSRLS;
    END IF;

    -- 2. Create authenticator role (used by PostgREST/APIs to switch to anon/authenticated)
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'authenticator') THEN
        CREATE ROLE authenticator NOLOGIN NOINHERIT;
    END IF;

    -- Service roles and deployment-specific passwords are provisioned by
    -- 000000_configure_supabase_roles.sh before this migration.
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'supabase_auth_admin')
       OR NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'supabase_storage_admin')
       OR NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'supabase_admin') THEN
        RAISE EXCEPTION 'Supabase service roles have not been provisioned';
    END IF;
END
$$;

-- Grant memberships
GRANT anon, authenticated, service_role TO authenticator;
GRANT anon, authenticated, service_role TO supabase_storage_admin;
GRANT anon, authenticated, service_role TO supabase_auth_admin;
SELECT format('GRANT supabase_auth_admin TO %I', current_user) \gexec
SELECT format('GRANT supabase_storage_admin TO %I', current_user) \gexec
SELECT format('GRANT supabase_admin TO %I', current_user) \gexec

-- Schema for Supabase Auth
CREATE SCHEMA IF NOT EXISTS auth AUTHORIZATION supabase_auth_admin;
GRANT ALL ON SCHEMA auth TO supabase_auth_admin;

-- Permissions on database & schemas
SELECT format('GRANT CREATE ON DATABASE %I TO supabase_auth_admin', current_database()) \gexec
SELECT format('GRANT CREATE ON DATABASE %I TO supabase_storage_admin', current_database()) \gexec
GRANT ALL ON SCHEMA public TO supabase_auth_admin;
GRANT ALL ON SCHEMA public TO supabase_storage_admin;
ALTER USER supabase_auth_admin CREATEROLE;

-- Set search_path for admin roles
ALTER USER supabase_auth_admin SET search_path = auth, public;
ALTER USER supabase_storage_admin SET search_path = storage, public;
ALTER USER authenticator SET search_path = public, auth;

GRANT USAGE ON SCHEMA public TO anon, authenticated, service_role;
GRANT ALL ON ALL TABLES IN SCHEMA public TO service_role;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO service_role;
GRANT ALL ON ALL ROUTINES IN SCHEMA public TO service_role;

-- Schema for Supabase Storage
CREATE SCHEMA IF NOT EXISTS storage AUTHORIZATION supabase_storage_admin;
GRANT ALL ON SCHEMA storage TO supabase_storage_admin;
GRANT USAGE ON SCHEMA storage TO anon, authenticated, service_role;
GRANT ALL ON ALL TABLES IN SCHEMA storage TO supabase_storage_admin, service_role;
GRANT ALL ON ALL SEQUENCES IN SCHEMA storage TO supabase_storage_admin, service_role;
GRANT ALL ON ALL ROUTINES IN SCHEMA storage TO supabase_storage_admin, service_role;

ALTER DEFAULT PRIVILEGES IN SCHEMA storage GRANT ALL ON TABLES TO supabase_storage_admin, service_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA storage GRANT ALL ON SEQUENCES TO supabase_storage_admin, service_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA storage GRANT ALL ON ROUTINES TO supabase_storage_admin, service_role;
