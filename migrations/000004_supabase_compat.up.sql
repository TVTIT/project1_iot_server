-- Supabase Compatibility & Row Level Security (RLS)
-- NOTE: This migration is applied AFTER Supabase Auth (GoTrue) initializes the auth schema.

-- 1. Add Foreign Key reference from profiles.id to auth.users.id
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints 
        WHERE constraint_name = 'profiles_id_fkey' AND table_name = 'profiles'
    ) THEN
        ALTER TABLE profiles
            ADD CONSTRAINT profiles_id_fkey
            FOREIGN KEY (id) REFERENCES auth.users(id) ON DELETE CASCADE;
    END IF;
END $$;

-- 2. Helper function to create profile automatically on user signup
CREATE OR REPLACE FUNCTION public.handle_new_user()
RETURNS trigger AS $$
BEGIN
  INSERT INTO public.profiles (id, full_name)
  VALUES (new.id, new.raw_user_meta_data->>'full_name')
  ON CONFLICT (id) DO NOTHING;
  RETURN new;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Trigger to create profile when auth.users row is inserted
DROP TRIGGER IF EXISTS on_auth_user_created ON auth.users;
CREATE TRIGGER on_auth_user_created
  AFTER INSERT ON auth.users
  FOR EACH ROW EXECUTE PROCEDURE public.handle_new_user();

-- 3. Enable Row Level Security on business relational tables
ALTER TABLE profiles ENABLE ROW LEVEL SECURITY;
ALTER TABLE user_gateways ENABLE ROW LEVEL SECURITY;
ALTER TABLE gateways ENABLE ROW LEVEL SECURITY;
ALTER TABLE sensors ENABLE ROW LEVEL SECURITY;
ALTER TABLE media_objects ENABLE ROW LEVEL SECURITY;

-- 4. RLS Policies: Profiles
DROP POLICY IF EXISTS "profiles_select_policy" ON profiles;
CREATE POLICY "profiles_select_policy" ON profiles
    FOR SELECT TO authenticated
    USING (auth.uid() = id);

DROP POLICY IF EXISTS "profiles_update_policy" ON profiles;
CREATE POLICY "profiles_update_policy" ON profiles
    FOR UPDATE TO authenticated
    USING (auth.uid() = id);

-- 5. RLS Policies: User Gateways
DROP POLICY IF EXISTS "user_gateways_select_policy" ON user_gateways;
CREATE POLICY "user_gateways_select_policy" ON user_gateways
    FOR SELECT TO authenticated
    USING (auth.uid() = user_id);

-- 6. RLS Policies: Gateways
DROP POLICY IF EXISTS "gateways_select_policy" ON gateways;
CREATE POLICY "gateways_select_policy" ON gateways
    FOR SELECT TO authenticated
    USING (
        EXISTS (
            SELECT 1 FROM user_gateways ug
            WHERE ug.gateway_id = gateways.gateway_id
              AND ug.user_id = auth.uid()
        )
    );

-- 7. RLS Policies: Sensors
DROP POLICY IF EXISTS "sensors_select_policy" ON sensors;
CREATE POLICY "sensors_select_policy" ON sensors
    FOR SELECT TO authenticated
    USING (
        EXISTS (
            SELECT 1 FROM user_gateways ug
            WHERE ug.gateway_id = sensors.gateway_id
              AND ug.user_id = auth.uid()
        )
    );

-- 8. RLS Policies: Media Objects
DROP POLICY IF EXISTS "media_objects_select_policy" ON media_objects;
CREATE POLICY "media_objects_select_policy" ON media_objects
    FOR SELECT TO authenticated
    USING (
        EXISTS (
            SELECT 1 FROM user_gateways ug
            WHERE ug.gateway_id = media_objects.gateway_id
              AND ug.user_id = auth.uid()
        )
    );
