-- Seed Development Data for Local Testing
-- Includes sample Gateway, Sensors, User-Gateway mapping, and initial Digital Twin entities

DO $$
DECLARE
    v_user_id UUID;
    v_gw_entity_uuid UUID := 'e0000000-0000-0000-0000-000000000001'::uuid;
    v_sensor_entity_uuid UUID := 'e0000000-0000-0000-0000-000000000002'::uuid;
BEGIN
    -- 1. Insert Sample Gateway
    INSERT INTO public.gateways (gateway_id, name, description)
    VALUES (
        'gateway_001',
        'Gateway TI AM5728 Lab',
        'Primary development and evaluation gateway board'
    )
    ON CONFLICT (gateway_id) DO UPDATE 
    SET name = EXCLUDED.name, description = EXCLUDED.description;

    -- 2. Insert Sample Sensors for gateway_001
    INSERT INTO public.sensors (sensor_id, gateway_id, name, unit)
    VALUES 
        ('sensor_001', 'gateway_001', 'Vibration / Accelerometer 100Hz', 'm/s2'),
        ('sensor_002', 'gateway_001', 'Temperature Sensor', 'degC')
    ON CONFLICT (gateway_id, sensor_id) DO UPDATE
    SET name = EXCLUDED.name, unit = EXCLUDED.unit;

    -- 3. Link existing dev user to gateway_001 as owner
    SELECT id INTO v_user_id 
    FROM auth.users 
    WHERE email = 'student_test@example.com' 
    LIMIT 1;

    IF v_user_id IS NOT NULL THEN
        INSERT INTO public.user_gateways (user_id, gateway_id, role)
        VALUES (v_user_id, 'gateway_001', 'owner')
        ON CONFLICT (user_id, gateway_id) DO UPDATE 
        SET role = 'owner';
    END IF;

    -- 4. Insert Digital Twin Entities (Section 10.2 of AGENTS.md)
    -- Gateway Entity
    INSERT INTO public.twin_entities (
        id,
        entity_id,
        entity_type,
        name,
        attributes
    ) VALUES (
        v_gw_entity_uuid,
        'urn:ngsi-ld:Gateway:gateway_001',
        'Gateway',
        'Gateway TI AM5728 Lab',
        jsonb_build_object(
            'connectionStatus', 'online',
            'model', 'AM5728',
            'ip', '192.168.1.100'
        )
    )
    ON CONFLICT (id) DO UPDATE
    SET entity_id = EXCLUDED.entity_id,
        entity_type = EXCLUDED.entity_type,
        name = EXCLUDED.name,
        attributes = EXCLUDED.attributes;

    -- Sensor Entity
    INSERT INTO public.twin_entities (
        id,
        entity_id,
        entity_type,
        name,
        attributes
    ) VALUES (
        v_sensor_entity_uuid,
        'urn:ngsi-ld:Sensor:sensor_001',
        'Sensor',
        'Vibration / Accelerometer 100Hz',
        jsonb_build_object(
            'unit', 'm/s2',
            'sampling_frequency_hz', 100
        )
    )
    ON CONFLICT (id) DO UPDATE
    SET entity_id = EXCLUDED.entity_id,
        entity_type = EXCLUDED.entity_type,
        name = EXCLUDED.name,
        attributes = EXCLUDED.attributes;

    -- 5. Insert Relationship: Gateway hasSensor Sensor
    INSERT INTO public.twin_relationships (
        source_entity_id,
        relationship_type,
        target_entity_id
    ) VALUES (
        v_gw_entity_uuid,
        'hasSensor',
        v_sensor_entity_uuid
    )
    ON CONFLICT (source_entity_id, relationship_type, target_entity_id) DO NOTHING;

    -- 6. Insert Initial Twin State for Gateway
    INSERT INTO public.twin_states (
        entity_id,
        reported_state,
        desired_state,
        reported_version,
        desired_version,
        last_reported_at,
        last_desired_at
    ) VALUES (
        v_gw_entity_uuid,
        jsonb_build_object('sampling_interval_seconds', 10, 'status', 'online'),
        jsonb_build_object('sampling_interval_seconds', 10, 'status', 'online'),
        1,
        1,
        now(),
        now()
    )
    ON CONFLICT (entity_id) DO UPDATE
    SET reported_state = EXCLUDED.reported_state,
        desired_state = EXCLUDED.desired_state,
        reported_version = EXCLUDED.reported_version,
        desired_version = EXCLUDED.desired_version,
        last_reported_at = EXCLUDED.last_reported_at,
        last_desired_at = EXCLUDED.last_desired_at;

END $$;
