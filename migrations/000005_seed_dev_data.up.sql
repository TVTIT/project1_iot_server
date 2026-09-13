-- Seed Development Data for Local Testing
-- Includes sample Gateway, Sensors, User-Gateway mapping, and initial FL Model

DO $$
DECLARE
    v_user_id UUID;
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

    -- 4. Insert Default Federated Learning Model (Dense Autoencoder, input_window: 100)
    INSERT INTO public.fl_models (
        model_id,
        name,
        architecture_json,
        input_window,
        param_count
    ) VALUES (
        'a0000000-0000-0000-0000-000000000001',
        'Dense-Autoencoder-100Hz',
        jsonb_build_object(
            'input_dim', 100,
            'hidden_layers', jsonb_build_array(64, 32, 64),
            'output_dim', 200,
            'activation', 'relu',
            'learning_rate', 0.001
        ),
        100,
        15200
    )
    ON CONFLICT (model_id) DO UPDATE
    SET name = EXCLUDED.name, 
        architecture_json = EXCLUDED.architecture_json,
        input_window = EXCLUDED.input_window,
        param_count = EXCLUDED.param_count;

END $$;
