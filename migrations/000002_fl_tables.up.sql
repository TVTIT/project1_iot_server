-- 7. Federated Learning (FL) Tables

CREATE TABLE IF NOT EXISTS fl_models (
    model_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    architecture_json JSONB NOT NULL,
    input_window INT NOT NULL,
    param_count BIGINT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS fl_global_models (
    model_id UUID REFERENCES fl_models(model_id) ON DELETE CASCADE,
    round_number INT NOT NULL,
    storage_path TEXT NOT NULL,
    checksum TEXT,
    metrics_json JSONB,
    created_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (model_id, round_number)
);

CREATE TABLE IF NOT EXISTS fl_rounds (
    round_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    model_id UUID REFERENCES fl_models(model_id) ON DELETE CASCADE,
    round_number INT NOT NULL,
    state TEXT NOT NULL DEFAULT 'created', -- created, open, collecting, aggregating, completed, aborted
    target_client_count INT NOT NULL DEFAULT 2,
    min_updates_required INT NOT NULL DEFAULT 2,
    deadline_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS fl_round_participants (
    round_id UUID REFERENCES fl_rounds(round_id) ON DELETE CASCADE,
    gateway_id TEXT REFERENCES gateways(gateway_id) ON DELETE CASCADE,
    state TEXT NOT NULL DEFAULT 'invited', -- invited, downloaded, submitted, failed
    invited_at TIMESTAMPTZ DEFAULT now(),
    downloaded_at TIMESTAMPTZ,
    submitted_at TIMESTAMPTZ,
    PRIMARY KEY (round_id, gateway_id)
);

CREATE TABLE IF NOT EXISTS fl_client_updates (
    update_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    round_id UUID REFERENCES fl_rounds(round_id) ON DELETE CASCADE,
    gateway_id TEXT REFERENCES gateways(gateway_id) ON DELETE CASCADE,
    message_id UUID NOT NULL,
    storage_path TEXT NOT NULL,
    num_samples INT NOT NULL,
    checksum TEXT,
    size_bytes BIGINT,
    train_loss DOUBLE PRECISION,
    validation_status TEXT DEFAULT 'pending',
    received_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE (round_id, gateway_id),
    UNIQUE (gateway_id, message_id)
);
