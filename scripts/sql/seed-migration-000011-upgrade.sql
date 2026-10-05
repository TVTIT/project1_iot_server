\set ON_ERROR_STOP on
-- Isolated v10 fixture: duplicate legacy pending rows are legal in v10.
INSERT INTO profiles (id, full_name) VALUES ('12000000-0000-4000-8000-000000000001', 'Legacy actor');
INSERT INTO gateways (gateway_id, name) VALUES ('legacy11_active', 'Legacy active'), ('legacy11_revoked', 'Legacy revoked'), ('legacy11_pending', 'Legacy pending');
INSERT INTO gateway_mqtt_credential_events (operation_id, gateway_id, credential_version, action, status, actor_user_id, completed_at) VALUES
('12000000-0000-4000-8000-000000000002', 'legacy11_active', 2, 'rotate', 'succeeded', '12000000-0000-4000-8000-000000000001', now()),
('12000000-0000-4000-8000-000000000003', 'legacy11_revoked', 3, 'revoke', 'succeeded', '12000000-0000-4000-8000-000000000001', now()),
('12000000-0000-4000-8000-000000000004', 'legacy11_pending', NULL, 'recover', 'pending', NULL, NULL),
('12000000-0000-4000-8000-000000000005', 'legacy11_pending', 7, 'provision', 'pending', '12000000-0000-4000-8000-000000000001', NULL);
INSERT INTO gateway_mqtt_credentials (gateway_id, credential_version, status, last_operation_id, changed_by, activated_at, revoked_at) VALUES
('legacy11_active', 2, 'active', '12000000-0000-4000-8000-000000000002', '12000000-0000-4000-8000-000000000001', now(), NULL),
('legacy11_revoked', 3, 'revoked', '12000000-0000-4000-8000-000000000003', '12000000-0000-4000-8000-000000000001', now(), now());
CREATE TABLE migration11_golden AS
SELECT 'event' AS kind, to_jsonb(e) AS data FROM gateway_mqtt_credential_events e WHERE gateway_id LIKE 'legacy11_%'
UNION ALL SELECT 'credential', to_jsonb(c) FROM gateway_mqtt_credentials c WHERE gateway_id LIKE 'legacy11_%'
UNION ALL SELECT 'profile', to_jsonb(p) FROM profiles p WHERE id = '12000000-0000-4000-8000-000000000001'
UNION ALL SELECT 'gateway', to_jsonb(g) FROM gateways g WHERE gateway_id LIKE 'legacy11_%';
