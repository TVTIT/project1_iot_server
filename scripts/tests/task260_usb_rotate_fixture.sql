-- Disposable fixture only, NOT application schema. No password or hash fields.
CREATE TABLE rotate_authority (
 gateway text PRIMARY KEY CHECK (gateway IN ('A','B')),
 epoch bigint NOT NULL DEFAULT 0,
 admission text NOT NULL CHECK (admission IN ('active','maintenance','recovery_needed'))
);
CREATE TABLE rotate_jobs (
 operation text PRIMARY KEY CHECK (operation ~ '^[a-f0-9]{32}$'),
 gateway text NOT NULL REFERENCES rotate_authority,
 epoch bigint NOT NULL,
 status text NOT NULL CHECK (status IN ('pending','verified','active','recovery_needed')),
 delivery text NOT NULL DEFAULT 'unknown' CHECK (delivery IN ('unknown','device_updated'))
);
INSERT INTO rotate_authority VALUES ('A',0,'active'),('B',0,'active');
