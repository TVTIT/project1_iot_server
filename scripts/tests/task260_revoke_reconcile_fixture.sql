-- FIXTURE ONLY, not an application migration. No credential/hash columns.
CREATE TABLE authority (
  gateway text PRIMARY KEY CHECK (gateway IN ('A', 'B')),
  revoked boolean NOT NULL DEFAULT false,
  epoch bigint NOT NULL DEFAULT 0 CHECK (epoch >= 0)
);
CREATE TABLE jobs (
  operation text PRIMARY KEY CHECK (operation ~ '^[a-f0-9]{32}$'),
  gateway text NOT NULL UNIQUE REFERENCES authority(gateway),
  status text NOT NULL CHECK (status IN ('pending', 'snapshot_observed')),
  attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0)
);
INSERT INTO authority(gateway) VALUES ('A'), ('B');
