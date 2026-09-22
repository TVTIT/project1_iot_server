-- Backfill every existing Digital Twin entity with its owning Gateway.
\set ON_ERROR_STOP on

BEGIN;

-- Gateway entities use the stable NGSI-LD identifier as the authoritative mapping.
UPDATE twin_entities AS entity
SET gateway_id = gateway.gateway_id
FROM gateways AS gateway
WHERE entity.entity_type = 'Gateway'
  AND entity.entity_id = 'urn:ngsi-ld:Gateway:' || gateway.gateway_id
  AND entity.gateway_id IS NULL;

-- A Sensor inherits ownership only when it has exactly one hasSensor
-- relationship from a Gateway. Other entity types must be assigned explicitly
-- before this migration can complete.
WITH resolved_owners AS (
    SELECT
        child.id AS entity_uuid,
        min(parent.gateway_id) AS gateway_id
    FROM twin_entities AS child
    JOIN twin_relationships AS relationship
      ON relationship.target_entity_id = child.id
    JOIN twin_entities AS parent
      ON parent.id = relationship.source_entity_id
    WHERE child.entity_type = 'Sensor'
      AND parent.entity_type = 'Gateway'
      AND relationship.relationship_type = 'hasSensor'
      AND parent.gateway_id IS NOT NULL
    GROUP BY child.id
    HAVING count(DISTINCT parent.id) = 1
)
UPDATE twin_entities AS entity
SET gateway_id = owner.gateway_id
FROM resolved_owners AS owner
WHERE entity.id = owner.entity_uuid
  AND entity.gateway_id IS NULL;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM twin_entities AS child
        JOIN twin_relationships AS relationship
          ON relationship.target_entity_id = child.id
        JOIN twin_entities AS parent
          ON parent.id = relationship.source_entity_id
        WHERE child.entity_type = 'Sensor'
          AND parent.entity_type = 'Gateway'
          AND relationship.relationship_type = 'hasSensor'
          AND parent.gateway_id IS NOT NULL
        GROUP BY child.id
        HAVING count(DISTINCT parent.id) > 1
    ) THEN
        RAISE EXCEPTION
            'Digital Twin ownership backfill is ambiguous: an entity belongs to multiple Gateways';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM twin_entities
        WHERE gateway_id IS NULL
    ) THEN
        RAISE EXCEPTION
            'Digital Twin ownership backfill is incomplete: some entities have no Gateway';
    END IF;
END
$$;

COMMIT;
