\set ON_ERROR_STOP on
-- Read-only advisory rehearsal. The migration repeats these checks under locks.
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM public.twin_commands c JOIN public.twin_entities e ON e.id=c.entity_id
               WHERE e.entity_type='Sensor' AND c.status IN('pending','published','acknowledged')
               GROUP BY c.entity_id HAVING count(*)>1) THEN
        RAISE EXCEPTION 'Stage 5 preflight: multiple active commands for a Sensor; operator resolution required, no rows changed';
    END IF;
    IF EXISTS (SELECT 1 FROM public.twin_commands WHERE desired_version<0 OR expires_at<=issued_at)
       OR EXISTS (SELECT 1 FROM public.twin_states WHERE desired_version<0 OR reported_version<0) THEN
        RAISE EXCEPTION 'Stage 5 preflight: negative version or invalid command deadline; operator resolution required, no rows changed';
    END IF;
END $$;
SELECT c.action,c.status,e.entity_type,count(*) AS legacy_rows
FROM public.twin_commands c JOIN public.twin_entities e ON e.id=c.entity_id
GROUP BY c.action,c.status,e.entity_type ORDER BY c.action,c.status,e.entity_type;
\echo 'Migration 000023 preflight passed. Existing actions/payloads remain legacy, not dispatchable.'
