WITH seed_commands AS MATERIALIZED (
    SELECT candidate.command_id,candidate.root_command_id
    FROM public.twin_commands candidate
    JOIN public.twin_states state ON state.entity_id=candidate.entity_id
    JOIN public.twin_entities entity ON entity.id=candidate.entity_id
    WHERE candidate.stage6_managed IS TRUE
      AND candidate.control_version=1
      AND candidate.action='update_configuration'
      AND entity.entity_type='Sensor'
      AND (
          (candidate.origin='client'
           AND candidate.parent_command_id IS NULL
           AND candidate.command_id=candidate.root_command_id
           AND candidate.desired_version < state.desired_version
           AND candidate.issued_at <
               ((statement_timestamp() AT TIME ZONE 'UTC' - interval '720 hours') AT TIME ZONE 'UTC'))
          OR candidate.desired_version=state.desired_version
      )
),
retention_scope AS MATERIALIZED (
    SELECT command_id FROM seed_commands
    UNION
    SELECT related.command_id
    FROM public.twin_commands related
    JOIN seed_commands seed ON related.root_command_id=seed.command_id
                            OR related.parent_command_id=seed.command_id
                            OR related.command_id=seed.root_command_id
),
request_counts AS MATERIALIZED (
    SELECT command_id, count(*) AS receipt_count
    FROM public.twin_command_requests
    WHERE command_id IN (SELECT command_id FROM retention_scope)
    GROUP BY command_id
),
command_facts AS MATERIALIZED (
    SELECT c.command_id,c.entity_id,c.action,c.parameters,c.status,c.desired_version,
           c.idempotency_key,c.issued_by,c.issued_at,c.expires_at,c.acknowledged_at,
           c.completed_at,c.attempt_count,c.error_code,c.target_entity_type,
           c.control_version,c.requested_frequency_hz,c.result_received_at,
           c.late_result_received_at,c.stage6_managed,c.first_reserved_at,
           c.retry_stop_reason,c.receipt_received_at,c.cancelled_at,c.cancelled_by,
           c.origin,c.root_command_id,c.parent_command_id,c.original_actor_id,
           c.reconcile_blocked,c.auto_successors_used,
           e.entity_type AS actual_entity_type,
           e.gateway_id AS actual_gateway_id,
           e.entity_id AS actual_urn,
           COALESCE(request_counts.receipt_count,0) AS command_receipt_count,
           CASE
             WHEN c.parameters->>'sample_interval_us' ~ '^[0-9]{1,16}$'
             THEN CASE
                    WHEN (c.parameters->>'sample_interval_us')::numeric BETWEEN 10000 AND 9007199254740992
                    THEN (c.parameters->>'sample_interval_us')::numeric
                    ELSE NULL
                  END
             ELSE NULL
           END AS parameter_interval,
           frequency.requested_hz,
           frequency.requested_interval,
           frequency.rational_hz,
           CASE
             WHEN c.receipt_bytes IS NULL AND c.receipt_hash IS NULL
                  AND c.receipt_received_at IS NULL
             THEN c.retry_stop_reason IS DISTINCT FROM 'acknowledged'
             ELSE (SELECT count(*) = 1 AND bool_and(
                  c.receipt_hash = encode(sha256(c.receipt_bytes), 'hex')
                  AND receipt_doc.body IS NOT NULL
                  AND (SELECT count(*) FROM jsonb_object_keys(receipt_doc.body)) = 8
                  AND receipt_doc.body->>'protocol_version' = '1'
                 AND receipt_doc.body->>'message_type' = 'command_receipt'
                 AND receipt_doc.body->>'command_id' = c.command_id::text
                 AND receipt_doc.body->>'gateway_id' = e.gateway_id
                 AND receipt_doc.body->>'entity_id' = e.entity_id
                 AND receipt_doc.body->>'desired_version' = c.desired_version::text
                 AND receipt_doc.body->>'command_hash' = encode(sha256(o.payload_bytes), 'hex')
                 AND pg_input_is_valid(receipt_doc.body->>'received_at', 'timestamp with time zone')
                 AND (receipt_doc.body->>'received_at')::timestamptz >= c.issued_at
                 AND c.receipt_received_at >= c.issued_at
                 AND c.receipt_received_at < c.expires_at
             )
             FROM public.twin_outbox o
             CROSS JOIN LATERAL (
                 SELECT pg_temp.stage6_retention_json(c.receipt_bytes) AS body
             ) receipt_doc
             WHERE o.command_id = c.command_id
               AND c.receipt_bytes IS NOT NULL
                AND c.receipt_hash IS NOT NULL
                AND c.receipt_received_at IS NOT NULL
             )
           END AS receipt_valid,
           (
             SELECT count(*) = 1 AND bool_and(
                 receipt.status_code = 202
                 AND receipt.actor_id = c.original_actor_id
                 AND receipt.entity_id = c.entity_id
                 AND receipt.idempotency_key = c.idempotency_key
                 AND receipt.fingerprint ~ '^[0-9a-f]{64}$'
                  AND receipt.fingerprint = encode(sha256(convert_to(
                      '["update_configuration","' || frequency.rational_hz || '","' ||
                      (c.desired_version - 1)::text || '"]', 'UTF8')), 'hex')
                 AND doc.body IS NOT NULL
                  AND (SELECT count(*) FROM jsonb_object_keys(doc.body)) = 9
                 AND doc.body->>'entity_id' = e.entity_id
                 AND doc.body->>'command_id' = c.command_id::text
                 AND doc.body->>'status' = 'pending'
                 AND doc.body->>'desired_version' = c.desired_version::text
                 AND jsonb_typeof(doc.body->'sample_interval_us') = 'number'
                 AND doc.body->>'sample_interval_us' = c.parameters->>'sample_interval_us'
                 AND doc.body->>'status_url' = '/v1/digital-twins/' || e.entity_id ||
                                               '/commands/' || c.command_id::text
                 AND jsonb_typeof(doc.body->'requested_frequency_hz') = 'number'
                 AND CASE
                       WHEN length(doc.body->>'requested_frequency_hz') <= 128
                        AND (doc.body->>'requested_frequency_hz')
                              ~ '^-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?$'
                        AND pg_input_is_valid(doc.body->>'requested_frequency_hz', 'numeric')
                       THEN (doc.body->>'requested_frequency_hz')::numeric = frequency.requested_hz
                       ELSE false
                     END
                 AND jsonb_typeof(doc.body->'effective_frequency_hz') = 'number'
                 AND CASE
                       WHEN pg_input_is_valid(doc.body->>'effective_frequency_hz', 'double precision')
                       THEN (doc.body->>'effective_frequency_hz')::double precision
                            = 1000000::double precision / NULLIF(frequency.requested_interval, 0)::double precision
                       ELSE false
                     END
                 AND CASE
                       WHEN pg_input_is_valid(doc.body->>'expires_at', 'timestamp with time zone')
                       THEN (doc.body->>'expires_at')::timestamptz = c.expires_at
                       ELSE false
                     END
             )
             FROM public.twin_command_requests receipt
             CROSS JOIN LATERAL (
                 SELECT pg_temp.stage6_retention_json(receipt.response_bytes) AS body
             ) doc
             WHERE receipt.actor_id = c.original_actor_id
               AND receipt.entity_id = c.entity_id
               AND receipt.idempotency_key = c.idempotency_key
               AND receipt.command_id = c.command_id
           ) AND COALESCE(request_counts.receipt_count,0) = 1 AS admission_valid,
           c.status IN ('succeeded', 'failed', 'timeout', 'cancelled')
             AND c.completed_at IS NOT NULL
             AND c.expires_at > c.issued_at
             AND c.completed_at >= c.issued_at
             AND (c.first_reserved_at IS NULL OR c.first_reserved_at >= c.issued_at)
             AND (c.result_received_at IS NULL OR c.result_received_at >= c.issued_at)
             AND (c.late_result_received_at IS NULL OR c.late_result_received_at >= c.issued_at)
             AND (
                 (c.status IN ('succeeded', 'failed')
                  AND c.first_reserved_at IS NOT NULL AND c.attempt_count > 0
                  AND c.result_received_at IS NOT NULL)
                 OR
                   (c.status = 'timeout' AND c.error_code = 'COMMAND_EXPIRED'
                    AND c.completed_at >= c.expires_at
                     AND (c.retry_stop_reason IS NULL
                         OR (c.retry_stop_reason = 'acknowledged' AND c.acknowledged_at IS NOT NULL)
                         OR (c.retry_stop_reason = 'retry_exhausted' AND c.attempt_count > 0)))
                 OR
                 (c.status = 'cancelled' AND c.cancelled_at = c.completed_at
                  AND c.cancelled_by IS NOT NULL AND c.first_reserved_at IS NULL
                  AND c.attempt_count = 0 AND c.retry_stop_reason = 'cancelled'
                  AND c.reconcile_blocked)
             )
             AND (
                 (c.late_result_received_at IS NULL
                  AND c.late_result_bytes IS NULL AND c.late_result_hash IS NULL)
                 OR
                 (c.status = 'timeout' AND c.late_result_received_at IS NOT NULL
                  AND c.late_result_bytes IS NOT NULL
                  AND c.late_result_hash = encode(sha256(c.late_result_bytes), 'hex')
                  AND parsed.late_result_document IS NOT NULL
                  AND parsed.late_result_document->>'command_id' = c.command_id::text
                  AND parsed.late_result_document->>'gateway_id' = e.gateway_id
                  AND parsed.late_result_document->>'entity_id' = e.entity_id
                  AND parsed.late_result_document->>'desired_version' = c.desired_version::text
                  AND parsed.late_result_document->>'protocol_version' = '1'
                  AND parsed.late_result_document->>'message_type' = 'command_result'
                  AND parsed.late_result_document->>'status' IN ('succeeded', 'failed')
                  AND CASE
                         WHEN pg_input_is_valid(parsed.late_result_document->>'completed_at', 'timestamp with time zone')
                         THEN (parsed.late_result_document->>'completed_at')::timestamptz >= c.issued_at
                          AND (parsed.late_result_document->>'status' <> 'succeeded'
                               OR (parsed.late_result_document->>'completed_at')::timestamptz <= c.expires_at)
                         ELSE false
                       END
                   AND CASE parsed.late_result_document->>'status'
                         WHEN 'succeeded' THEN
                           parsed.late_result_document->'reported_state' = c.parameters
                           AND wire.late_interval = c.parameters->>'sample_interval_us'
                           AND NOT (parsed.late_result_document ? 'error_code')
                           AND NOT (parsed.late_result_document ? 'error_message')
                          WHEN 'failed' THEN jsonb_typeof(parsed.late_result_document->'error_code') = 'string'
                            AND parsed.late_result_document->>'error_code'
                                IN ('COMMAND_EXPIRED','STALE_DESIRED_VERSION','UNKNOWN_SENSOR',
                                    'UNSUPPORTED_CONFIGURATION','APPLY_FAILED')
                            AND NOT (parsed.late_result_document ? 'reported_state')
                         ELSE false
                       END
                   AND c.late_result_received_at >= c.issued_at
                  )
             )
             AND CASE c.status
                   WHEN 'succeeded' THEN c.result_bytes IS NOT NULL
                     AND c.result_hash = encode(sha256(c.result_bytes), 'hex')
                     AND c.result_received_at IS NOT NULL
                     AND parsed.result_document IS NOT NULL
                   AND parsed.result_document->>'command_id' = c.command_id::text
                   AND parsed.result_document->>'gateway_id' = e.gateway_id
                   AND parsed.result_document->>'entity_id' = e.entity_id
                   AND parsed.result_document->>'protocol_version' = '1'
                   AND parsed.result_document->>'message_type' = 'command_result'
                   AND parsed.result_document->>'desired_version' = c.desired_version::text
                    AND parsed.result_document->>'status' = 'succeeded'
                    AND NOT (parsed.result_document ? 'error_code')
                    AND NOT (parsed.result_document ? 'error_message')
                    AND CASE
                          WHEN pg_input_is_valid(parsed.result_document->>'completed_at', 'timestamp with time zone')
                          THEN (parsed.result_document->>'completed_at')::timestamptz <= c.expires_at
                          ELSE false
                        END
                    AND parsed.result_document->'reported_state' = c.parameters
                    AND wire.result_interval = c.parameters->>'sample_interval_us'
                     AND CASE
                           WHEN pg_input_is_valid(parsed.result_document->>'completed_at', 'timestamp with time zone')
                           THEN (parsed.result_document->>'completed_at')::timestamptz = c.completed_at
                           ELSE false
                         END
                   WHEN 'failed' THEN c.result_bytes IS NOT NULL
                     AND c.result_hash = encode(sha256(c.result_bytes), 'hex')
                     AND c.result_received_at IS NOT NULL
                     AND parsed.result_document IS NOT NULL
                   AND parsed.result_document->>'command_id' = c.command_id::text
                   AND parsed.result_document->>'gateway_id' = e.gateway_id
                   AND parsed.result_document->>'entity_id' = e.entity_id
                   AND parsed.result_document->>'protocol_version' = '1'
                   AND parsed.result_document->>'message_type' = 'command_result'
                   AND parsed.result_document->>'desired_version' = c.desired_version::text
                    AND parsed.result_document->>'status' = 'failed'
                     AND parsed.result_document->>'error_code' = c.error_code
                     AND jsonb_typeof(parsed.result_document->'error_code') = 'string'
                     AND c.error_code IN ('COMMAND_EXPIRED','STALE_DESIRED_VERSION','UNKNOWN_SENSOR',
                                          'UNSUPPORTED_CONFIGURATION','APPLY_FAILED')
                    AND NOT (parsed.result_document ? 'reported_state')
                     AND CASE
                           WHEN pg_input_is_valid(parsed.result_document->>'completed_at', 'timestamp with time zone')
                           THEN (parsed.result_document->>'completed_at')::timestamptz = c.completed_at
                           ELSE false
                         END
                   WHEN 'timeout' THEN true
                   WHEN 'cancelled' THEN c.cancelled_at IS NOT NULL
                     AND c.cancelled_by IS NOT NULL
                   ELSE false
                 END AS terminal_evidence_valid,
           (SELECT count(*) = 1 AND bool_and(
                 o.payload_bytes IS NOT NULL
                 AND outbox_doc.body IS NOT NULL
                 AND outbox_doc.body = o.payload
                AND o.topic = 'gateways/' || e.gateway_id || '/commands/' || c.command_id::text
                 AND outbox_doc.body->>'protocol_version' = '1'
                 AND outbox_doc.body->>'message_type' = 'command'
                 AND outbox_doc.body->>'command_id' = c.command_id::text
                 AND outbox_doc.body->>'gateway_id' = e.gateway_id
                 AND outbox_doc.body->>'entity_id' = e.entity_id
                 AND outbox_doc.body->>'action' = c.action
                 AND outbox_doc.body->'parameters' = c.parameters
                 AND outbox_doc.body->>'desired_version' = c.desired_version::text
                 AND CASE
                       WHEN pg_input_is_valid(outbox_doc.body->>'issued_at', 'timestamp with time zone')
                       THEN (outbox_doc.body->>'issued_at')::timestamptz = c.issued_at
                       ELSE false
                     END
                 AND CASE
                       WHEN pg_input_is_valid(outbox_doc.body->>'expires_at', 'timestamp with time zone')
                       THEN (outbox_doc.body->>'expires_at')::timestamptz = c.expires_at
                       ELSE false
                     END
                  AND o.attempt_count = c.attempt_count
                  AND CASE
                        WHEN c.status IN ('timeout','cancelled','failed')
                        THEN o.status = 'failed'
                        WHEN c.status = 'succeeded'
                        THEN o.status = 'published'
                          OR (o.status = 'failed' AND c.receipt_bytes IS NOT NULL
                              AND c.receipt_hash = encode(sha256(c.receipt_bytes), 'hex')
                              AND c.receipt_received_at >= c.issued_at
                              AND c.receipt_received_at < c.expires_at
                              AND pg_temp.stage6_retention_json(c.receipt_bytes)->>'protocol_version' = '1'
                              AND pg_temp.stage6_retention_json(c.receipt_bytes)->>'message_type' = 'command_receipt'
                              AND pg_temp.stage6_retention_json(c.receipt_bytes)->>'command_id' = c.command_id::text
                              AND pg_temp.stage6_retention_json(c.receipt_bytes)->>'gateway_id' = e.gateway_id
                              AND pg_temp.stage6_retention_json(c.receipt_bytes)->>'entity_id' = e.entity_id
                              AND pg_temp.stage6_retention_json(c.receipt_bytes)->>'desired_version' = c.desired_version::text
                              AND pg_temp.stage6_retention_json(c.receipt_bytes)->>'command_hash'
                                  = encode(sha256(o.payload_bytes), 'hex')
                              AND pg_input_is_valid(pg_temp.stage6_retention_json(c.receipt_bytes)->>'received_at',
                                                    'timestamp with time zone')
                              AND (pg_temp.stage6_retention_json(c.receipt_bytes)->>'received_at')::timestamptz
                                  >= c.issued_at
                              AND (SELECT count(*) FROM jsonb_object_keys(
                                  pg_temp.stage6_retention_json(c.receipt_bytes))) = 8)
                        WHEN c.status IN ('pending','published')
                        THEN o.status = c.status
                        WHEN c.status = 'acknowledged'
                        THEN o.status = 'failed' AND c.receipt_bytes IS NOT NULL
                          AND c.receipt_hash = encode(sha256(c.receipt_bytes), 'hex')
                          AND c.receipt_received_at >= c.issued_at
                          AND c.receipt_received_at < c.expires_at
                          AND pg_temp.stage6_retention_json(c.receipt_bytes)->>'protocol_version' = '1'
                          AND pg_temp.stage6_retention_json(c.receipt_bytes)->>'message_type' = 'command_receipt'
                          AND pg_temp.stage6_retention_json(c.receipt_bytes)->>'command_id' = c.command_id::text
                          AND pg_temp.stage6_retention_json(c.receipt_bytes)->>'gateway_id' = e.gateway_id
                          AND pg_temp.stage6_retention_json(c.receipt_bytes)->>'entity_id' = e.entity_id
                          AND pg_temp.stage6_retention_json(c.receipt_bytes)->>'desired_version' = c.desired_version::text
                          AND pg_temp.stage6_retention_json(c.receipt_bytes)->>'command_hash'
                              = encode(sha256(o.payload_bytes), 'hex')
                          AND pg_input_is_valid(pg_temp.stage6_retention_json(c.receipt_bytes)->>'received_at',
                                                'timestamp with time zone')
                          AND (pg_temp.stage6_retention_json(c.receipt_bytes)->>'received_at')::timestamptz
                              >= c.issued_at
                          AND (SELECT count(*) FROM jsonb_object_keys(
                              pg_temp.stage6_retention_json(c.receipt_bytes))) = 8
                       ELSE false
                     END
            ) FROM public.twin_outbox o
              CROSS JOIN LATERAL (
                  SELECT pg_temp.stage6_retention_json(o.payload_bytes) AS body
              ) outbox_doc
            WHERE o.command_id = c.command_id) AS outbox_valid
    FROM public.twin_commands c
    JOIN public.twin_entities e ON e.id = c.entity_id
    JOIN retention_scope scope ON scope.command_id=c.command_id
    LEFT JOIN request_counts ON request_counts.command_id = c.command_id
    CROSS JOIN LATERAL (
        SELECT CASE
                 WHEN length(c.requested_frequency_hz) <= 128
                  AND c.requested_frequency_hz ~ '^-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?$'
                  AND pg_input_is_valid(c.requested_frequency_hz, 'numeric')
                  AND CASE
                        WHEN position('e' IN lower(c.requested_frequency_hz)) > 0
                        THEN ltrim(substring(c.requested_frequency_hz FROM '[eE][+-]?([0-9]+)$'), '0')
                              ~ '^([0-9]{0,2}|1[01][0-9]|12[0-8])$'
                        ELSE true
                      END
                 THEN c.requested_frequency_hz::numeric
                 ELSE NULL
               END AS requested_hz
    ) raw_frequency
    CROSS JOIN LATERAL (
        SELECT raw_frequency.requested_hz,
               CASE
                 WHEN raw_frequency.requested_hz > 0 AND raw_frequency.requested_hz <= 100
                  AND c.parameters->>'sample_interval_us' ~ '^[0-9]{1,16}$'
                  AND (c.parameters->>'sample_interval_us')::numeric BETWEEN 10000 AND 9007199254740992
                  AND ((c.parameters->>'sample_interval_us')::numeric - 0.5)
                         * raw_frequency.requested_hz <= 1000000
                  AND 1000000 < ((c.parameters->>'sample_interval_us')::numeric + 0.5)
                         * raw_frequency.requested_hz
                 THEN (c.parameters->>'sample_interval_us')::numeric
                 ELSE NULL
               END AS requested_interval,
               CASE
                 -- big.Rat.RatString omits the denominator for integers.
                 WHEN raw_frequency.requested_hz > 0 AND raw_frequency.requested_hz <= 100
                  AND scale(trim_scale(raw_frequency.requested_hz)) = 0
                 THEN trim_scale(raw_frequency.requested_hz)::text
                 WHEN raw_frequency.requested_hz > 0 AND raw_frequency.requested_hz <= 100
                 THEN trim_scale((
                     (trim_scale(raw_frequency.requested_hz)
                       * power(10::numeric, scale(trim_scale(raw_frequency.requested_hz))))
                     / gcd(
                         trim_scale(raw_frequency.requested_hz)
                           * power(10::numeric, scale(trim_scale(raw_frequency.requested_hz))),
                         power(10::numeric, scale(trim_scale(raw_frequency.requested_hz)))
                     )
                 ))::text || '/' || trim_scale((
                     power(10::numeric, scale(trim_scale(raw_frequency.requested_hz)))
                     / gcd(
                         trim_scale(raw_frequency.requested_hz)
                           * power(10::numeric, scale(trim_scale(raw_frequency.requested_hz))),
                         power(10::numeric, scale(trim_scale(raw_frequency.requested_hz)))
                     )
                 ))::text
                 ELSE NULL
               END AS rational_hz
    ) frequency
    CROSS JOIN LATERAL (
        SELECT pg_temp.stage6_retention_json(c.result_bytes) AS result_document,
               pg_temp.stage6_retention_json(c.late_result_bytes) AS late_result_document
    ) parsed
    CROSS JOIN LATERAL (
        -- JSONB normalizes exponent notation and silently deduplicates object keys.
        -- Validate that raw JSON contains reported_state exactly once, and that
        -- the original reported_state object contains exactly one entry named sample_interval_us.
        -- Only after these duplicate-preserving checks pass, extract the original
        -- scalar token for the integer wire check.
        SELECT CASE
                 WHEN parsed.result_document IS NOT NULL
                  AND jsonb_typeof(parsed.result_document) = 'object'
                  AND parsed.result_document ? 'reported_state'
                  AND jsonb_typeof(parsed.result_document->'reported_state') = 'object'
                  AND (
                      SELECT count(*) = 1
                      FROM json_each(convert_from(c.result_bytes, 'UTF8')::json) top_entry
                      WHERE top_entry.key = 'reported_state'
                  )
                  AND (
                      SELECT count(*) = 1 AND count(*) FILTER (WHERE nested_entry.key = 'sample_interval_us') = 1
                      FROM json_each(
                          (SELECT top_entry.value
                           FROM json_each(convert_from(c.result_bytes, 'UTF8')::json) top_entry
                           WHERE top_entry.key = 'reported_state'
                           LIMIT 1)
                      ) nested_entry
                  )
                 THEN (convert_from(c.result_bytes, 'UTF8')::json
                       ->'reported_state'->'sample_interval_us')::text
                 ELSE NULL
               END AS result_interval,
               CASE
                 WHEN parsed.late_result_document IS NOT NULL
                  AND jsonb_typeof(parsed.late_result_document) = 'object'
                  AND parsed.late_result_document ? 'reported_state'
                  AND jsonb_typeof(parsed.late_result_document->'reported_state') = 'object'
                  AND (
                      SELECT count(*) = 1
                      FROM json_each(convert_from(c.late_result_bytes, 'UTF8')::json) top_entry
                      WHERE top_entry.key = 'reported_state'
                  )
                  AND (
                      SELECT count(*) = 1 AND count(*) FILTER (WHERE nested_entry.key = 'sample_interval_us') = 1
                      FROM json_each(
                          (SELECT top_entry.value
                           FROM json_each(convert_from(c.late_result_bytes, 'UTF8')::json) top_entry
                           WHERE top_entry.key = 'reported_state'
                           LIMIT 1)
                      ) nested_entry
                  )
                 THEN (convert_from(c.late_result_bytes, 'UTF8')::json
                       ->'reported_state'->'sample_interval_us')::text
                 ELSE NULL
               END AS late_interval
    ) wire
 ),
command_dependencies AS MATERIALIZED (
    SELECT reference_id,
           count(*) AS dependency_count,
           max(command_id::text) FILTER (
               WHERE parent_command_id = reference_id
                 AND root_command_id = reference_id
           ) AS child_id
    FROM (
        SELECT command_id,root_command_id AS reference_id,root_command_id,parent_command_id
        FROM public.twin_commands
        WHERE root_command_id IS NOT NULL AND command_id <> root_command_id
        UNION
        SELECT command_id,parent_command_id AS reference_id,root_command_id,parent_command_id
        FROM public.twin_commands
        WHERE parent_command_id IS NOT NULL
          AND command_id <> parent_command_id
          AND parent_command_id IS DISTINCT FROM root_command_id
    ) dependencies
    GROUP BY reference_id
),
current_version_counts AS MATERIALIZED (
    SELECT entity_id,desired_version,count(*) AS command_count
    FROM public.twin_commands
    GROUP BY entity_id,desired_version
),
current_intents AS MATERIALIZED (
    SELECT state.entity_id,state.desired_version AS current_version,
           current_command.command_id,current_command.root_command_id
    FROM public.twin_states state
    JOIN current_version_counts version_count
      ON version_count.entity_id=state.entity_id
     AND version_count.desired_version=state.desired_version
     AND version_count.command_count=1
    JOIN command_facts current_command
      ON current_command.entity_id=state.entity_id
     AND current_command.desired_version=state.desired_version
    LEFT JOIN command_dependencies current_deps
      ON current_deps.reference_id=current_command.command_id
    WHERE CASE
            WHEN jsonb_typeof(state.desired_state->'sample_interval_us')='number'
             AND state.desired_state->>'sample_interval_us' ~ '^[0-9]{1,16}$'
            THEN (state.desired_state->>'sample_interval_us')::numeric
                 BETWEEN 10000 AND 9007199254740992
            ELSE false
          END
      AND current_command.stage6_managed IS TRUE
      AND current_command.control_version=1
      AND current_command.actual_entity_type='Sensor'
      AND current_command.target_entity_type='Sensor'
      AND current_command.action='update_configuration'
      AND current_command.status IN ('pending','published','acknowledged',
                                     'succeeded','failed','timeout','cancelled')
      AND current_command.outbox_valid
      AND (current_command.status IN ('pending','published','acknowledged')
           OR current_command.terminal_evidence_valid)
      AND current_command.receipt_valid
      AND current_command.parameter_interval IS NOT NULL
      AND current_command.requested_interval=current_command.parameter_interval
      AND current_command.parameter_interval =
          (state.desired_state->>'sample_interval_us')::numeric
      AND (
          (current_command.origin='client'
           AND current_command.command_id=current_command.root_command_id
           AND current_command.parent_command_id IS NULL
           AND current_command.original_actor_id=current_command.issued_by
           AND current_command.admission_valid
           AND current_command.auto_successors_used=0
           AND COALESCE(current_deps.dependency_count,0)=0)
          OR
          (current_command.origin='reconciliation'
           AND current_command.parent_command_id IS NOT NULL
           AND current_command.root_command_id=current_command.parent_command_id
           AND current_command.issued_by IS NULL
           AND current_command.idempotency_key IS NULL
           AND current_command.auto_successors_used=0
           AND current_command.command_receipt_count=0
           AND EXISTS (
               SELECT 1
               FROM command_facts current_root
               JOIN command_dependencies root_deps
                 ON root_deps.reference_id=current_root.command_id
                AND root_deps.dependency_count=1
               WHERE current_root.command_id=current_command.root_command_id
                 AND current_root.entity_id=current_command.entity_id
                 AND current_root.stage6_managed IS TRUE
                 AND current_root.control_version=1
                 AND current_root.actual_entity_type='Sensor'
                 AND current_root.target_entity_type='Sensor'
                 AND current_root.action=current_command.action
                 AND current_root.origin='client'
                 AND current_root.command_id=current_root.root_command_id
                 AND current_root.parent_command_id IS NULL
                 AND current_root.original_actor_id=current_root.issued_by
                 AND current_root.admission_valid
                 AND current_root.auto_successors_used=1
                 AND current_root.terminal_evidence_valid
                 AND current_root.receipt_valid
                 AND current_root.outbox_valid
                 AND current_root.desired_version+1=current_command.desired_version
                 AND current_root.parameter_interval=current_command.parameter_interval
                 AND current_root.requested_interval=current_command.requested_interval
                 AND current_root.requested_frequency_hz=current_command.requested_frequency_hz
                 AND current_root.original_actor_id=current_command.original_actor_id
                 AND current_command.issued_at >= GREATEST(
                     current_root.completed_at,
                     COALESCE(current_root.result_received_at,current_root.completed_at))
                 AND (
                     NOT current_root.reconcile_blocked
                     OR (current_command.status='cancelled'
                         AND current_command.reconcile_blocked
                         AND current_command.retry_stop_reason='cancelled'
                         AND current_command.cancelled_at=current_command.completed_at
                         AND current_command.cancelled_by IS NOT NULL
                         AND current_command.first_reserved_at IS NULL
                         AND current_command.attempt_count=0)
                 )
           )
           AND COALESCE(current_deps.dependency_count,0)=0)
      )
),
root_shape AS (
    SELECT r.command_id AS root_id,
           r.entity_id,
           r.desired_version AS root_version,
           s.desired_version AS current_version,
           COALESCE(dep.dependency_count,0) AS dependency_count,
           dep.child_id,
           child.desired_version AS child_version,
           r.issued_at AS root_issued_at,
           r.expires_at AS root_expires_at,
           r.completed_at AS root_completed_at,
           r.acknowledged_at AS root_acknowledged_at,
           r.cancelled_at AS root_cancelled_at,
           r.first_reserved_at AS root_first_reserved_at,
           r.receipt_received_at AS root_receipt_received_at,
           r.result_received_at AS root_result_received_at,
           r.late_result_received_at AS root_late_result_received_at,
           child.issued_at AS child_issued_at,
           child.expires_at AS child_expires_at,
           child.completed_at AS child_completed_at,
           child.acknowledged_at AS child_acknowledged_at,
           child.cancelled_at AS child_cancelled_at,
           child.first_reserved_at AS child_first_reserved_at,
           child.receipt_received_at AS child_receipt_received_at,
           child.result_received_at AS child_result_received_at,
           child.late_result_received_at AS child_late_result_received_at
    FROM command_facts r
    JOIN public.twin_states s ON s.entity_id = r.entity_id
    JOIN current_intents current ON current.entity_id=r.entity_id
                                AND current.current_version=s.desired_version
    LEFT JOIN command_dependencies dep ON dep.reference_id=r.command_id
    LEFT JOIN command_facts child ON child.command_id = dep.child_id::uuid
    LEFT JOIN command_dependencies child_deps ON child_deps.reference_id=child.command_id
    WHERE r.stage6_managed IS TRUE
      AND r.control_version = 1
      AND r.actual_entity_type = 'Sensor'
      AND r.target_entity_type = 'Sensor'
      AND r.action = 'update_configuration'
      AND r.origin = 'client'
      AND r.root_command_id = r.command_id
      AND r.parent_command_id IS NULL
      AND r.original_actor_id IS NOT NULL
      AND r.original_actor_id = r.issued_by
      AND r.admission_valid
      AND r.parameter_interval IS NOT NULL
      AND r.parameter_interval = r.requested_interval
      AND r.desired_version >= 0
      AND r.expires_at > r.issued_at
      AND r.terminal_evidence_valid
      AND r.receipt_valid
      AND r.outbox_valid
      AND (
          (r.auto_successors_used = 0 AND COALESCE(dep.dependency_count,0) = 0)
          OR
          (r.auto_successors_used = 1 AND dep.dependency_count = 1
           AND dep.child_id IS NOT NULL
           AND child.command_id IS NOT NULL
           AND child.stage6_managed IS TRUE
           AND child.control_version = 1
           AND child.target_entity_type = 'Sensor'
           AND child.actual_entity_type = 'Sensor'
           AND child.actual_gateway_id = r.actual_gateway_id
           AND child.entity_id = r.entity_id
           AND child.action = r.action
           AND child.origin = 'reconciliation'
           AND child.root_command_id = r.command_id
           AND child.parent_command_id = r.command_id
           AND child.original_actor_id = r.original_actor_id
           AND child.issued_by IS NULL
           AND child.idempotency_key IS NULL
           AND child.auto_successors_used = 0
           AND child.parameter_interval IS NOT NULL
           AND child.parameter_interval = r.parameter_interval
           AND child.requested_interval = r.requested_interval
           AND child.requested_frequency_hz = r.requested_frequency_hz
           AND child.desired_version = r.desired_version + 1
            AND (
                NOT r.reconcile_blocked
                OR (child.status = 'cancelled' AND child.reconcile_blocked
                    AND child.retry_stop_reason = 'cancelled'
                    AND child.cancelled_at = child.completed_at
                    AND child.cancelled_by IS NOT NULL
                    AND child.first_reserved_at IS NULL AND child.attempt_count = 0)
            )
           AND child.desired_version < s.desired_version
           AND child.expires_at > child.issued_at
            AND child.issued_at >= GREATEST(r.completed_at,
                                             COALESCE(r.result_received_at, r.completed_at))
            AND child.terminal_evidence_valid
            AND child.receipt_valid
            AND child.outbox_valid
            AND child.command_receipt_count = 0
            AND COALESCE(child_deps.dependency_count,0) = 0)
       )
      AND r.status IN ('succeeded', 'failed', 'timeout', 'cancelled')
      AND s.desired_version > r.desired_version
       AND CASE
             WHEN jsonb_typeof(s.desired_state->'sample_interval_us') = 'number'
               AND (s.desired_state->>'sample_interval_us') ~ '^[0-9]{1,16}$'
             THEN (s.desired_state->>'sample_interval_us')::numeric BETWEEN 10000 AND 9007199254740992
             ELSE false
           END
       AND r.command_receipt_count = 1
       AND current.command_id <> r.command_id
       AND current.root_command_id <> r.command_id
),
lineages AS (
    SELECT shape.root_id,
           shape.child_id::uuid AS child_id,
           evidence.last_evidence
    FROM root_shape shape
    CROSS JOIN LATERAL (
        SELECT max(ts) AS last_evidence
        FROM (
            SELECT unnest(ARRAY[
                shape.root_issued_at, shape.root_expires_at, shape.root_completed_at,
                shape.root_acknowledged_at, shape.root_cancelled_at,
                shape.root_first_reserved_at, shape.root_receipt_received_at,
                shape.root_result_received_at, shape.root_late_result_received_at,
                shape.child_issued_at, shape.child_expires_at, shape.child_completed_at,
                shape.child_acknowledged_at, shape.child_cancelled_at,
                shape.child_first_reserved_at, shape.child_receipt_received_at,
                shape.child_result_received_at, shape.child_late_result_received_at
            ]) AS ts
            UNION ALL
            SELECT request.created_at FROM public.twin_command_requests request
            WHERE request.command_id = shape.root_id
            UNION ALL
            SELECT event_time FROM public.twin_outbox outbox
            CROSS JOIN LATERAL unnest(ARRAY[outbox.created_at, outbox.next_attempt_at, outbox.published_at]) event_time
            WHERE outbox.command_id = shape.root_id OR outbox.command_id = shape.child_id::uuid
        ) all_evidence
    ) evidence
    WHERE evidence.last_evidence <
          ((statement_timestamp() AT TIME ZONE 'UTC' - interval '720 hours') AT TIME ZONE 'UTC')
)
SELECT COALESCE(jsonb_agg(
           jsonb_build_object('root_id', root_id, 'child_id', child_id)
           ORDER BY last_evidence, root_id
       ), '[]'::jsonb)::text AS retention_candidates,
       count(*)::integer AS retention_count
FROM (
    SELECT root_id, child_id, last_evidence
    FROM lineages
    ORDER BY last_evidence, root_id
    LIMIT 100
) bounded_candidates
\gset
