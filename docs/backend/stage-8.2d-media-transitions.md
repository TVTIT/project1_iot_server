# Stage 8.2d — Durable media grant and completion transitions

`internal/media.Repository` remains PostgreSQL-only; it does no Storage/network
I/O. A future signer uses this sequence:

1. `ReserveGrant(GrantRequest)` authorizes the current Gateway credential, locks
   its managed intent row, and only returns `need_upload` for a committed,
   pending, unexpired intent whose deadline is later than the reservation
   horizon and which has no unresolved possible PUT.
2. The caller signs outside SQL. `GrantReservation` contains the row ID,
   `GrantGeneration`, `ReservedUntil`, and intent deadline, but no URL/token.
   The caller supplies the bounded signing timeout, configured signed-URL TTL,
   and in-flight grace; the horizon covers their sum from reservation time.
3. `ConfirmGrant(reservation, exactSignedURLExpiry)` reauthorizes and fences
   by managed row ID, pending state, intent deadline, grant generation, and
   current reservation horizon. Publish only if it returns true and the exact
   expiry is no later than `ReservedUntil`. Any signing/confirmation failure
   leaves the reserved horizon and possible-PUT fence intact.

`next_reconcile_at` is the durable unresolved-PUT fence. Duplicate grant
requests return `retry_after_completion` without a URL until authorized
`Complete` records completion and a generation-matched verifier calls
`RecordMissing` after the reserved reconcile horizon has elapsed. That
transition clears the fence and returns a still-live intent to pending, so a
previous URL cannot still authorize a later PUT when a replacement is issued.
If a live pending intent has no unresolved grant but cannot fit a full new
reservation horizon before its deadline, `ReserveGrant` returns non-terminal
`pending` without modifying durable state or prematurely expiring the intent;
only genuinely elapsed deadlines become `expired`.
`Complete` schedules a live pending row as verifying once;
replays do not reset due time, verification attempts, or generation. Expired
pending/verifying intents become terminal `expired`, with cleanup no earlier
than both intent expiry and `latest_upload_grant_expires_at`.

This slice does not implement signing, URL publication, verifier claims or
workers, expiry scanning, Storage access, MQTT wiring, or cleanup execution.
