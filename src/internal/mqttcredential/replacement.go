package mqttcredential

import (
	"context"

	"github.com/google/uuid"
)

// ReplacementRequest is internal trusted input. The higher layer must prove
// worker/native quiescence before admission; DB CAS alone cannot fence MQTT.
type ReplacementRequest struct {
	ActorUserID    uuid.UUID
	OperationID    uuid.UUID
	IdempotencyKey uuid.UUID
	Guard          OperationGuard
}

// ReplacementRepository deliberately does not extend ordinary runtime interfaces.
// Admission replay is always metadata-only, including pending/lost-response jobs.
// There is no automatic resume, root traversal, secret retrieval, or DB owner.
type ReplacementRepository interface {
	BeginReplacement(context.Context, ReplacementRequest) (BeginResult, error)
}
