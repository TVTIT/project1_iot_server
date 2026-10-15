package telemetry

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/gateway"
)

// CommitOutcome is supplied by the persistence caller only after it has
// established the corresponding committed ledger result.
type CommitOutcome uint8

const (
	// CommitOutcomeUnconfirmed indicates the caller has no commit confirmation.
	CommitOutcomeUnconfirmed CommitOutcome = iota
	// CommitOutcomeCommitted indicates the new message is committed.
	CommitOutcomeCommitted
	// CommitOutcomeDuplicateCommitted indicates a prior commit was confirmed.
	CommitOutcomeDuplicateCommitted
)

// CommitACK is the five-field v1 application-level database commit ACK.
// Building this DTO is a conversion gate; it does not prove a database commit.
type CommitACK struct {
	ProtocolVersion int       `json:"protocol_version"`
	MessageType     string    `json:"message_type"`
	MessageID       uuid.UUID `json:"message_id"`
	Status          string    `json:"status"`
	CommittedAt     time.Time `json:"committed_at"`
}

// NewCommitACK converts a confirmed commit outcome into its wire topic and DTO.
// The caller is responsible for confirming the commit or committed duplicate.
func NewCommitACK(gatewayID string, messageID uuid.UUID, outcome CommitOutcome, confirmedAt time.Time) (string, CommitACK, error) {
	if gateway.ValidateGatewayID(gatewayID) != nil {
		return "", CommitACK{}, invalidPayloadError("gateway_id")
	}
	if messageID == uuid.Nil || messageID.Variant() != uuid.RFC4122 || (messageID.Version() != 4 && messageID.Version() != 7) {
		return "", CommitACK{}, invalidPayloadError("message_id")
	}
	if outcome != CommitOutcomeCommitted && outcome != CommitOutcomeDuplicateCommitted {
		return "", CommitACK{}, invalidPayloadError("outcome")
	}
	if confirmedAt.IsZero() {
		return "", CommitACK{}, invalidPayloadError("committed_at")
	}
	committedAt := confirmedAt.UTC()
	if year := committedAt.Year(); year < 1 || year > 9999 {
		return "", CommitACK{}, invalidPayloadError("committed_at")
	}

	ack := CommitACK{
		ProtocolVersion: 1,
		MessageType:     "telemetry_ack",
		MessageID:       messageID,
		Status:          "committed",
		CommittedAt:     committedAt,
	}
	topic := fmt.Sprintf("gateways/%s/acks/%s", gatewayID, messageID)
	return topic, ack, nil
}
