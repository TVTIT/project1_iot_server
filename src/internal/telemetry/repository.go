package telemetry

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Repository persists one validated telemetry message. Implementations own a
// single transaction for the deduplication ledger, raw samples, the Sensor
// Twin reported-state advance, and the temporal sample_interval_us observation.
// A committed result confirms that this repository's ledger, raw, current-state,
// and temporal writes all committed together; it does not publish an
// application ACK, so a caller must not treat the result as delivered. The
// duplicate outcome confirms only the prior ledger commit and writes no other
// step.
type Repository interface {
	Persist(context.Context, *Message) (PersistResult, error)
}

// TxBeginner is the small pgx pool boundary used by a repository. pgxpool.Pool
// implements it directly, while tests can provide a fake transaction source.
type TxBeginner interface {
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
}

// MessageIdentity identifies the validated resource and message within which
// the candidate samples were received.
type MessageIdentity struct {
	GatewayID string
	SensorID  string
	BootID    uuid.UUID
	MessageID uuid.UUID
}

// PersistResult describes the ledger, raw, current-state, and temporal write
// after transaction completion. Its zero value is deliberately unconfirmed and
// therefore is never success.
type PersistResult struct {
	Identity     MessageIdentity
	Outcome      CommitOutcome
	SamplesSaved int
}
