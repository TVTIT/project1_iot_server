package telemetry

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Repository persists one validated telemetry message. Implementations own a
// single transaction for the raw-message ledger and samples. A committed result
// confirms only this repository's writes; it does not include Digital Twin
// updates or authorize publishing an application ACK.
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

// PersistResult describes the ledger/raw write after transaction completion.
// Its zero value is deliberately unconfirmed and therefore is never success.
type PersistResult struct {
	Identity     MessageIdentity
	Outcome      CommitOutcome
	SamplesSaved int
}
