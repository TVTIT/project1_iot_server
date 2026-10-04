package mqttcredential

import (
	"context"
	"testing"

	"github.com/google/uuid"
)

var _ Repository = (*PostgresRepository)(nil)

func TestPostgresFinalizationInvalidInput(t *testing.T) {
	r := &PostgresRepository{}
	for _, write := range []func(context.Context, OperationUpdate) (Metadata, error){r.ConditionalFinalize, r.FailKnown, r.RequireRecovery} {
		_, err := write(context.Background(), OperationUpdate{})
		requireCode(t, err, CodeInvalidRequest)
	}
	for _, limit := range []int{-1, 0, 1001} {
		_, err := r.ListUnresolved(context.Background(), uuid.Nil, limit)
		requireCode(t, err, CodeInvalidRequest)
		_, err = r.ListDurableRevocations(context.Background(), "", limit)
		requireCode(t, err, CodeInvalidRequest)
	}
}
