package digitaltwin

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// AuthorizeControlTarget checks current membership before HTTP input errors can
// reveal scoped target information. Admission independently rechecks under locks
// through commit; this preliminary transaction conveys no mutation authority.
func (r *PostgresControlRepository) AuthorizeControlTarget(ctx context.Context, actor uuid.UUID, urn string, mutation bool) error {
	if ctx == nil || actor == uuid.Nil {
		return ErrInvalidInput
	}
	p, err := ParseEntityURN(urn)
	if err != nil {
		return err
	}
	if r == nil || isNilDependency(r.db) {
		return ErrDependencyUnavailable
	}
	ctx, cancel := context.WithTimeout(ctx, r.config.DBTimeout)
	defer cancel()
	tx, err := r.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return classifyAvailabilityError(err)
	}
	defer rollbackTx(tx)
	_, err = lockControlTarget(ctx, tx, actor, p, mutation)
	return err
}
