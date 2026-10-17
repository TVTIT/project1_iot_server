package gateway

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// ProvisioningDatabase is satisfied by the application's existing pgx pool.
type ProvisioningDatabase interface {
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
}

// ErrProvisioningAdminLookup distinguishes authorization infrastructure failures
// from mutation failures for the HTTP service-unavailable mapping.
var ErrProvisioningAdminLookup = errors.New("provisioning admin lookup failed")

type postgresProvisioningRepository struct{ database ProvisioningDatabase }

// NewPostgresProvisioningRepository adapts the existing pool to atomic provisioning.
func NewPostgresProvisioningRepository(database ProvisioningDatabase) (ProvisioningRepository, error) {
	if isNilDependency(database) {
		return nil, fmt.Errorf("provisioning database is required")
	}
	return &postgresProvisioningRepository{database: database}, nil
}

// NewPostgresOwnerSensorProvisioningRepository creates an adapter for owner sensor provisioning.
func NewPostgresOwnerSensorProvisioningRepository(database ProvisioningDatabase) (OwnerSensorProvisioningRepository, error) {
	if isNilDependency(database) {
		return nil, fmt.Errorf("provisioning database is required")
	}
	return &postgresProvisioningRepository{database: database}, nil
}

// NewPostgresAdminOrOwnerSensorProvisioningRepository creates an adapter for admin-or-owner sensor provisioning.
func NewPostgresAdminOrOwnerSensorProvisioningRepository(database ProvisioningDatabase) (AdminOrOwnerSensorProvisioningRepository, error) {
	if isNilDependency(database) {
		return nil, fmt.Errorf("provisioning database is required")
	}
	return &postgresProvisioningRepository{database: database}, nil
}

// A cancelled caller must not prevent rollback and return of the pool connection.
func rollbackProvisioning(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}
