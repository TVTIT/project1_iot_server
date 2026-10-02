package auth

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const platformAdminQuery = `SELECT EXISTS (
    SELECT 1
    FROM platform_admins
    WHERE user_id = $1
)`

// QueryRower is the subset of pgxpool.Pool required by the admin checker.
type QueryRower interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

// PlatformAdminChecker determines admin membership from PostgreSQL.
type PlatformAdminChecker interface {
	IsPlatformAdmin(context.Context, uuid.UUID) (bool, error)
}

type postgresPlatformAdminChecker struct {
	database QueryRower
}

// NewPostgresPlatformAdminChecker creates a fail-closed PostgreSQL membership checker.
func NewPostgresPlatformAdminChecker(database QueryRower) (PlatformAdminChecker, error) {
	if IsNilDependency(database) {
		return nil, fmt.Errorf("platform admin database is required")
	}
	return &postgresPlatformAdminChecker{database: database}, nil
}

func (c *postgresPlatformAdminChecker) IsPlatformAdmin(ctx context.Context, userID uuid.UUID) (bool, error) {
	var admin bool
	if err := c.database.QueryRow(ctx, platformAdminQuery, userID).Scan(&admin); err != nil {
		return false, fmt.Errorf("lookup platform admin membership: %w", err)
	}
	return admin, nil
}
