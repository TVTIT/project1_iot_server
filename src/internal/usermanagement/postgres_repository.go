package usermanagement

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	// ErrInconsistentProfile indicates an Auth user exists without a corresponding profile row.
	ErrInconsistentProfile = errors.New("inconsistent user profile: profile row missing")

	// ErrActorNotAdmin indicates actor does not possess platform admin privileges in transaction.
	ErrActorNotAdmin = errors.New("actor is not a platform admin")
)

// PostgresRepository implements Repository using PostgreSQL.
type PostgresRepository struct {
	pool *pgxpool.Pool
}

var _ Repository = (*PostgresRepository)(nil)

// NewPostgresRepository creates a PostgresRepository backed by pgxpool.Pool.
func NewPostgresRepository(pool *pgxpool.Pool) (*PostgresRepository, error) {
	if pool == nil {
		return nil, fmt.Errorf("database pool cannot be nil")
	}
	return &PostgresRepository{pool: pool}, nil
}

// VerifyActorIsAdmin checks whether actorID is present in public.platform_admins.
func (r *PostgresRepository) VerifyActorIsAdmin(ctx context.Context, actorID uuid.UUID) error {
	if actorID == uuid.Nil {
		return ErrActorNotAdmin
	}
	var isAdmin bool
	err := r.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM public.platform_admins WHERE user_id = $1)`,
		actorID,
	).Scan(&isAdmin)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		return fmt.Errorf("verify actor platform admin: %w", err)
	}
	if !isAdmin {
		return ErrActorNotAdmin
	}
	return nil
}

// VerifyCreatedUserProfile verifies that the user profile was created with the expected name,
// and confirms the new user has neither platform_admin role nor gateway memberships.
func (r *PostgresRepository) VerifyCreatedUserProfile(ctx context.Context, targetID uuid.UUID, expectedFullName string) error {
	if targetID == uuid.Nil {
		return fmt.Errorf("%w: target_id cannot be nil", ErrInvalidInput)
	}

	var fullName *string
	var hasAdmin bool
	var hasMemberships bool

	err := r.pool.QueryRow(ctx,
		`SELECT 
			p.full_name,
			EXISTS(SELECT 1 FROM public.platform_admins pa WHERE pa.user_id = $1) AS has_admin,
			EXISTS(SELECT 1 FROM public.user_gateways ug WHERE ug.user_id = $1) AS has_memberships
		 FROM public.profiles p
		 WHERE p.id = $1`,
		targetID,
	).Scan(&fullName, &hasAdmin, &hasMemberships)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrInconsistentProfile
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		return fmt.Errorf("query profile verification: %w", err)
	}

	if fullName == nil || *fullName != expectedFullName {
		actual := "<nil>"
		if fullName != nil {
			actual = *fullName
		}
		return fmt.Errorf("%w: full_name mismatch (got %q, want %q)", ErrInconsistentProfile, actual, expectedFullName)
	}

	if hasAdmin {
		return fmt.Errorf("%w: new user unexpectedly has platform_admin role", ErrInconsistentProfile)
	}

	if hasMemberships {
		return fmt.Errorf("%w: new user unexpectedly has gateway memberships", ErrInconsistentProfile)
	}

	return nil
}

func isActorNotAdminError(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && strings.Contains(pgErr.Message, "actor is not a platform admin") {
		return true
	}
	return false
}

// ListUsersProjection queries the security-definer function for bounded allowlisted user rows.
func (r *PostgresRepository) ListUsersProjection(ctx context.Context, actorID uuid.UUID, limit int) ([]UserProjectionRow, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT user_id, email, full_name, platform_role, created_at, profile_exists 
		 FROM public.list_admin_users_projection($1, $2)`,
		actorID, limit,
	)
	if err != nil {
		if isActorNotAdminError(err) {
			return nil, ErrActorNotAdmin
		}
		return nil, fmt.Errorf("query list_admin_users_projection: %w", err)
	}
	defer rows.Close()

	var result []UserProjectionRow
	for rows.Next() {
		var row UserProjectionRow
		if err := rows.Scan(&row.UserID, &row.Email, &row.FullName, &row.PlatformRole, &row.CreatedAt, &row.ProfileExists); err != nil {
			return nil, fmt.Errorf("scan user projection row: %w", err)
		}
		if !row.ProfileExists {
			return nil, ErrInconsistentProfile
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		if isActorNotAdminError(err) {
			return nil, ErrActorNotAdmin
		}
		return nil, fmt.Errorf("iterate user projection rows: %w", err)
	}
	return result, nil
}

// ListUserGatewayRoles reads memberships for the given user IDs up to limit.
func (r *PostgresRepository) ListUserGatewayRoles(ctx context.Context, userIDs []uuid.UUID, limit int) ([]UserGatewayRoleRow, error) {
	if len(userIDs) == 0 {
		return []UserGatewayRoleRow{}, nil
	}

	rows, err := r.pool.Query(ctx,
		`SELECT user_id, gateway_id, role 
		 FROM public.user_gateways 
		 WHERE user_id = ANY($1) 
		 ORDER BY user_id ASC, gateway_id ASC 
		 LIMIT $2`,
		userIDs, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("query user_gateways: %w", err)
	}
	defer rows.Close()

	var result []UserGatewayRoleRow
	for rows.Next() {
		var row UserGatewayRoleRow
		if err := rows.Scan(&row.UserID, &row.GatewayID, &row.Role); err != nil {
			return nil, fmt.Errorf("scan user gateway role row: %w", err)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate user gateway role rows: %w", err)
	}
	return result, nil
}

// UpdateUserProfileAndRole updates full_name and/or platform_role atomically in one transaction.
func (r *PostgresRepository) UpdateUserProfileAndRole(ctx context.Context, actorID, targetID uuid.UUID, fullName *string, platformRole *string) (UpdatedUser, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return UpdatedUser{}, fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	// 1. Transaction-local actor recheck
	var isActorAdmin bool
	err = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM public.platform_admins WHERE user_id = $1)", actorID).Scan(&isActorAdmin)
	if err != nil {
		return UpdatedUser{}, fmt.Errorf("recheck actor platform admin: %w", err)
	}
	if !isActorAdmin {
		return UpdatedUser{}, ErrActorNotAdmin
	}

	// 2. Lock target profile row and check existence
	var currentName *string
	err = tx.QueryRow(ctx, "SELECT full_name FROM public.profiles WHERE id = $1 FOR UPDATE", targetID).Scan(&currentName)
	if errors.Is(err, pgx.ErrNoRows) {
		return UpdatedUser{}, ErrUserNotFound
	}
	if err != nil {
		return UpdatedUser{}, fmt.Errorf("lock target profile: %w", err)
	}

	// 3. Self-demotion check
	if actorID == targetID && platformRole != nil && *platformRole == "user" {
		return UpdatedUser{}, ErrSelfDemotionForbidden
	}

	// 4. Update full_name if requested
	if fullName != nil {
		_, err = tx.Exec(ctx, "UPDATE public.profiles SET full_name = $1 WHERE id = $2", *fullName, targetID)
		if err != nil {
			return UpdatedUser{}, fmt.Errorf("update profile full_name: %w", err)
		}
		currentName = fullName
	}

	// 5. Update platform_role if requested
	if platformRole != nil {
		switch *platformRole {
		case "platform_admin":
			_, err = tx.Exec(ctx,
				"INSERT INTO public.platform_admins (user_id, granted_by) VALUES ($1, $2) ON CONFLICT (user_id) DO NOTHING",
				targetID, actorID,
			)
			if err != nil {
				return UpdatedUser{}, fmt.Errorf("grant platform admin: %w", err)
			}
		case "user":
			_, err = tx.Exec(ctx, "DELETE FROM public.platform_admins WHERE user_id = $1", targetID)
			if err != nil {
				return UpdatedUser{}, fmt.Errorf("revoke platform admin: %w", err)
			}
		}
	}

	// 6. Inspect final platform_role
	var isTargetAdmin bool
	err = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM public.platform_admins WHERE user_id = $1)", targetID).Scan(&isTargetAdmin)
	if err != nil {
		return UpdatedUser{}, fmt.Errorf("inspect target platform admin: %w", err)
	}
	finalRole := "user"
	if isTargetAdmin {
		finalRole = "platform_admin"
	}

	if err := tx.Commit(ctx); err != nil {
		return UpdatedUser{}, fmt.Errorf("commit profile and role update: %w", err)
	}

	return UpdatedUser{
		ID:           targetID,
		FullName:     currentName,
		PlatformRole: finalRole,
	}, nil
}

// AssignGatewayUser assigns an operator or viewer role on a gateway.
func (r *PostgresRepository) AssignGatewayUser(ctx context.Context, actorID uuid.UUID, gatewayID string, targetID uuid.UUID, role string) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	// 1. Transaction-local actor recheck
	var isActorAdmin bool
	err = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM public.platform_admins WHERE user_id = $1)", actorID).Scan(&isActorAdmin)
	if err != nil {
		return fmt.Errorf("recheck actor platform admin: %w", err)
	}
	if !isActorAdmin {
		return ErrActorNotAdmin
	}

	// 2. Lock target Gateway row FOR UPDATE and check existence
	var gwName string
	err = tx.QueryRow(ctx, "SELECT name FROM public.gateways WHERE gateway_id = $1 FOR UPDATE", gatewayID).Scan(&gwName)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrGatewayNotFound
	}
	if err != nil {
		return fmt.Errorf("lock gateway row: %w", err)
	}

	// 3. Check target user exists in profiles
	var targetProfileExists bool
	err = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM public.profiles WHERE id = $1)", targetID).Scan(&targetProfileExists)
	if err != nil {
		return fmt.Errorf("check target profile: %w", err)
	}
	if !targetProfileExists {
		return ErrUserNotFound
	}

	// 4. Lock current membership if it exists
	var currentRole string
	err = tx.QueryRow(ctx, "SELECT role FROM public.user_gateways WHERE gateway_id = $1 AND user_id = $2 FOR UPDATE", gatewayID, targetID).Scan(&currentRole)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("check current membership: %w", err)
	}

	if err == nil {
		// Existing membership found
		if currentRole == "owner" {
			return ErrOwnerTransferRequired
		}
		if currentRole == role {
			// Exact no-op: target already has this role, preserve created_at and avoid unnecessary write
			if err := tx.Commit(ctx); err != nil {
				return fmt.Errorf("commit no-op gateway user assignment: %w", err)
			}
			return nil
		}
		// Update existing membership role (created_at is preserved since it is not touched)
		_, err = tx.Exec(ctx, "UPDATE public.user_gateways SET role = $1 WHERE gateway_id = $2 AND user_id = $3", role, gatewayID, targetID)
		if err != nil {
			return fmt.Errorf("update user_gateways role: %w", err)
		}
	} else {
		// No existing membership, insert new membership
		_, err = tx.Exec(ctx, "INSERT INTO public.user_gateways (user_id, gateway_id, role) VALUES ($1, $2, $3)", targetID, gatewayID, role)
		if err != nil {
			return fmt.Errorf("insert user_gateways role: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit gateway user assignment: %w", err)
	}
	return nil
}

// RemoveGatewayUser serializes with assignment/owner transfer on the parent Gateway.
// A missing membership commits a no-op; owner membership is never deleted.
func (r *PostgresRepository) RemoveGatewayUser(ctx context.Context, actorID uuid.UUID, gatewayID string, targetID uuid.UUID) (resultErr error) {
	defer func() {
		var netErr net.Error
		var pgErr *pgconn.PgError
		if errors.As(resultErr, &netErr) || errors.Is(resultErr, io.EOF) ||
			(errors.As(resultErr, &pgErr) && (strings.HasPrefix(pgErr.Code, "08") || pgErr.Code == "57P01" || pgErr.Code == "57P02" || pgErr.Code == "57P03" || pgErr.Code == "53300")) {
			resultErr = errors.Join(ErrMembershipUnavailable, resultErr)
		}
	}()
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("%w: begin membership removal: %w", ErrMembershipUnavailable, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var admin bool
	if err := tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM public.platform_admins WHERE user_id = $1)", actorID).Scan(&admin); err != nil {
		// Authorization cannot be established; do not treat this as a denial or success.
		return fmt.Errorf("%w: %w", ErrMembershipUnavailable, err)
	}
	if !admin {
		return ErrActorNotAdmin
	}
	var gw string
	if err := tx.QueryRow(ctx, "SELECT gateway_id FROM public.gateways WHERE gateway_id = $1 FOR UPDATE", gatewayID).Scan(&gw); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrGatewayNotFound
		}
		return fmt.Errorf("lock removal gateway: %w", err)
	}
	var role string
	err = tx.QueryRow(ctx, "SELECT role FROM public.user_gateways WHERE gateway_id = $1 AND user_id = $2 FOR UPDATE", gatewayID, targetID).Scan(&role)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("lock removal membership: %w", err)
	}
	if err == nil {
		switch role {
		case "owner":
			return ErrOwnerTransferRequired
		case "operator", "viewer":
			tag, err := tx.Exec(ctx, `DELETE FROM public.user_gateways WHERE gateway_id = $1 AND user_id = $2 AND role IN ('operator', 'viewer')`, gatewayID, targetID)
			if err != nil {
				return fmt.Errorf("delete membership: %w", err)
			}
			if tag.RowsAffected() != 1 {
				return fmt.Errorf("unexpected membership removal row count")
			}
		default:
			return fmt.Errorf("unexpected membership role")
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit membership removal: %w", err)
	}
	return nil
}

// TransferGatewayOwner transfers ownership of a gateway atomically.
func (r *PostgresRepository) TransferGatewayOwner(ctx context.Context, actorID uuid.UUID, gatewayID string, targetID uuid.UUID) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	// 1. Transaction-local actor platform admin recheck
	var isActorAdmin bool
	err = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM public.platform_admins WHERE user_id = $1)", actorID).Scan(&isActorAdmin)
	if err != nil {
		return fmt.Errorf("recheck actor platform admin: %w", err)
	}
	if !isActorAdmin {
		return ErrActorNotAdmin
	}

	// 2. Lock target Gateway row FOR UPDATE and check existence (same mechanism and lock order as U5)
	var gwName string
	err = tx.QueryRow(ctx, "SELECT name FROM public.gateways WHERE gateway_id = $1 FOR UPDATE", gatewayID).Scan(&gwName)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrGatewayNotFound
	}
	if err != nil {
		return fmt.Errorf("lock gateway row: %w", err)
	}

	// 3. Check target user exists in profiles
	var targetProfileExists bool
	err = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM public.profiles WHERE id = $1)", targetID).Scan(&targetProfileExists)
	if err != nil {
		return fmt.Errorf("check target profile: %w", err)
	}
	if !targetProfileExists {
		return ErrUserNotFound
	}

	// 4. Locked current owner cardinality check
	rows, err := tx.Query(ctx, "SELECT user_id FROM public.user_gateways WHERE gateway_id = $1 AND role = 'owner' FOR UPDATE", gatewayID)
	if err != nil {
		return fmt.Errorf("query current owners: %w", err)
	}
	defer rows.Close()

	var currentOwnerIDs []uuid.UUID
	for rows.Next() {
		var uid uuid.UUID
		if err := rows.Scan(&uid); err != nil {
			return fmt.Errorf("scan current owner: %w", err)
		}
		currentOwnerIDs = append(currentOwnerIDs, uid)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate current owners: %w", err)
	}
	rows.Close()

	if len(currentOwnerIDs) != 1 {
		// Zero or multiple owners in database -> fail closed, never auto-repair inconsistent legacy state
		return ErrGatewayOwnerInconsistent
	}

	currentOwnerID := currentOwnerIDs[0]

	// 5. Same owner is exact no-op
	if currentOwnerID == targetID {
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit no-op owner transfer: %w", err)
		}
		return nil
	}

	// 6. Demote old owner to 'viewer' (preserves old owner's created_at)
	_, err = tx.Exec(ctx, "UPDATE public.user_gateways SET role = 'viewer' WHERE gateway_id = $1 AND user_id = $2", gatewayID, currentOwnerID)
	if err != nil {
		return fmt.Errorf("demote old owner to viewer: %w", err)
	}

	// 7. Check target's current membership to promote target to 'owner' (preserves existing created_at if exists)
	var targetCurrentRole string
	err = tx.QueryRow(ctx, "SELECT role FROM public.user_gateways WHERE gateway_id = $1 AND user_id = $2 FOR UPDATE", gatewayID, targetID).Scan(&targetCurrentRole)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("check target membership: %w", err)
	}

	if err == nil {
		// Target already has membership (operator or viewer) -> update to owner
		_, err = tx.Exec(ctx, "UPDATE public.user_gateways SET role = 'owner' WHERE gateway_id = $1 AND user_id = $2", gatewayID, targetID)
		if err != nil {
			return fmt.Errorf("update target membership to owner: %w", err)
		}
	} else {
		// Target has no membership -> insert new owner row
		_, err = tx.Exec(ctx, "INSERT INTO public.user_gateways (user_id, gateway_id, role) VALUES ($1, $2, 'owner')", targetID, gatewayID)
		if err != nil {
			return fmt.Errorf("insert target membership as owner: %w", err)
		}
	}

	// 8. Final verification: exactly one owner exists for gateway
	var finalOwnerCount int
	err = tx.QueryRow(ctx, "SELECT count(*) FROM public.user_gateways WHERE gateway_id = $1 AND role = 'owner'", gatewayID).Scan(&finalOwnerCount)
	if err != nil {
		return fmt.Errorf("verify final owner count: %w", err)
	}
	if finalOwnerCount != 1 {
		return ErrGatewayOwnerInconsistent
	}

	// 9. Commit
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit gateway owner transfer: %w", err)
	}
	return nil
}
