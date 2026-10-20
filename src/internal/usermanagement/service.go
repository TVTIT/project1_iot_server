package usermanagement

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/gateway"
)

// Service coordinates admin user management business logic.
type Service struct {
	authClient AuthAdminClient
	repo       Repository
	timeout    time.Duration
	listConfig UserListConfig
}

var (
	_ UserCreator            = (*Service)(nil)
	_ UserLister             = (*Service)(nil)
	_ UserUpdater            = (*Service)(nil)
	_ GatewayUserAssigner    = (*Service)(nil)
	_ GatewayUserRemover     = (*Service)(nil)
	_ GatewayOwnerTransferer = (*Service)(nil)
)

// NewService creates a new Service.
func NewService(authClient AuthAdminClient, repo Repository, timeout time.Duration) (*Service, error) {
	if authClient == nil {
		return nil, fmt.Errorf("auth client cannot be nil")
	}
	if repo == nil {
		return nil, fmt.Errorf("repository cannot be nil")
	}
	return &Service{
		authClient: authClient,
		repo:       repo,
		timeout:    timeout,
		listConfig: DefaultUserListConfig(),
	}, nil
}

// WithUserListConfig sets custom bounds for user list operations.
func (s *Service) WithUserListConfig(cfg UserListConfig) *Service {
	s.listConfig = cfg
	return s
}

// BuildUserSummaryList maps projection rows and gateway role rows into bounded UserSummary items.
// It verifies profile existence, sorts memberships by gateway_id ASC, avoids duplicating users from joins,
// and enforces response size bounds before writing HTTP responses.
func BuildUserSummaryList(userRows []UserProjectionRow, roleRows []UserGatewayRoleRow, maxResponseBytes int) ([]UserSummary, error) {
	for _, r := range userRows {
		if !r.ProfileExists {
			return nil, ErrInconsistentProfile
		}
	}

	if len(userRows) == 0 {
		return []UserSummary{}, nil
	}

	rolesByUser := make(map[uuid.UUID][]UserGatewayRole, len(userRows))
	for _, r := range roleRows {
		rolesByUser[r.UserID] = append(rolesByUser[r.UserID], UserGatewayRole{
			GatewayID: r.GatewayID,
			Role:      r.Role,
		})
	}
	for _, roles := range rolesByUser {
		sort.SliceStable(roles, func(i, j int) bool {
			return roles[i].GatewayID < roles[j].GatewayID
		})
	}

	summaries := make([]UserSummary, 0, len(userRows))
	for _, r := range userRows {
		gwRoles := rolesByUser[r.UserID]
		if gwRoles == nil {
			gwRoles = []UserGatewayRole{}
		}
		summaries = append(summaries, UserSummary{
			ID:           r.UserID,
			Email:        r.Email,
			FullName:     r.FullName,
			PlatformRole: r.PlatformRole,
			CreatedAt:    r.CreatedAt,
			GatewayRoles: gwRoles,
		})
	}

	if maxResponseBytes > 0 {
		payload := struct {
			Items []UserSummary `json:"items"`
		}{Items: summaries}
		data, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("serialize user list response: %w", err)
		}
		if len(data) > maxResponseBytes {
			return nil, ErrCapacityExceeded
		}
	}

	return summaries, nil
}

// ListUsers lists all users with their memberships.
func (s *Service) ListUsers(ctx context.Context, actorID uuid.UUID) ([]UserSummary, error) {
	// 1. Single deadline budget
	if s.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.timeout)
		defer cancel()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// 2. DB actor platform-admin authorization recheck
	if err := s.repo.VerifyActorIsAdmin(ctx, actorID); err != nil {
		return nil, err
	}

	// 3. Resolve bounds configuration
	cfg := s.listConfig
	if cfg.MaxUsers <= 0 {
		cfg.MaxUsers = 1000
	}
	if cfg.MaxMemberships <= 0 {
		cfg.MaxMemberships = 10000
	}
	if cfg.MaxResponseBytes <= 0 {
		cfg.MaxResponseBytes = 2 * 1024 * 1024
	}

	// 4. Query user projection reading up to bound + 1 to detect overflow
	userRows, err := s.repo.ListUsersProjection(ctx, actorID, cfg.MaxUsers+1)
	if err != nil {
		return nil, err
	}
	if len(userRows) > cfg.MaxUsers {
		return nil, ErrCapacityExceeded
	}

	if len(userRows) == 0 {
		return []UserSummary{}, nil
	}

	// 5. Collect user IDs in projection order
	userIDs := make([]uuid.UUID, len(userRows))
	for i, r := range userRows {
		userIDs[i] = r.UserID
	}

	// 6. Query gateway roles reading up to bound + 1
	roleRows, err := s.repo.ListUserGatewayRoles(ctx, userIDs, cfg.MaxMemberships+1)
	if err != nil {
		return nil, err
	}
	if len(roleRows) > cfg.MaxMemberships {
		return nil, ErrCapacityExceeded
	}

	// 7. Assemble user summaries and enforce serialization bound
	return BuildUserSummaryList(userRows, roleRows, cfg.MaxResponseBytes)
}

// UpdateUser updates full_name and/or platform_role for a user.
func (s *Service) UpdateUser(ctx context.Context, actorID, targetID uuid.UUID, input UpdateUserInput) (UpdatedUser, error) {
	// 1. Validate input before any database interaction
	if err := ValidateUpdateUserInput(input); err != nil {
		return UpdatedUser{}, err
	}

	// 2. Self-demotion check prevents any write when an admin attempts to revoke own platform admin
	if err := CheckSelfDemotion(actorID, targetID, input); err != nil {
		return UpdatedUser{}, err
	}

	// 3. Bound operations under one total deadline budget
	if s.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.timeout)
		defer cancel()
	}
	if err := ctx.Err(); err != nil {
		return UpdatedUser{}, err
	}

	// 4. Delegate to repository transaction which handles actor recheck, row locking, updates, and commit
	return s.repo.UpdateUserProfileAndRole(ctx, actorID, targetID, input.FullName, input.PlatformRole)
}

// AssignGatewayUser assigns an operator or viewer role on a gateway.
func (s *Service) AssignGatewayUser(ctx context.Context, actorID uuid.UUID, gatewayID string, targetID uuid.UUID, role string) error {
	// 1. Validate role
	if err := ValidateGatewayRole(role); err != nil {
		return err
	}

	// 2. Validate input IDs
	if actorID == uuid.Nil {
		return fmt.Errorf("%w: actor_id cannot be nil", ErrInvalidInput)
	}
	if targetID == uuid.Nil {
		return fmt.Errorf("%w: target_id cannot be nil", ErrInvalidInput)
	}
	if strings.TrimSpace(gatewayID) == "" {
		return fmt.Errorf("%w: gateway_id cannot be blank", ErrInvalidInput)
	}

	// 3. Bound operations under one total deadline budget
	if s.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.timeout)
		defer cancel()
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	// 4. Delegate to repository transaction
	return s.repo.AssignGatewayUser(ctx, actorID, gatewayID, targetID, role)
}

// RemoveGatewayUser revokes only membership, never the account or global role.
func (s *Service) RemoveGatewayUser(ctx context.Context, actorID uuid.UUID, gatewayID string, targetID uuid.UUID) error {
	if actorID == uuid.Nil || targetID == uuid.Nil || gateway.ValidateGatewayID(gatewayID) != nil || gatewayID == "backend_service" {
		return ErrInvalidInput
	}
	if s.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.timeout)
		defer cancel()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.repo.RemoveGatewayUser(ctx, actorID, gatewayID, targetID)
}

// TransferGatewayOwner transfers ownership of a gateway atomically.
func (s *Service) TransferGatewayOwner(ctx context.Context, actorID uuid.UUID, gatewayID string, targetID uuid.UUID) error {
	// 1. Validate input IDs
	if actorID == uuid.Nil {
		return fmt.Errorf("%w: actor_id cannot be nil", ErrInvalidInput)
	}
	if targetID == uuid.Nil {
		return fmt.Errorf("%w: target_id cannot be nil", ErrInvalidInput)
	}
	if strings.TrimSpace(gatewayID) == "" {
		return fmt.Errorf("%w: gateway_id cannot be blank", ErrInvalidInput)
	}

	// 2. Bound operations under one total deadline budget
	if s.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.timeout)
		defer cancel()
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	// 3. Delegate to repository transaction
	return s.repo.TransferGatewayOwner(ctx, actorID, gatewayID, targetID)
}

// CreateUser creates an account via Auth Admin and verifies profile creation.
func (s *Service) CreateUser(ctx context.Context, actorID uuid.UUID, input CreateUserInput) (CreatedUser, error) {
	// 1. Validate before external call
	if err := ValidateCreateUserInput(&input); err != nil {
		return CreatedUser{}, err
	}

	// 2. Bound operations under one total deadline budget
	if s.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.timeout)
		defer cancel()
	}
	if err := ctx.Err(); err != nil {
		return CreatedUser{}, err
	}

	// 3. DB actor platform-admin authorization before Auth side effects
	if err := s.repo.VerifyActorIsAdmin(ctx, actorID); err != nil {
		return CreatedUser{}, err
	}

	// 4. Upstream Auth Admin account creation
	authInput := AuthAdminCreateUserInput{
		Email:    input.Email,
		Password: input.Password,
		FullName: input.FullName,
	}
	authUser, err := s.authClient.CreateUser(ctx, authInput)
	if err != nil {
		return CreatedUser{}, err
	}

	// 5. Post-creation verification of exact profile UUID/name and absence of global role/membership
	if err := s.repo.VerifyCreatedUserProfile(ctx, authUser.ID, input.FullName); err != nil {
		return CreatedUser{}, &ProfileVerificationError{
			TargetID: authUser.ID,
			Cause:    err,
		}
	}

	// 6. Safe success DTO
	return CreatedUser{
		ID:           authUser.ID,
		Email:        authUser.Email,
		FullName:     input.FullName,
		PlatformRole: "user",
		CreatedAt:    authUser.CreatedAt,
	}, nil
}
