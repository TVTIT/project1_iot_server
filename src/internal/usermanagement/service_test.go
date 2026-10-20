package usermanagement_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/usermanagement"
)

type mockAuthAdminClient struct {
	calls     int
	lastCtx   context.Context
	lastInput usermanagement.AuthAdminCreateUserInput
	fn        func(ctx context.Context, input usermanagement.AuthAdminCreateUserInput) (usermanagement.AuthAdminUser, error)
}

func (m *mockAuthAdminClient) CreateUser(ctx context.Context, input usermanagement.AuthAdminCreateUserInput) (usermanagement.AuthAdminUser, error) {
	m.calls++
	m.lastCtx = ctx
	m.lastInput = input
	if m.fn != nil {
		return m.fn(ctx, input)
	}
	return usermanagement.AuthAdminUser{
		ID:        uuid.New(),
		Email:     input.Email,
		CreatedAt: time.Now().UTC(),
	}, nil
}

type mockRepository struct {
	verifyAdminCalls int
	lastAdminCtx     context.Context
	lastActorID      uuid.UUID
	verifyAdminFn    func(ctx context.Context, actorID uuid.UUID) error

	verifyProfileCalls int
	lastProfileCtx     context.Context
	lastTargetID       uuid.UUID
	lastExpectedName   string
	verifyProfileFn    func(ctx context.Context, targetID uuid.UUID, expectedFullName string) error

	listProjectionCalls int
	lastProjectionCtx   context.Context
	lastProjectionActor uuid.UUID
	lastProjectionLimit int
	listProjectionFn    func(ctx context.Context, actorID uuid.UUID, limit int) ([]usermanagement.UserProjectionRow, error)

	listGatewayRolesCalls  int
	lastGatewayRolesCtx    context.Context
	lastGatewayUserIDs     []uuid.UUID
	lastGatewayRolesLimit  int
	listGatewayRolesFn     func(ctx context.Context, userIDs []uuid.UUID, limit int) ([]usermanagement.UserGatewayRoleRow, error)
	updateCalls            int
	lastUpdateCtx          context.Context
	lastUpdateActorID      uuid.UUID
	lastUpdateTargetID     uuid.UUID
	lastUpdateFullName     *string
	lastUpdatePlatformRole *string
	updateFn               func(ctx context.Context, actorID, targetID uuid.UUID, fullName *string, platformRole *string) (usermanagement.UpdatedUser, error)

	assignCalls       int
	lastAssignCtx     context.Context
	lastAssignActorID uuid.UUID
	lastAssignGateway string
	lastAssignTarget  uuid.UUID
	lastAssignRole    string
	assignFn          func(ctx context.Context, actorID uuid.UUID, gatewayID string, targetID uuid.UUID, role string) error

	transferCalls       int
	lastTransferCtx     context.Context
	lastTransferActorID uuid.UUID
	lastTransferGateway string
	lastTransferTarget  uuid.UUID
	transferFn          func(ctx context.Context, actorID uuid.UUID, gatewayID string, targetID uuid.UUID) error
}

func (m *mockRepository) RemoveGatewayUser(context.Context, uuid.UUID, string, uuid.UUID) error {
	return nil
}

func (m *mockRepository) VerifyActorIsAdmin(ctx context.Context, actorID uuid.UUID) error {
	m.verifyAdminCalls++
	m.lastAdminCtx = ctx
	m.lastActorID = actorID
	if m.verifyAdminFn != nil {
		return m.verifyAdminFn(ctx, actorID)
	}
	return nil
}

func (m *mockRepository) VerifyCreatedUserProfile(ctx context.Context, targetID uuid.UUID, expectedFullName string) error {
	m.verifyProfileCalls++
	m.lastProfileCtx = ctx
	m.lastTargetID = targetID
	m.lastExpectedName = expectedFullName
	if m.verifyProfileFn != nil {
		return m.verifyProfileFn(ctx, targetID, expectedFullName)
	}
	return nil
}

func (m *mockRepository) ListUsersProjection(ctx context.Context, actorID uuid.UUID, limit int) ([]usermanagement.UserProjectionRow, error) {
	m.listProjectionCalls++
	m.lastProjectionCtx = ctx
	m.lastProjectionActor = actorID
	m.lastProjectionLimit = limit
	if m.listProjectionFn != nil {
		return m.listProjectionFn(ctx, actorID, limit)
	}
	return nil, nil
}

func (m *mockRepository) ListUserGatewayRoles(ctx context.Context, userIDs []uuid.UUID, limit int) ([]usermanagement.UserGatewayRoleRow, error) {
	m.listGatewayRolesCalls++
	m.lastGatewayRolesCtx = ctx
	m.lastGatewayUserIDs = userIDs
	m.lastGatewayRolesLimit = limit
	if m.listGatewayRolesFn != nil {
		return m.listGatewayRolesFn(ctx, userIDs, limit)
	}
	return nil, nil
}

func (m *mockRepository) UpdateUserProfileAndRole(ctx context.Context, actorID, targetID uuid.UUID, fullName *string, platformRole *string) (usermanagement.UpdatedUser, error) {
	m.updateCalls++
	m.lastUpdateCtx = ctx
	m.lastUpdateActorID = actorID
	m.lastUpdateTargetID = targetID
	m.lastUpdateFullName = fullName
	m.lastUpdatePlatformRole = platformRole
	if m.updateFn != nil {
		return m.updateFn(ctx, actorID, targetID, fullName, platformRole)
	}
	return usermanagement.UpdatedUser{}, nil
}

func (m *mockRepository) AssignGatewayUser(ctx context.Context, actorID uuid.UUID, gatewayID string, targetID uuid.UUID, role string) error {
	m.assignCalls++
	m.lastAssignCtx = ctx
	m.lastAssignActorID = actorID
	m.lastAssignGateway = gatewayID
	m.lastAssignTarget = targetID
	m.lastAssignRole = role
	if m.assignFn != nil {
		return m.assignFn(ctx, actorID, gatewayID, targetID, role)
	}
	return nil
}

func (m *mockRepository) TransferGatewayOwner(ctx context.Context, actorID uuid.UUID, gatewayID string, targetID uuid.UUID) error {
	m.transferCalls++
	m.lastTransferCtx = ctx
	m.lastTransferActorID = actorID
	m.lastTransferGateway = gatewayID
	m.lastTransferTarget = targetID
	if m.transferFn != nil {
		return m.transferFn(ctx, actorID, gatewayID, targetID)
	}
	return nil
}

func TestService_NewService_Validation(t *testing.T) {
	repo := &mockRepository{}
	authClient := &mockAuthAdminClient{}

	if _, err := usermanagement.NewService(nil, repo, 5*time.Second); err == nil {
		t.Error("expected error when authClient is nil")
	}
	if _, err := usermanagement.NewService(authClient, nil, 5*time.Second); err == nil {
		t.Error("expected error when repo is nil")
	}
	svc, err := usermanagement.NewService(authClient, repo, 5*time.Second)
	if err != nil || svc == nil {
		t.Fatalf("expected valid service, got err: %v", err)
	}
}

func TestService_CreateUser_ValidationBeforeExternalCall(t *testing.T) {
	authClient := &mockAuthAdminClient{}
	repo := &mockRepository{}
	svc, err := usermanagement.NewService(authClient, repo, 5*time.Second)
	if err != nil {
		t.Fatalf("NewService failed: %v", err)
	}

	actorID := uuid.New()
	invalidInputs := []struct {
		name  string
		input usermanagement.CreateUserInput
	}{
		{
			name: "blank full_name",
			input: usermanagement.CreateUserInput{
				FullName: "   ",
				Email:    "test@example.com",
				Password: "password123",
			},
		},
		{
			name: "invalid email",
			input: usermanagement.CreateUserInput{
				FullName: "Nguyen Van A",
				Email:    "not-an-email",
				Password: "password123",
			},
		},
		{
			name: "password too short",
			input: usermanagement.CreateUserInput{
				FullName: "Nguyen Van A",
				Email:    "test@example.com",
				Password: "123",
			},
		},
	}

	for _, tc := range invalidInputs {
		t.Run(tc.name, func(t *testing.T) {
			_, err := svc.CreateUser(context.Background(), actorID, tc.input)
			if !errors.Is(err, usermanagement.ErrInvalidInput) {
				t.Fatalf("expected ErrInvalidInput, got %v", err)
			}
			if repo.verifyAdminCalls != 0 {
				t.Errorf("repo.VerifyActorIsAdmin was called %d times, want 0", repo.verifyAdminCalls)
			}
			if authClient.calls != 0 {
				t.Errorf("authClient.CreateUser was called %d times, want 0", authClient.calls)
			}
		})
	}
}

func TestService_CreateUser_ActorNotAdmin(t *testing.T) {
	authClient := &mockAuthAdminClient{}
	repo := &mockRepository{
		verifyAdminFn: func(_ context.Context, _ uuid.UUID) error {
			return usermanagement.ErrActorNotAdmin
		},
	}
	svc, _ := usermanagement.NewService(authClient, repo, 5*time.Second)

	input := usermanagement.CreateUserInput{
		FullName: "Nguyen Van A",
		Email:    "test@example.com",
		Password: "password123",
	}

	_, err := svc.CreateUser(context.Background(), uuid.New(), input)
	if !errors.Is(err, usermanagement.ErrActorNotAdmin) {
		t.Fatalf("expected ErrActorNotAdmin, got %v", err)
	}
	if authClient.calls != 0 {
		t.Errorf("authClient.CreateUser called %d times, want 0 (no Auth side effects on denied actor)", authClient.calls)
	}
	if repo.verifyProfileCalls != 0 {
		t.Errorf("repo.VerifyCreatedUserProfile called %d times, want 0", repo.verifyProfileCalls)
	}
}

func TestService_CreateUser_ActorCheckDatabaseError(t *testing.T) {
	dbErr := errors.New("db connection lost")
	authClient := &mockAuthAdminClient{}
	repo := &mockRepository{
		verifyAdminFn: func(_ context.Context, _ uuid.UUID) error {
			return dbErr
		},
	}
	svc, _ := usermanagement.NewService(authClient, repo, 5*time.Second)

	input := usermanagement.CreateUserInput{
		FullName: "Nguyen Van A",
		Email:    "test@example.com",
		Password: "password123",
	}

	_, err := svc.CreateUser(context.Background(), uuid.New(), input)
	if !errors.Is(err, dbErr) {
		t.Fatalf("expected dbErr, got %v", err)
	}
	if authClient.calls != 0 {
		t.Errorf("authClient.CreateUser called %d times, want 0", authClient.calls)
	}
}

func TestService_CreateUser_OneTotalDeadline(t *testing.T) {
	authClient := &mockAuthAdminClient{}
	repo := &mockRepository{}
	svc, _ := usermanagement.NewService(authClient, repo, 100*time.Millisecond)

	input := usermanagement.CreateUserInput{
		FullName: "Nguyen Van A",
		Email:    "test@example.com",
		Password: "password123",
	}

	_, err := svc.CreateUser(context.Background(), uuid.New(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify all operations shared the exact same deadline
	adminDeadline, ok1 := repo.lastAdminCtx.Deadline()
	authDeadline, ok2 := authClient.lastCtx.Deadline()
	profileDeadline, ok3 := repo.lastProfileCtx.Deadline()

	if !ok1 || !ok2 || !ok3 {
		t.Fatalf("expected all contexts to have deadline, got ok1=%v, ok2=%v, ok3=%v", ok1, ok2, ok3)
	}
	if adminDeadline != authDeadline || authDeadline != profileDeadline {
		t.Errorf("deadlines differed: admin=%v, auth=%v, profile=%v", adminDeadline, authDeadline, profileDeadline)
	}

	// Verify timeout expiration before call returns DeadlineExceeded
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = svc.CreateUser(canceledCtx, uuid.New(), input)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestService_CreateUser_AuthClientFailure_NoSideEffects(t *testing.T) {
	testCases := []struct {
		name        string
		authErr     error
		expectedErr error
	}{
		{
			name:        "duplicate email",
			authErr:     usermanagement.ErrUserAlreadyExists,
			expectedErr: usermanagement.ErrUserAlreadyExists,
		},
		{
			name:        "weak password from upstream",
			authErr:     usermanagement.ErrInvalidInput,
			expectedErr: usermanagement.ErrInvalidInput,
		},
		{
			name:        "auth service unavailable",
			authErr:     usermanagement.ErrAuthUnavailable,
			expectedErr: usermanagement.ErrAuthUnavailable,
		},
		{
			name:        "timeout during auth call",
			authErr:     context.DeadlineExceeded,
			expectedErr: context.DeadlineExceeded,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			authClient := &mockAuthAdminClient{
				fn: func(_ context.Context, _ usermanagement.AuthAdminCreateUserInput) (usermanagement.AuthAdminUser, error) {
					return usermanagement.AuthAdminUser{}, tc.authErr
				},
			}
			repo := &mockRepository{}
			svc, _ := usermanagement.NewService(authClient, repo, 5*time.Second)

			input := usermanagement.CreateUserInput{
				FullName: "Nguyen Van A",
				Email:    "test@example.com",
				Password: "password123",
			}

			_, err := svc.CreateUser(context.Background(), uuid.New(), input)
			if !errors.Is(err, tc.expectedErr) {
				t.Fatalf("expected %v, got %v", tc.expectedErr, err)
			}
			if authClient.calls != 1 {
				t.Errorf("authClient.CreateUser calls = %d, want exactly 1 (no auto-retry on failure)", authClient.calls)
			}
			if repo.verifyProfileCalls != 0 {
				t.Errorf("repo.VerifyCreatedUserProfile called %d times, want 0", repo.verifyProfileCalls)
			}
		})
	}
}

func TestService_CreateUser_ProfileVerificationFailure_MissingProfile(t *testing.T) {
	createdID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	authClient := &mockAuthAdminClient{
		fn: func(_ context.Context, input usermanagement.AuthAdminCreateUserInput) (usermanagement.AuthAdminUser, error) {
			return usermanagement.AuthAdminUser{
				ID:        createdID,
				Email:     input.Email,
				CreatedAt: time.Now().UTC(),
			}, nil
		},
	}
	repo := &mockRepository{
		verifyProfileFn: func(_ context.Context, _ uuid.UUID, _ string) error {
			return usermanagement.ErrInconsistentProfile
		},
	}
	svc, _ := usermanagement.NewService(authClient, repo, 5*time.Second)

	input := usermanagement.CreateUserInput{
		FullName: "Nguyen Van A",
		Email:    "test@example.com",
		Password: "super-secret-password-xyz",
	}

	_, err := svc.CreateUser(context.Background(), uuid.New(), input)
	if err == nil {
		t.Fatal("expected error on missing profile, got nil")
	}

	// Must match both ErrProfileVerificationFailed and ErrInconsistentProfile
	if !errors.Is(err, usermanagement.ErrProfileVerificationFailed) {
		t.Errorf("expected error to match ErrProfileVerificationFailed, got %v", err)
	}
	if !errors.Is(err, usermanagement.ErrInconsistentProfile) {
		t.Errorf("expected error to match ErrInconsistentProfile, got %v", err)
	}

	// Must preserve safe created target UUID
	var pve *usermanagement.ProfileVerificationError
	if !errors.As(err, &pve) {
		t.Fatalf("expected *ProfileVerificationError, got %T", err)
	}
	if pve.TargetID != createdID {
		t.Errorf("TargetID = %s, want %s", pve.TargetID, createdID)
	}

	// Secret safety: password must NOT appear in error string
	errStr := err.Error()
	if strings.Contains(errStr, "super-secret-password-xyz") {
		t.Errorf("password leaked in error message: %s", errStr)
	}

	// Invariant: no retry
	if authClient.calls != 1 {
		t.Errorf("authClient.CreateUser calls = %d, want 1", authClient.calls)
	}
}

func TestService_CreateUser_ProfileVerificationFailure_NameMismatch(t *testing.T) {
	createdID := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	authClient := &mockAuthAdminClient{
		fn: func(_ context.Context, input usermanagement.AuthAdminCreateUserInput) (usermanagement.AuthAdminUser, error) {
			return usermanagement.AuthAdminUser{
				ID:        createdID,
				Email:     input.Email,
				CreatedAt: time.Now().UTC(),
			}, nil
		},
	}
	repo := &mockRepository{
		verifyProfileFn: func(_ context.Context, _ uuid.UUID, _ string) error {
			return errors.New("name mismatch in profile")
		},
	}
	svc, _ := usermanagement.NewService(authClient, repo, 5*time.Second)

	input := usermanagement.CreateUserInput{
		FullName: "Nguyen Van A",
		Email:    "test@example.com",
		Password: "password123",
	}

	_, err := svc.CreateUser(context.Background(), uuid.New(), input)
	if err == nil {
		t.Fatal("expected error on name mismatch, got nil")
	}

	var pve *usermanagement.ProfileVerificationError
	if !errors.As(err, &pve) {
		t.Fatalf("expected *ProfileVerificationError, got %T", err)
	}
	if pve.TargetID != createdID {
		t.Errorf("TargetID = %s, want %s", pve.TargetID, createdID)
	}
}

func TestService_CreateUser_ProfileVerificationFailure_UnexpectedAdminOrMembership(t *testing.T) {
	createdID := uuid.MustParse("33333333-3333-3333-3333-333333333333")
	authClient := &mockAuthAdminClient{
		fn: func(_ context.Context, input usermanagement.AuthAdminCreateUserInput) (usermanagement.AuthAdminUser, error) {
			return usermanagement.AuthAdminUser{
				ID:        createdID,
				Email:     input.Email,
				CreatedAt: time.Now().UTC(),
			}, nil
		},
	}
	repo := &mockRepository{
		verifyProfileFn: func(_ context.Context, _ uuid.UUID, _ string) error {
			return errors.New("new user unexpectedly has platform_admin role")
		},
	}
	svc, _ := usermanagement.NewService(authClient, repo, 5*time.Second)

	input := usermanagement.CreateUserInput{
		FullName: "Nguyen Van A",
		Email:    "test@example.com",
		Password: "password123",
	}

	_, err := svc.CreateUser(context.Background(), uuid.New(), input)
	if err == nil {
		t.Fatal("expected error on unexpected admin role, got nil")
	}

	var pve *usermanagement.ProfileVerificationError
	if !errors.As(err, &pve) {
		t.Fatalf("expected *ProfileVerificationError, got %T", err)
	}
	if pve.TargetID != createdID {
		t.Errorf("TargetID = %s, want %s", pve.TargetID, createdID)
	}
}

func TestService_CreateUser_Success(t *testing.T) {
	createdID := uuid.MustParse("44444444-4444-4444-4444-444444444444")
	createdAt := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	authClient := &mockAuthAdminClient{
		fn: func(_ context.Context, input usermanagement.AuthAdminCreateUserInput) (usermanagement.AuthAdminUser, error) {
			if input.Email != "a@example.com" || input.Password != "password123" || input.FullName != "Nguyen Van A" {
				t.Fatalf("unexpected auth input: %+v", input)
			}
			return usermanagement.AuthAdminUser{
				ID:        createdID,
				Email:     input.Email,
				CreatedAt: createdAt,
			}, nil
		},
	}
	repo := &mockRepository{
		verifyAdminFn: func(_ context.Context, _ uuid.UUID) error {
			return nil
		},
		verifyProfileFn: func(_ context.Context, targetID uuid.UUID, expectedFullName string) error {
			if targetID != createdID || expectedFullName != "Nguyen Van A" {
				t.Fatalf("unexpected verify profile args: id=%s, name=%s", targetID, expectedFullName)
			}
			return nil
		},
	}
	svc, _ := usermanagement.NewService(authClient, repo, 5*time.Second)

	actorID := uuid.New()
	input := usermanagement.CreateUserInput{
		FullName: "Nguyen Van A",
		Email:    "  a@example.com  ", // Test whitespace normalization
		Password: "password123",
	}

	result, err := svc.CreateUser(context.Background(), actorID, input)
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}

	if result.ID != createdID {
		t.Errorf("ID = %s, want %s", result.ID, createdID)
	}
	if result.Email != "a@example.com" {
		t.Errorf("Email = %s, want a@example.com", result.Email)
	}
	if result.FullName != "Nguyen Van A" {
		t.Errorf("FullName = %s, want Nguyen Van A", result.FullName)
	}
	if result.PlatformRole != "user" {
		t.Errorf("PlatformRole = %s, want 'user'", result.PlatformRole)
	}
	if !result.CreatedAt.Equal(createdAt) {
		t.Errorf("CreatedAt = %v, want %v", result.CreatedAt, createdAt)
	}
	if repo.verifyAdminCalls != 1 {
		t.Errorf("repo.VerifyActorIsAdmin calls = %d, want 1", repo.verifyAdminCalls)
	}
	if authClient.calls != 1 {
		t.Errorf("authClient.CreateUser calls = %d, want 1", authClient.calls)
	}
	if repo.verifyProfileCalls != 1 {
		t.Errorf("repo.VerifyCreatedUserProfile calls = %d, want 1", repo.verifyProfileCalls)
	}
}

func TestService_ListUsers_ActorNotAdmin(t *testing.T) {
	repo := &mockRepository{
		verifyAdminFn: func(_ context.Context, _ uuid.UUID) error {
			return usermanagement.ErrActorNotAdmin
		},
	}
	svc, _ := usermanagement.NewService(&mockAuthAdminClient{}, repo, 5*time.Second)

	_, err := svc.ListUsers(context.Background(), uuid.New())
	if !errors.Is(err, usermanagement.ErrActorNotAdmin) {
		t.Fatalf("expected ErrActorNotAdmin, got %v", err)
	}
	if repo.listProjectionCalls != 0 {
		t.Errorf("repo.ListUsersProjection calls = %d, want 0", repo.listProjectionCalls)
	}
	if repo.listGatewayRolesCalls != 0 {
		t.Errorf("repo.ListUserGatewayRoles calls = %d, want 0", repo.listGatewayRolesCalls)
	}
}

func TestService_ListUsers_OneTotalDeadline(t *testing.T) {
	repo := &mockRepository{
		verifyAdminFn: func(_ context.Context, _ uuid.UUID) error {
			return nil
		},
		listProjectionFn: func(_ context.Context, _ uuid.UUID, _ int) ([]usermanagement.UserProjectionRow, error) {
			return []usermanagement.UserProjectionRow{
				{
					UserID:        uuid.New(),
					Email:         "u1@example.com",
					ProfileExists: true,
				},
			}, nil
		},
		listGatewayRolesFn: func(_ context.Context, _ []uuid.UUID, _ int) ([]usermanagement.UserGatewayRoleRow, error) {
			return []usermanagement.UserGatewayRoleRow{}, nil
		},
	}
	svc, _ := usermanagement.NewService(&mockAuthAdminClient{}, repo, 100*time.Millisecond)

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := svc.ListUsers(canceledCtx, uuid.New())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	// Verify all operations shared the exact same deadline
	_, err = svc.ListUsers(context.Background(), uuid.New())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	adminDeadline, ok1 := repo.lastAdminCtx.Deadline()
	projDeadline, ok2 := repo.lastProjectionCtx.Deadline()
	rolesDeadline, ok3 := repo.lastGatewayRolesCtx.Deadline()
	if !ok1 || !ok2 || !ok3 {
		t.Fatalf("expected all contexts to have deadline, got ok1=%v, ok2=%v, ok3=%v", ok1, ok2, ok3)
	}
	if adminDeadline != projDeadline || projDeadline != rolesDeadline {
		t.Errorf("deadlines differed: admin=%v, proj=%v, roles=%v", adminDeadline, projDeadline, rolesDeadline)
	}
}

func TestService_ListUsers_EmptyList(t *testing.T) {
	repo := &mockRepository{
		listProjectionFn: func(_ context.Context, _ uuid.UUID, _ int) ([]usermanagement.UserProjectionRow, error) {
			return []usermanagement.UserProjectionRow{}, nil
		},
	}
	svc, _ := usermanagement.NewService(&mockAuthAdminClient{}, repo, 5*time.Second)

	items, err := svc.ListUsers(context.Background(), uuid.New())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if items == nil {
		t.Fatal("expected non-nil empty slice, got nil")
	}
	if len(items) != 0 {
		t.Fatalf("expected 0 items, got %d", len(items))
	}
	if repo.listGatewayRolesCalls != 0 {
		t.Errorf("repo.ListUserGatewayRoles calls = %d, want 0", repo.listGatewayRolesCalls)
	}
}

func TestService_ListUsers_AllUsersAndStableOrder(t *testing.T) {
	u1 := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	u2 := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	u3 := uuid.MustParse("33333333-3333-3333-3333-333333333333")
	t1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	t3 := time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC)

	repo := &mockRepository{
		listProjectionFn: func(_ context.Context, _ uuid.UUID, _ int) ([]usermanagement.UserProjectionRow, error) {
			return []usermanagement.UserProjectionRow{
				{UserID: u1, Email: "u1@example.com", CreatedAt: t1, ProfileExists: true, PlatformRole: "user"},
				{UserID: u2, Email: "u2@example.com", CreatedAt: t2, ProfileExists: true, PlatformRole: "platform_admin"},
				{UserID: u3, Email: "u3@example.com", CreatedAt: t3, ProfileExists: true, PlatformRole: "user"},
			}, nil
		},
		listGatewayRolesFn: func(_ context.Context, _ []uuid.UUID, _ int) ([]usermanagement.UserGatewayRoleRow, error) {
			return []usermanagement.UserGatewayRoleRow{}, nil
		},
	}
	svc, _ := usermanagement.NewService(&mockAuthAdminClient{}, repo, 5*time.Second)

	items, err := svc.ListUsers(context.Background(), uuid.New())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(items) != 3 {
		t.Fatalf("len(items) = %d, want 3", len(items))
	}
	if items[0].ID != u1 || items[1].ID != u2 || items[2].ID != u3 {
		t.Errorf("unexpected order: [%s, %s, %s], want [%s, %s, %s]",
			items[0].ID, items[1].ID, items[2].ID, u1, u2, u3)
	}
}

func TestService_ListUsers_SortedMembershipsAndNoDuplicates(t *testing.T) {
	u1 := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	u2 := uuid.MustParse("22222222-2222-2222-2222-222222222222")

	repo := &mockRepository{
		listProjectionFn: func(_ context.Context, _ uuid.UUID, _ int) ([]usermanagement.UserProjectionRow, error) {
			return []usermanagement.UserProjectionRow{
				{UserID: u1, Email: "u1@example.com", ProfileExists: true, PlatformRole: "user"},
				{UserID: u2, Email: "u2@example.com", ProfileExists: true, PlatformRole: "user"},
			}, nil
		},
		listGatewayRolesFn: func(_ context.Context, _ []uuid.UUID, _ int) ([]usermanagement.UserGatewayRoleRow, error) {
			// Deliberately return unsorted gateway roles for user 1 to verify service enforces sorting
			return []usermanagement.UserGatewayRoleRow{
				{UserID: u1, GatewayID: "gw-zebra", Role: "viewer"},
				{UserID: u1, GatewayID: "gw-alpha", Role: "operator"},
			}, nil
		},
	}
	svc, _ := usermanagement.NewService(&mockAuthAdminClient{}, repo, 5*time.Second)

	items, err := svc.ListUsers(context.Background(), uuid.New())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("len(items) = %d, want 2 (user must not be duplicated from multiple memberships)", len(items))
	}

	// Verify User 1 memberships are sorted by gateway_id ASC
	if len(items[0].GatewayRoles) != 2 {
		t.Fatalf("len(items[0].GatewayRoles) = %d, want 2", len(items[0].GatewayRoles))
	}
	if items[0].GatewayRoles[0].GatewayID != "gw-alpha" || items[0].GatewayRoles[0].Role != "operator" {
		t.Errorf("first role = %+v, want gw-alpha / operator", items[0].GatewayRoles[0])
	}
	if items[0].GatewayRoles[1].GatewayID != "gw-zebra" || items[0].GatewayRoles[1].Role != "viewer" {
		t.Errorf("second role = %+v, want gw-zebra / viewer", items[0].GatewayRoles[1])
	}

	// Verify User 2 has empty slice (not nil)
	if items[1].GatewayRoles == nil || len(items[1].GatewayRoles) != 0 {
		t.Errorf("items[1].GatewayRoles = %+v, want empty non-nil slice", items[1].GatewayRoles)
	}

	// Invariant: single batched read, no N+1
	if repo.listGatewayRolesCalls != 1 {
		t.Errorf("listGatewayRolesCalls = %d, want 1 (batched query)", repo.listGatewayRolesCalls)
	}
	if len(repo.lastGatewayUserIDs) != 2 {
		t.Errorf("len(lastGatewayUserIDs) = %d, want 2", len(repo.lastGatewayUserIDs))
	}
}

func TestService_ListUsers_CanonicalFullNameAndLegacyNull(t *testing.T) {
	u1 := uuid.New()
	u2 := uuid.New()
	alice := "Alice Walker"

	repo := &mockRepository{
		listProjectionFn: func(_ context.Context, _ uuid.UUID, _ int) ([]usermanagement.UserProjectionRow, error) {
			return []usermanagement.UserProjectionRow{
				{UserID: u1, Email: "alice@example.com", FullName: &alice, ProfileExists: true, PlatformRole: "user"},
				{UserID: u2, Email: "legacy@example.com", FullName: nil, ProfileExists: true, PlatformRole: "user"},
			}, nil
		},
		listGatewayRolesFn: func(_ context.Context, _ []uuid.UUID, _ int) ([]usermanagement.UserGatewayRoleRow, error) {
			return []usermanagement.UserGatewayRoleRow{}, nil
		},
	}
	svc, _ := usermanagement.NewService(&mockAuthAdminClient{}, repo, 5*time.Second)

	items, err := svc.ListUsers(context.Background(), uuid.New())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if items[0].FullName == nil || *items[0].FullName != "Alice Walker" {
		t.Errorf("items[0].FullName = %v, want 'Alice Walker'", items[0].FullName)
	}
	if items[1].FullName != nil {
		t.Errorf("items[1].FullName = %v, want nil", items[1].FullName)
	}

	// Verify JSON serialization outputs "full_name": null for legacy profiles
	data, err := json.Marshal(items[1])
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}
	if !strings.Contains(string(data), `"full_name":null`) {
		t.Errorf("expected 'full_name':null in JSON, got: %s", string(data))
	}
}

func TestService_ListUsers_PlatformRoleFromDBOnly_NoSecrets(t *testing.T) {
	u1 := uuid.New()
	u2 := uuid.New()

	repo := &mockRepository{
		listProjectionFn: func(_ context.Context, _ uuid.UUID, _ int) ([]usermanagement.UserProjectionRow, error) {
			return []usermanagement.UserProjectionRow{
				{UserID: u1, Email: "admin@example.com", PlatformRole: "platform_admin", ProfileExists: true},
				{UserID: u2, Email: "user@example.com", PlatformRole: "user", ProfileExists: true},
			}, nil
		},
		listGatewayRolesFn: func(_ context.Context, _ []uuid.UUID, _ int) ([]usermanagement.UserGatewayRoleRow, error) {
			return []usermanagement.UserGatewayRoleRow{}, nil
		},
	}
	svc, _ := usermanagement.NewService(&mockAuthAdminClient{}, repo, 5*time.Second)

	items, err := svc.ListUsers(context.Background(), uuid.New())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if items[0].PlatformRole != "platform_admin" {
		t.Errorf("items[0].PlatformRole = %q, want platform_admin", items[0].PlatformRole)
	}
	if items[1].PlatformRole != "user" {
		t.Errorf("items[1].PlatformRole = %q, want user", items[1].PlatformRole)
	}

	// Verify no secret or internal metadata fields are serialized in JSON
	for _, item := range items {
		data, err := json.Marshal(item)
		if err != nil {
			t.Fatalf("marshal error: %v", err)
		}
		var rawMap map[string]any
		if err := json.Unmarshal(data, &rawMap); err != nil {
			t.Fatalf("unmarshal error: %v", err)
		}
		forbiddenKeys := []string{
			"password", "hash", "encrypted_password", "raw_user_meta_data",
			"app_metadata", "user_metadata", "token", "secret", "confirmation_token",
		}
		for _, key := range forbiddenKeys {
			if _, exists := rawMap[key]; exists {
				t.Errorf("forbidden secret key %q leaked in UserSummary JSON", key)
			}
		}
	}
}

func TestService_ListUsers_MissingProfileInconsistency(t *testing.T) {
	t.Run("row with ProfileExists false returns ErrInconsistentProfile", func(t *testing.T) {
		repo := &mockRepository{
			listProjectionFn: func(_ context.Context, _ uuid.UUID, _ int) ([]usermanagement.UserProjectionRow, error) {
				return []usermanagement.UserProjectionRow{
					{UserID: uuid.New(), Email: "orphan@example.com", ProfileExists: false},
				}, nil
			},
		}
		svc, _ := usermanagement.NewService(&mockAuthAdminClient{}, repo, 5*time.Second)

		items, err := svc.ListUsers(context.Background(), uuid.New())
		if !errors.Is(err, usermanagement.ErrInconsistentProfile) {
			t.Fatalf("expected ErrInconsistentProfile, got %v", err)
		}
		if items != nil {
			t.Fatalf("expected nil items on inconsistency error, got %+v", items)
		}
	})

	t.Run("repo returning ErrInconsistentProfile is propagated without partial list", func(t *testing.T) {
		repo := &mockRepository{
			listProjectionFn: func(_ context.Context, _ uuid.UUID, _ int) ([]usermanagement.UserProjectionRow, error) {
				return nil, usermanagement.ErrInconsistentProfile
			},
		}
		svc, _ := usermanagement.NewService(&mockAuthAdminClient{}, repo, 5*time.Second)

		items, err := svc.ListUsers(context.Background(), uuid.New())
		if !errors.Is(err, usermanagement.ErrInconsistentProfile) {
			t.Fatalf("expected ErrInconsistentProfile, got %v", err)
		}
		if items != nil {
			t.Fatalf("expected nil items, got %+v", items)
		}
	})
}

func TestService_ListUsers_CapacityExceeded_UserBoundPlusOne(t *testing.T) {
	repo := &mockRepository{
		listProjectionFn: func(_ context.Context, _ uuid.UUID, limit int) ([]usermanagement.UserProjectionRow, error) {
			// If MaxUsers is 2, limit requested must be 3
			if limit != 3 {
				t.Errorf("requested limit = %d, want 3 (MaxUsers + 1)", limit)
			}
			return []usermanagement.UserProjectionRow{
				{UserID: uuid.New(), ProfileExists: true},
				{UserID: uuid.New(), ProfileExists: true},
				{UserID: uuid.New(), ProfileExists: true},
			}, nil
		},
	}
	svc, _ := usermanagement.NewService(&mockAuthAdminClient{}, repo, 5*time.Second)
	svc.WithUserListConfig(usermanagement.UserListConfig{
		MaxUsers:         2,
		MaxMemberships:   100,
		MaxResponseBytes: 10000,
	})

	items, err := svc.ListUsers(context.Background(), uuid.New())
	if !errors.Is(err, usermanagement.ErrCapacityExceeded) {
		t.Fatalf("expected ErrCapacityExceeded, got %v", err)
	}
	if !errors.Is(err, usermanagement.ErrUserListCapacityExceeded) {
		t.Fatalf("expected ErrUserListCapacityExceeded, got %v", err)
	}
	if items != nil {
		t.Fatalf("expected nil items on capacity exceeded, got %+v (no partial list permitted)", items)
	}
	if repo.listGatewayRolesCalls != 0 {
		t.Errorf("repo.ListUserGatewayRoles calls = %d, want 0 when users overflow", repo.listGatewayRolesCalls)
	}
}

func TestService_ListUsers_CapacityExceeded_MembershipBoundPlusOne(t *testing.T) {
	u1 := uuid.New()
	repo := &mockRepository{
		listProjectionFn: func(_ context.Context, _ uuid.UUID, _ int) ([]usermanagement.UserProjectionRow, error) {
			return []usermanagement.UserProjectionRow{
				{UserID: u1, ProfileExists: true},
			}, nil
		},
		listGatewayRolesFn: func(_ context.Context, _ []uuid.UUID, limit int) ([]usermanagement.UserGatewayRoleRow, error) {
			// If MaxMemberships is 2, limit requested must be 3
			if limit != 3 {
				t.Errorf("requested limit = %d, want 3 (MaxMemberships + 1)", limit)
			}
			return []usermanagement.UserGatewayRoleRow{
				{UserID: u1, GatewayID: "gw1", Role: "viewer"},
				{UserID: u1, GatewayID: "gw2", Role: "operator"},
				{UserID: u1, GatewayID: "gw3", Role: "owner"},
			}, nil
		},
	}
	svc, _ := usermanagement.NewService(&mockAuthAdminClient{}, repo, 5*time.Second)
	svc.WithUserListConfig(usermanagement.UserListConfig{
		MaxUsers:         10,
		MaxMemberships:   2,
		MaxResponseBytes: 10000,
	})

	items, err := svc.ListUsers(context.Background(), uuid.New())
	if !errors.Is(err, usermanagement.ErrCapacityExceeded) {
		t.Fatalf("expected ErrCapacityExceeded, got %v", err)
	}
	if !errors.Is(err, usermanagement.ErrUserListCapacityExceeded) {
		t.Fatalf("expected ErrUserListCapacityExceeded, got %v", err)
	}
	if items != nil {
		t.Fatalf("expected nil items on capacity exceeded, got %+v (no partial list permitted)", items)
	}
}

func TestService_ListUsers_CapacityExceeded_EncodedResponseByteOverflow(t *testing.T) {
	u1 := uuid.New()
	name := "Alice Very Long Name To Trigger Byte Overflow"
	repo := &mockRepository{
		listProjectionFn: func(_ context.Context, _ uuid.UUID, _ int) ([]usermanagement.UserProjectionRow, error) {
			return []usermanagement.UserProjectionRow{
				{UserID: u1, Email: "alice@example.com", FullName: &name, ProfileExists: true, PlatformRole: "user"},
			}, nil
		},
		listGatewayRolesFn: func(_ context.Context, _ []uuid.UUID, _ int) ([]usermanagement.UserGatewayRoleRow, error) {
			return []usermanagement.UserGatewayRoleRow{}, nil
		},
	}
	svc, _ := usermanagement.NewService(&mockAuthAdminClient{}, repo, 5*time.Second)
	svc.WithUserListConfig(usermanagement.UserListConfig{
		MaxUsers:         10,
		MaxMemberships:   10,
		MaxResponseBytes: 50, // smaller than json payload {"items":[{...}]}
	})

	items, err := svc.ListUsers(context.Background(), uuid.New())
	if !errors.Is(err, usermanagement.ErrCapacityExceeded) {
		t.Fatalf("expected ErrCapacityExceeded, got %v", err)
	}
	if !errors.Is(err, usermanagement.ErrUserListCapacityExceeded) {
		t.Fatalf("expected ErrUserListCapacityExceeded, got %v", err)
	}
	if items != nil {
		t.Fatalf("expected nil items on byte overflow, got %+v (no partial list permitted)", items)
	}
}

// ============================================================================
// Task U4.2 & U4.3 (PATCH User Profile and Global Role Service Tests)
// ============================================================================

func TestService_UpdateUser_NameOnly(t *testing.T) {
	actorID := uuid.New()
	targetID := uuid.New()
	newName := "Nguyễn Văn B"

	repo := &mockRepository{
		updateFn: func(_ context.Context, actor, target uuid.UUID, fullName *string, role *string) (usermanagement.UpdatedUser, error) {
			if actor != actorID || target != targetID {
				t.Fatalf("unexpected actor=%v or target=%v", actor, target)
			}
			if fullName == nil || *fullName != newName {
				t.Fatalf("unexpected fullName: %v", fullName)
			}
			if role != nil {
				t.Fatalf("expected nil role, got: %v", *role)
			}
			return usermanagement.UpdatedUser{
				ID:           targetID,
				FullName:     fullName,
				PlatformRole: "user",
			}, nil
		},
	}
	svc, _ := usermanagement.NewService(&mockAuthAdminClient{}, repo, 5*time.Second)

	res, err := svc.UpdateUser(context.Background(), actorID, targetID, usermanagement.UpdateUserInput{
		FullName: &newName,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.ID != targetID || res.FullName == nil || *res.FullName != newName || res.PlatformRole != "user" {
		t.Fatalf("unexpected UpdatedUser result: %+v", res)
	}
	if repo.updateCalls != 1 {
		t.Errorf("repo.UpdateUserProfileAndRole calls = %d, want 1", repo.updateCalls)
	}
}

func TestService_UpdateUser_RoleOnly(t *testing.T) {
	actorID := uuid.New()
	targetID := uuid.New()
	roleAdmin := "platform_admin"
	currentName := "Nguyen Van C"

	repo := &mockRepository{
		updateFn: func(_ context.Context, actor, target uuid.UUID, fullName *string, role *string) (usermanagement.UpdatedUser, error) {
			if actor != actorID || target != targetID {
				t.Fatalf("unexpected actor=%v or target=%v", actor, target)
			}
			if fullName != nil {
				t.Fatalf("expected nil fullName, got: %v", *fullName)
			}
			if role == nil || *role != roleAdmin {
				t.Fatalf("unexpected role: %v", role)
			}
			return usermanagement.UpdatedUser{
				ID:           targetID,
				FullName:     &currentName,
				PlatformRole: *role,
			}, nil
		},
	}
	svc, _ := usermanagement.NewService(&mockAuthAdminClient{}, repo, 5*time.Second)

	res, err := svc.UpdateUser(context.Background(), actorID, targetID, usermanagement.UpdateUserInput{
		PlatformRole: &roleAdmin,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.ID != targetID || res.FullName == nil || *res.FullName != currentName || res.PlatformRole != roleAdmin {
		t.Fatalf("unexpected UpdatedUser result: %+v", res)
	}
	if repo.updateCalls != 1 {
		t.Errorf("repo.UpdateUserProfileAndRole calls = %d, want 1", repo.updateCalls)
	}
}

func TestService_UpdateUser_Combined(t *testing.T) {
	actorID := uuid.New()
	targetID := uuid.New()
	newName := "Trần Thị D"
	roleAdmin := "platform_admin"

	repo := &mockRepository{
		updateFn: func(_ context.Context, actor, target uuid.UUID, fullName *string, role *string) (usermanagement.UpdatedUser, error) {
			if actor != actorID || target != targetID {
				t.Fatalf("unexpected actor=%v or target=%v", actor, target)
			}
			if fullName == nil || *fullName != newName {
				t.Fatalf("unexpected fullName: %v", fullName)
			}
			if role == nil || *role != roleAdmin {
				t.Fatalf("unexpected role: %v", role)
			}
			return usermanagement.UpdatedUser{
				ID:           targetID,
				FullName:     fullName,
				PlatformRole: *role,
			}, nil
		},
	}
	svc, _ := usermanagement.NewService(&mockAuthAdminClient{}, repo, 5*time.Second)

	res, err := svc.UpdateUser(context.Background(), actorID, targetID, usermanagement.UpdateUserInput{
		FullName:     &newName,
		PlatformRole: &roleAdmin,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.ID != targetID || res.FullName == nil || *res.FullName != newName || res.PlatformRole != roleAdmin {
		t.Fatalf("unexpected UpdatedUser result: %+v", res)
	}
	if repo.updateCalls != 1 {
		t.Errorf("repo.UpdateUserProfileAndRole calls = %d, want 1", repo.updateCalls)
	}
}

func TestService_UpdateUser_TargetMissing(t *testing.T) {
	actorID := uuid.New()
	missingID := uuid.New()
	newName := "Anyone"

	repo := &mockRepository{
		updateFn: func(_ context.Context, _, _ uuid.UUID, _ *string, _ *string) (usermanagement.UpdatedUser, error) {
			return usermanagement.UpdatedUser{}, usermanagement.ErrUserNotFound
		},
	}
	svc, _ := usermanagement.NewService(&mockAuthAdminClient{}, repo, 5*time.Second)

	_, err := svc.UpdateUser(context.Background(), actorID, missingID, usermanagement.UpdateUserInput{
		FullName: &newName,
	})
	if !errors.Is(err, usermanagement.ErrUserNotFound) {
		t.Fatalf("expected ErrUserNotFound, got %v", err)
	}
}

func TestService_UpdateUser_NoOpGrant(t *testing.T) {
	actorID := uuid.New()
	targetID := uuid.New()
	roleAdmin := "platform_admin"
	currentName := "Already Admin"

	repo := &mockRepository{
		updateFn: func(_ context.Context, _, _ uuid.UUID, _ *string, role *string) (usermanagement.UpdatedUser, error) {
			return usermanagement.UpdatedUser{
				ID:           targetID,
				FullName:     &currentName,
				PlatformRole: *role,
			}, nil
		},
	}
	svc, _ := usermanagement.NewService(&mockAuthAdminClient{}, repo, 5*time.Second)

	res, err := svc.UpdateUser(context.Background(), actorID, targetID, usermanagement.UpdateUserInput{
		PlatformRole: &roleAdmin,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.PlatformRole != "platform_admin" {
		t.Fatalf("expected platform_admin, got %s", res.PlatformRole)
	}
	if repo.updateCalls != 1 {
		t.Errorf("repo calls = %d, want 1", repo.updateCalls)
	}
}

func TestService_UpdateUser_RoleRevoke(t *testing.T) {
	actorID := uuid.New()
	targetID := uuid.New()
	roleUser := "user"
	currentName := "Demoted User"

	repo := &mockRepository{
		updateFn: func(_ context.Context, _, _ uuid.UUID, _ *string, role *string) (usermanagement.UpdatedUser, error) {
			return usermanagement.UpdatedUser{
				ID:           targetID,
				FullName:     &currentName,
				PlatformRole: *role,
			}, nil
		},
	}
	svc, _ := usermanagement.NewService(&mockAuthAdminClient{}, repo, 5*time.Second)

	res, err := svc.UpdateUser(context.Background(), actorID, targetID, usermanagement.UpdateUserInput{
		PlatformRole: &roleUser,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.PlatformRole != "user" {
		t.Fatalf("expected user, got %s", res.PlatformRole)
	}
	if repo.updateCalls != 1 {
		t.Errorf("repo calls = %d, want 1", repo.updateCalls)
	}
}

func TestService_UpdateUser_SelfNameUpdatePermitted(t *testing.T) {
	actorID := uuid.New() // same as target
	newName := "Admin Self Rename"

	repo := &mockRepository{
		updateFn: func(_ context.Context, actor, target uuid.UUID, fullName *string, _ *string) (usermanagement.UpdatedUser, error) {
			if actor != target || actor != actorID {
				t.Fatalf("expected actor == target == actorID, got actor=%v, target=%v", actor, target)
			}
			return usermanagement.UpdatedUser{
				ID:           actorID,
				FullName:     fullName,
				PlatformRole: "platform_admin",
			}, nil
		},
	}
	svc, _ := usermanagement.NewService(&mockAuthAdminClient{}, repo, 5*time.Second)

	res, err := svc.UpdateUser(context.Background(), actorID, actorID, usermanagement.UpdateUserInput{
		FullName: &newName,
	})
	if err != nil {
		t.Fatalf("self name update must be permitted, got err: %v", err)
	}
	if *res.FullName != newName {
		t.Fatalf("expected new name %q, got %q", newName, *res.FullName)
	}
	if repo.updateCalls != 1 {
		t.Errorf("repo calls = %d, want 1", repo.updateCalls)
	}
}

func TestService_UpdateUser_SelfPlatformAdminNoOp(t *testing.T) {
	actorID := uuid.New() // same as target
	roleAdmin := "platform_admin"
	name := "Admin Self"

	repo := &mockRepository{
		updateFn: func(_ context.Context, actor, target uuid.UUID, _ *string, role *string) (usermanagement.UpdatedUser, error) {
			if actor != target || actor != actorID {
				t.Fatalf("expected actor == target == actorID")
			}
			return usermanagement.UpdatedUser{
				ID:           actorID,
				FullName:     &name,
				PlatformRole: *role,
			}, nil
		},
	}
	svc, _ := usermanagement.NewService(&mockAuthAdminClient{}, repo, 5*time.Second)

	res, err := svc.UpdateUser(context.Background(), actorID, actorID, usermanagement.UpdateUserInput{
		PlatformRole: &roleAdmin,
	})
	if err != nil {
		t.Fatalf("self platform_admin no-op must be permitted, got err: %v", err)
	}
	if res.PlatformRole != "platform_admin" {
		t.Fatalf("expected platform_admin, got %s", res.PlatformRole)
	}
	if repo.updateCalls != 1 {
		t.Errorf("repo calls = %d, want 1", repo.updateCalls)
	}
}

func TestService_UpdateUser_SelfDemotionWithCombinedNameRejectedBeforeNameWrite(t *testing.T) {
	actorID := uuid.New() // same as target
	attemptedName := "Attempted Name"
	roleUser := "user"

	repo := &mockRepository{
		updateFn: func(_ context.Context, _, _ uuid.UUID, _ *string, _ *string) (usermanagement.UpdatedUser, error) {
			t.Fatal("repo must NOT be called when self demotion is detected")
			return usermanagement.UpdatedUser{}, nil
		},
	}
	svc, _ := usermanagement.NewService(&mockAuthAdminClient{}, repo, 5*time.Second)

	_, err := svc.UpdateUser(context.Background(), actorID, actorID, usermanagement.UpdateUserInput{
		FullName:     &attemptedName,
		PlatformRole: &roleUser,
	})
	if !errors.Is(err, usermanagement.ErrSelfDemotionForbidden) {
		t.Fatalf("expected ErrSelfDemotionForbidden, got %v", err)
	}
	if repo.updateCalls != 0 {
		t.Fatalf("repo.UpdateUserProfileAndRole was called %d times; must be 0 (rejected before write)", repo.updateCalls)
	}
}

func TestService_UpdateUser_SelfDemotionRoleOnlyRejectedBeforeRepoCall(t *testing.T) {
	actorID := uuid.New()
	roleUser := "user"

	repo := &mockRepository{
		updateFn: func(_ context.Context, _, _ uuid.UUID, _ *string, _ *string) (usermanagement.UpdatedUser, error) {
			t.Fatal("repo must NOT be called on self demotion")
			return usermanagement.UpdatedUser{}, nil
		},
	}
	svc, _ := usermanagement.NewService(&mockAuthAdminClient{}, repo, 5*time.Second)

	_, err := svc.UpdateUser(context.Background(), actorID, actorID, usermanagement.UpdateUserInput{
		PlatformRole: &roleUser,
	})
	if !errors.Is(err, usermanagement.ErrSelfDemotionForbidden) {
		t.Fatalf("expected ErrSelfDemotionForbidden, got %v", err)
	}
	if repo.updateCalls != 0 {
		t.Fatalf("repo calls = %d, want 0", repo.updateCalls)
	}
}

func TestService_UpdateUser_AuthorizationDBRecheckInsideMutation(t *testing.T) {
	actorID := uuid.New()
	targetID := uuid.New()
	newName := "Name"

	repo := &mockRepository{
		updateFn: func(_ context.Context, _, _ uuid.UUID, _ *string, _ *string) (usermanagement.UpdatedUser, error) {
			return usermanagement.UpdatedUser{}, usermanagement.ErrActorNotAdmin
		},
	}
	svc, _ := usermanagement.NewService(&mockAuthAdminClient{}, repo, 5*time.Second)

	_, err := svc.UpdateUser(context.Background(), actorID, targetID, usermanagement.UpdateUserInput{
		FullName: &newName,
	})
	if !errors.Is(err, usermanagement.ErrActorNotAdmin) {
		t.Fatalf("expected ErrActorNotAdmin from repo transaction recheck, got %v", err)
	}
}

func TestService_UpdateUser_OneTotalDeadline(t *testing.T) {
	actorID := uuid.New()
	targetID := uuid.New()
	newName := "Name"

	repo := &mockRepository{
		updateFn: func(ctx context.Context, _, _ uuid.UUID, _ *string, _ *string) (usermanagement.UpdatedUser, error) {
			if _, ok := ctx.Deadline(); !ok {
				t.Fatal("expected context to have a deadline")
			}
			return usermanagement.UpdatedUser{ID: targetID, FullName: &newName, PlatformRole: "user"}, nil
		},
	}
	svc, _ := usermanagement.NewService(&mockAuthAdminClient{}, repo, 100*time.Millisecond)

	// Pre-canceled context returns error without calling repo
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := svc.UpdateUser(canceledCtx, actorID, targetID, usermanagement.UpdateUserInput{FullName: &newName})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if repo.updateCalls != 0 {
		t.Fatalf("repo was called on pre-canceled context: %d", repo.updateCalls)
	}

	// Normal call receives context deadline
	res, err := svc.UpdateUser(context.Background(), actorID, targetID, usermanagement.UpdateUserInput{FullName: &newName})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.ID != targetID {
		t.Fatalf("unexpected result: %+v", res)
	}
}

func TestService_UpdateUser_DatabaseErrorPropagated(t *testing.T) {
	actorID := uuid.New()
	targetID := uuid.New()
	newName := "Name"
	dbErr := errors.New("connection reset by peer during commit")

	repo := &mockRepository{
		updateFn: func(_ context.Context, _, _ uuid.UUID, _ *string, _ *string) (usermanagement.UpdatedUser, error) {
			return usermanagement.UpdatedUser{}, dbErr
		},
	}
	svc, _ := usermanagement.NewService(&mockAuthAdminClient{}, repo, 5*time.Second)

	_, err := svc.UpdateUser(context.Background(), actorID, targetID, usermanagement.UpdateUserInput{FullName: &newName})
	if !errors.Is(err, dbErr) {
		t.Fatalf("expected dbErr propagated, got %v (must never map to success)", err)
	}
}

func TestService_UpdateUser_InvalidInput(t *testing.T) {
	actorID := uuid.New()
	targetID := uuid.New()
	repo := &mockRepository{}
	svc, _ := usermanagement.NewService(&mockAuthAdminClient{}, repo, 5*time.Second)

	// 1. Both fields nil
	_, err := svc.UpdateUser(context.Background(), actorID, targetID, usermanagement.UpdateUserInput{})
	if !errors.Is(err, usermanagement.ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput for empty input, got %v", err)
	}

	// 2. Blank full_name
	blank := "   "
	_, err = svc.UpdateUser(context.Background(), actorID, targetID, usermanagement.UpdateUserInput{FullName: &blank})
	if !errors.Is(err, usermanagement.ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput for blank name, got %v", err)
	}

	// 3. Invalid role
	invalidRole := "superuser"
	_, err = svc.UpdateUser(context.Background(), actorID, targetID, usermanagement.UpdateUserInput{PlatformRole: &invalidRole})
	if !errors.Is(err, usermanagement.ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput for invalid role, got %v", err)
	}

	if repo.updateCalls != 0 {
		t.Fatalf("repo was called on invalid input: %d", repo.updateCalls)
	}
}

func TestService_AssignGatewayUser_InsertOperator(t *testing.T) {
	actorID := uuid.New()
	targetID := uuid.New()
	gatewayID := "gw_001"
	repo := &mockRepository{}
	svc, _ := usermanagement.NewService(&mockAuthAdminClient{}, repo, 5*time.Second)

	err := svc.AssignGatewayUser(context.Background(), actorID, gatewayID, targetID, "operator")
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if repo.assignCalls != 1 {
		t.Fatalf("expected 1 assign call, got %d", repo.assignCalls)
	}
	if repo.lastAssignActorID != actorID || repo.lastAssignGateway != gatewayID || repo.lastAssignTarget != targetID || repo.lastAssignRole != "operator" {
		t.Fatalf("unexpected assign arguments: actor=%v, gw=%s, target=%v, role=%s",
			repo.lastAssignActorID, repo.lastAssignGateway, repo.lastAssignTarget, repo.lastAssignRole)
	}
}

func TestService_AssignGatewayUser_InsertViewer(t *testing.T) {
	actorID := uuid.New()
	targetID := uuid.New()
	gatewayID := "gw_002"
	repo := &mockRepository{}
	svc, _ := usermanagement.NewService(&mockAuthAdminClient{}, repo, 5*time.Second)

	err := svc.AssignGatewayUser(context.Background(), actorID, gatewayID, targetID, "viewer")
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if repo.assignCalls != 1 {
		t.Fatalf("expected 1 assign call, got %d", repo.assignCalls)
	}
	if repo.lastAssignRole != "viewer" {
		t.Fatalf("expected role viewer, got %s", repo.lastAssignRole)
	}
}

func TestService_AssignGatewayUser_ChangeRoles(t *testing.T) {
	actorID := uuid.New()
	targetID := uuid.New()
	gatewayID := "gw_003"
	repo := &mockRepository{}
	svc, _ := usermanagement.NewService(&mockAuthAdminClient{}, repo, 5*time.Second)

	// Change operator to viewer
	err := svc.AssignGatewayUser(context.Background(), actorID, gatewayID, targetID, "viewer")
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	// Change viewer to operator
	err = svc.AssignGatewayUser(context.Background(), actorID, gatewayID, targetID, "operator")
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if repo.assignCalls != 2 {
		t.Fatalf("expected 2 assign calls, got %d", repo.assignCalls)
	}
}

func TestService_AssignGatewayUser_ExactNoOp(t *testing.T) {
	actorID := uuid.New()
	targetID := uuid.New()
	gatewayID := "gw_004"
	repo := &mockRepository{}
	svc, _ := usermanagement.NewService(&mockAuthAdminClient{}, repo, 5*time.Second)

	err := svc.AssignGatewayUser(context.Background(), actorID, gatewayID, targetID, "operator")
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if repo.assignCalls != 1 {
		t.Fatalf("expected 1 assign call, got %d", repo.assignCalls)
	}
}

func TestService_AssignGatewayUser_OwnerBodyRejected(t *testing.T) {
	actorID := uuid.New()
	targetID := uuid.New()
	gatewayID := "gw_005"
	repo := &mockRepository{}
	svc, _ := usermanagement.NewService(&mockAuthAdminClient{}, repo, 5*time.Second)

	err := svc.AssignGatewayUser(context.Background(), actorID, gatewayID, targetID, "owner")
	if !errors.Is(err, usermanagement.ErrOwnerRoleNotAllowed) {
		t.Fatalf("expected ErrOwnerRoleNotAllowed, got %v", err)
	}
	if repo.assignCalls != 0 {
		t.Fatalf("repo must not be called when role is owner, got calls=%d", repo.assignCalls)
	}
}

func TestService_AssignGatewayUser_InvalidRoleRejected(t *testing.T) {
	actorID := uuid.New()
	targetID := uuid.New()
	gatewayID := "gw_006"
	repo := &mockRepository{}
	svc, _ := usermanagement.NewService(&mockAuthAdminClient{}, repo, 5*time.Second)

	for _, invalidRole := range []string{"superadmin", "platform_admin", "admin", "foo", ""} {
		err := svc.AssignGatewayUser(context.Background(), actorID, gatewayID, targetID, invalidRole)
		if !errors.Is(err, usermanagement.ErrInvalidRole) && !errors.Is(err, usermanagement.ErrInvalidInput) {
			t.Fatalf("expected ErrInvalidRole or ErrInvalidInput for %q, got %v", invalidRole, err)
		}
	}
	if repo.assignCalls != 0 {
		t.Fatalf("repo must not be called on invalid role, got calls=%d", repo.assignCalls)
	}
}

func TestService_AssignGatewayUser_ExistingOwnerRejected(t *testing.T) {
	actorID := uuid.New()
	targetID := uuid.New()
	gatewayID := "gw_007"
	repo := &mockRepository{
		assignFn: func(_ context.Context, _ uuid.UUID, _ string, _ uuid.UUID, _ string) error {
			return usermanagement.ErrOwnerTransferRequired
		},
	}
	svc, _ := usermanagement.NewService(&mockAuthAdminClient{}, repo, 5*time.Second)

	err := svc.AssignGatewayUser(context.Background(), actorID, gatewayID, targetID, "operator")
	if !errors.Is(err, usermanagement.ErrOwnerTransferRequired) {
		t.Fatalf("expected ErrOwnerTransferRequired, got %v", err)
	}
}

func TestService_AssignGatewayUser_MissingUser(t *testing.T) {
	actorID := uuid.New()
	targetID := uuid.New()
	gatewayID := "gw_008"
	repo := &mockRepository{
		assignFn: func(_ context.Context, _ uuid.UUID, _ string, _ uuid.UUID, _ string) error {
			return usermanagement.ErrUserNotFound
		},
	}
	svc, _ := usermanagement.NewService(&mockAuthAdminClient{}, repo, 5*time.Second)

	err := svc.AssignGatewayUser(context.Background(), actorID, gatewayID, targetID, "operator")
	if !errors.Is(err, usermanagement.ErrUserNotFound) {
		t.Fatalf("expected ErrUserNotFound, got %v", err)
	}
}

func TestService_AssignGatewayUser_MissingGateway(t *testing.T) {
	actorID := uuid.New()
	targetID := uuid.New()
	gatewayID := "gw_nonexistent"
	repo := &mockRepository{
		assignFn: func(_ context.Context, _ uuid.UUID, _ string, _ uuid.UUID, _ string) error {
			return usermanagement.ErrGatewayNotFound
		},
	}
	svc, _ := usermanagement.NewService(&mockAuthAdminClient{}, repo, 5*time.Second)

	err := svc.AssignGatewayUser(context.Background(), actorID, gatewayID, targetID, "operator")
	if !errors.Is(err, usermanagement.ErrGatewayNotFound) {
		t.Fatalf("expected ErrGatewayNotFound, got %v", err)
	}
}

func TestService_AssignGatewayUser_ActorNotAdmin(t *testing.T) {
	actorID := uuid.New()
	targetID := uuid.New()
	gatewayID := "gw_009"
	repo := &mockRepository{
		assignFn: func(_ context.Context, _ uuid.UUID, _ string, _ uuid.UUID, _ string) error {
			return usermanagement.ErrActorNotAdmin
		},
	}
	svc, _ := usermanagement.NewService(&mockAuthAdminClient{}, repo, 5*time.Second)

	err := svc.AssignGatewayUser(context.Background(), actorID, gatewayID, targetID, "operator")
	if !errors.Is(err, usermanagement.ErrActorNotAdmin) {
		t.Fatalf("expected ErrActorNotAdmin, got %v", err)
	}
}

func TestService_AssignGatewayUser_DatabaseErrorPropagated(t *testing.T) {
	actorID := uuid.New()
	targetID := uuid.New()
	gatewayID := "gw_010"
	dbErr := errors.New("db deadlock on commit")
	repo := &mockRepository{
		assignFn: func(_ context.Context, _ uuid.UUID, _ string, _ uuid.UUID, _ string) error {
			return dbErr
		},
	}
	svc, _ := usermanagement.NewService(&mockAuthAdminClient{}, repo, 5*time.Second)

	err := svc.AssignGatewayUser(context.Background(), actorID, gatewayID, targetID, "operator")
	if !errors.Is(err, dbErr) {
		t.Fatalf("expected dbErr propagated, got %v (must never map to success)", err)
	}
}

func TestService_AssignGatewayUser_ContextTimeout(t *testing.T) {
	actorID := uuid.New()
	targetID := uuid.New()
	gatewayID := "gw_011"
	repo := &mockRepository{}
	svc, _ := usermanagement.NewService(&mockAuthAdminClient{}, repo, 5*time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := svc.AssignGatewayUser(ctx, actorID, gatewayID, targetID, "operator")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if repo.assignCalls != 0 {
		t.Fatalf("repo must not be called when context is already canceled, got calls=%d", repo.assignCalls)
	}
}

func TestService_AssignGatewayUser_InvalidInputs(t *testing.T) {
	actorID := uuid.New()
	targetID := uuid.New()
	gatewayID := "gw_012"
	repo := &mockRepository{}
	svc, _ := usermanagement.NewService(&mockAuthAdminClient{}, repo, 5*time.Second)

	// Nil actor
	if err := svc.AssignGatewayUser(context.Background(), uuid.Nil, gatewayID, targetID, "operator"); !errors.Is(err, usermanagement.ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput for nil actor, got %v", err)
	}
	// Nil target
	if err := svc.AssignGatewayUser(context.Background(), actorID, gatewayID, uuid.Nil, "operator"); !errors.Is(err, usermanagement.ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput for nil target, got %v", err)
	}
	// Blank gatewayID
	if err := svc.AssignGatewayUser(context.Background(), actorID, "   ", targetID, "operator"); !errors.Is(err, usermanagement.ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput for blank gatewayID, got %v", err)
	}

	if repo.assignCalls != 0 {
		t.Fatalf("repo must not be called on invalid input, got calls=%d", repo.assignCalls)
	}
}

func TestService_TransferGatewayOwner_TargetWithoutMembership(t *testing.T) {
	actorID := uuid.New()
	targetID := uuid.New()
	gatewayID := "gw_transfer_001"
	repo := &mockRepository{}
	svc, err := usermanagement.NewService(&mockAuthAdminClient{}, repo, 5*time.Second)
	if err != nil {
		t.Fatalf("NewService failed: %v", err)
	}

	err = svc.TransferGatewayOwner(context.Background(), actorID, gatewayID, targetID)
	if err != nil {
		t.Fatalf("expected nil error on valid transfer, got %v", err)
	}

	if repo.transferCalls != 1 {
		t.Fatalf("expected 1 repo transfer call, got %d", repo.transferCalls)
	}
	if repo.lastTransferActorID != actorID {
		t.Fatalf("expected actorID %s, got %s", actorID, repo.lastTransferActorID)
	}
	if repo.lastTransferGateway != gatewayID {
		t.Fatalf("expected gatewayID %s, got %s", gatewayID, repo.lastTransferGateway)
	}
	if repo.lastTransferTarget != targetID {
		t.Fatalf("expected targetID %s, got %s", targetID, repo.lastTransferTarget)
	}
}

func TestService_TransferGatewayOwner_TargetOperator(t *testing.T) {
	actorID := uuid.New()
	targetID := uuid.New()
	gatewayID := "gw_transfer_002"
	repo := &mockRepository{}
	svc, _ := usermanagement.NewService(&mockAuthAdminClient{}, repo, 5*time.Second)

	err := svc.TransferGatewayOwner(context.Background(), actorID, gatewayID, targetID)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if repo.transferCalls != 1 {
		t.Fatalf("expected 1 repo call, got %d", repo.transferCalls)
	}
}

func TestService_TransferGatewayOwner_TargetViewer(t *testing.T) {
	actorID := uuid.New()
	targetID := uuid.New()
	gatewayID := "gw_transfer_003"
	repo := &mockRepository{}
	svc, _ := usermanagement.NewService(&mockAuthAdminClient{}, repo, 5*time.Second)

	err := svc.TransferGatewayOwner(context.Background(), actorID, gatewayID, targetID)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if repo.transferCalls != 1 {
		t.Fatalf("expected 1 repo call, got %d", repo.transferCalls)
	}
}

func TestService_TransferGatewayOwner_AlreadyOwner_NoOp(t *testing.T) {
	actorID := uuid.New()
	targetID := uuid.New()
	gatewayID := "gw_transfer_004"
	repo := &mockRepository{}
	svc, _ := usermanagement.NewService(&mockAuthAdminClient{}, repo, 5*time.Second)

	err := svc.TransferGatewayOwner(context.Background(), actorID, gatewayID, targetID)
	if err != nil {
		t.Fatalf("expected nil error on already-owner no-op, got %v", err)
	}
	if repo.transferCalls != 1 {
		t.Fatalf("expected 1 repo call, got %d", repo.transferCalls)
	}
}

func TestService_TransferGatewayOwner_AbsentTargetUser(t *testing.T) {
	actorID := uuid.New()
	targetID := uuid.New()
	gatewayID := "gw_transfer_005"
	repo := &mockRepository{
		transferFn: func(_ context.Context, _ uuid.UUID, _ string, _ uuid.UUID) error {
			return usermanagement.ErrUserNotFound
		},
	}
	svc, _ := usermanagement.NewService(&mockAuthAdminClient{}, repo, 5*time.Second)

	err := svc.TransferGatewayOwner(context.Background(), actorID, gatewayID, targetID)
	if !errors.Is(err, usermanagement.ErrUserNotFound) {
		t.Fatalf("expected ErrUserNotFound, got %v", err)
	}
}

func TestService_TransferGatewayOwner_AbsentGateway(t *testing.T) {
	actorID := uuid.New()
	targetID := uuid.New()
	gatewayID := "gw_transfer_006"
	repo := &mockRepository{
		transferFn: func(_ context.Context, _ uuid.UUID, _ string, _ uuid.UUID) error {
			return usermanagement.ErrGatewayNotFound
		},
	}
	svc, _ := usermanagement.NewService(&mockAuthAdminClient{}, repo, 5*time.Second)

	err := svc.TransferGatewayOwner(context.Background(), actorID, gatewayID, targetID)
	if !errors.Is(err, usermanagement.ErrGatewayNotFound) {
		t.Fatalf("expected ErrGatewayNotFound, got %v", err)
	}
}

func TestService_TransferGatewayOwner_ZeroOwners(t *testing.T) {
	actorID := uuid.New()
	targetID := uuid.New()
	gatewayID := "gw_transfer_007"
	repo := &mockRepository{
		transferFn: func(_ context.Context, _ uuid.UUID, _ string, _ uuid.UUID) error {
			return usermanagement.ErrGatewayOwnerInconsistent
		},
	}
	svc, _ := usermanagement.NewService(&mockAuthAdminClient{}, repo, 5*time.Second)

	err := svc.TransferGatewayOwner(context.Background(), actorID, gatewayID, targetID)
	if !errors.Is(err, usermanagement.ErrGatewayOwnerInconsistent) {
		t.Fatalf("expected ErrGatewayOwnerInconsistent on zero owners, got %v", err)
	}
}

func TestService_TransferGatewayOwner_MultipleOwners(t *testing.T) {
	actorID := uuid.New()
	targetID := uuid.New()
	gatewayID := "gw_transfer_008"
	repo := &mockRepository{
		transferFn: func(_ context.Context, _ uuid.UUID, _ string, _ uuid.UUID) error {
			return usermanagement.ErrGatewayOwnerInconsistent
		},
	}
	svc, _ := usermanagement.NewService(&mockAuthAdminClient{}, repo, 5*time.Second)

	err := svc.TransferGatewayOwner(context.Background(), actorID, gatewayID, targetID)
	if !errors.Is(err, usermanagement.ErrGatewayOwnerInconsistent) {
		t.Fatalf("expected ErrGatewayOwnerInconsistent on multiple owners, got %v", err)
	}
}

func TestService_TransferGatewayOwner_ActorNotAdmin(t *testing.T) {
	actorID := uuid.New()
	targetID := uuid.New()
	gatewayID := "gw_transfer_009"
	repo := &mockRepository{
		transferFn: func(_ context.Context, _ uuid.UUID, _ string, _ uuid.UUID) error {
			return usermanagement.ErrActorNotAdmin
		},
	}
	svc, _ := usermanagement.NewService(&mockAuthAdminClient{}, repo, 5*time.Second)

	err := svc.TransferGatewayOwner(context.Background(), actorID, gatewayID, targetID)
	if !errors.Is(err, usermanagement.ErrActorNotAdmin) {
		t.Fatalf("expected ErrActorNotAdmin, got %v", err)
	}
}

func TestService_TransferGatewayOwner_DatabaseErrorPropagated(t *testing.T) {
	actorID := uuid.New()
	targetID := uuid.New()
	gatewayID := "gw_transfer_010"
	dbErr := errors.New("db connection lost")
	repo := &mockRepository{
		transferFn: func(_ context.Context, _ uuid.UUID, _ string, _ uuid.UUID) error {
			return dbErr
		},
	}
	svc, _ := usermanagement.NewService(&mockAuthAdminClient{}, repo, 5*time.Second)

	err := svc.TransferGatewayOwner(context.Background(), actorID, gatewayID, targetID)
	if !errors.Is(err, dbErr) {
		t.Fatalf("expected dbErr propagated, got %v", err)
	}
}

func TestService_TransferGatewayOwner_ContextTimeout(t *testing.T) {
	actorID := uuid.New()
	targetID := uuid.New()
	gatewayID := "gw_transfer_011"
	repo := &mockRepository{}
	svc, _ := usermanagement.NewService(&mockAuthAdminClient{}, repo, 5*time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := svc.TransferGatewayOwner(ctx, actorID, gatewayID, targetID)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if repo.transferCalls != 0 {
		t.Fatalf("repo must not be called when context already canceled, got calls=%d", repo.transferCalls)
	}
}

func TestService_TransferGatewayOwner_InvalidInputs(t *testing.T) {
	actorID := uuid.New()
	targetID := uuid.New()
	gatewayID := "gw_transfer_012"
	repo := &mockRepository{}
	svc, _ := usermanagement.NewService(&mockAuthAdminClient{}, repo, 5*time.Second)

	// Nil actor
	if err := svc.TransferGatewayOwner(context.Background(), uuid.Nil, gatewayID, targetID); !errors.Is(err, usermanagement.ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput for nil actor, got %v", err)
	}
	// Nil target
	if err := svc.TransferGatewayOwner(context.Background(), actorID, gatewayID, uuid.Nil); !errors.Is(err, usermanagement.ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput for nil target, got %v", err)
	}
	// Blank gatewayID
	if err := svc.TransferGatewayOwner(context.Background(), actorID, "   ", targetID); !errors.Is(err, usermanagement.ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput for blank gatewayID, got %v", err)
	}

	if repo.transferCalls != 0 {
		t.Fatalf("repo must not be called on invalid inputs, got calls=%d", repo.transferCalls)
	}
}
