package usermanagement_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"iot-platform/internal/auth"
	"iot-platform/internal/digitaltwin"
	"iot-platform/internal/gateway"
	"iot-platform/internal/httpserver"
	"iot-platform/internal/usermanagement"
)

func resolveAuthTestConfig(t *testing.T) (setupURL, backendURL, authBaseURL, anonKey, serviceRoleKey string) {
	t.Helper()

	setupURL, backendURL = resolveDatabaseURLs(t)

	authBaseURL = os.Getenv("USER_MGMT_TEST_AUTH_URL")
	anonKey = os.Getenv("USER_MGMT_TEST_ANON_KEY")
	serviceRoleKey = os.Getenv("USER_MGMT_TEST_SERVICE_ROLE_KEY")

	if authBaseURL != "" && anonKey != "" && serviceRoleKey != "" {
		return setupURL, backendURL, authBaseURL, anonKey, serviceRoleKey
	}

	dir, err := os.Getwd()
	if err != nil {
		return setupURL, backendURL, "", "", ""
	}

	var envPath string
	for {
		candidate := filepath.Join(dir, ".env")
		if _, statErr := os.Stat(candidate); statErr == nil {
			envPath = candidate
			break
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}

	if envPath == "" {
		return setupURL, backendURL, "", "", ""
	}

	content, err := os.ReadFile(envPath)
	if err != nil {
		return setupURL, backendURL, "", "", ""
	}

	envMap := make(map[string]string)
	for _, line := range strings.Split(string(content), "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "#") || !strings.Contains(trimmed, "=") {
			continue
		}
		parts := strings.SplitN(trimmed, "=", 2)
		envMap[strings.TrimSpace(parts[0])] = strings.Trim(strings.TrimSpace(parts[1]), `"'`)
	}

	if authBaseURL == "" {
		authBaseURL = "http://127.0.0.1/auth/v1"
	}
	if anonKey == "" {
		anonKey = envMap["ANON_KEY"]
	}
	if serviceRoleKey == "" {
		serviceRoleKey = envMap["SERVICE_ROLE_KEY"]
	}

	return setupURL, backendURL, authBaseURL, anonKey, serviceRoleKey
}

func randomTestHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic("test entropy unavailable")
	}
	return hex.EncodeToString(b)
}

// decoratedRepoForFailingProfile wraps a real Repository but simulates a profile verification failure.
type decoratedRepoForFailingProfile struct {
	usermanagement.Repository
	injectedErr error
}

// Simulate Auth committing an account while its response is lost to the caller.
type lostCreateResponse struct{ usermanagement.AuthAdminClient }

func (c lostCreateResponse) CreateUser(ctx context.Context, in usermanagement.AuthAdminCreateUserInput) (usermanagement.AuthAdminUser, error) {
	if _, err := c.AuthAdminClient.CreateUser(ctx, in); err != nil {
		return usermanagement.AuthAdminUser{}, err
	}
	return usermanagement.AuthAdminUser{}, context.DeadlineExceeded
}

func (d *decoratedRepoForFailingProfile) VerifyCreatedUserProfile(_ context.Context, _ uuid.UUID, _ string) error {
	return d.injectedErr
}

type staticTokenVerifier struct {
	tokens map[string]auth.Principal
}

func (s staticTokenVerifier) Verify(_ context.Context, token string) (auth.Principal, error) {
	if p, ok := s.tokens[token]; ok {
		return p, nil
	}
	return auth.Principal{}, errors.New("invalid token")
}

//nolint:revive // Test helpers keep testing.T first for consistent call sites.
func checkedCleanup(t *testing.T, pool *pgxpool.Pool, ctx context.Context, query string, args ...any) {
	t.Helper()
	if _, err := pool.Exec(ctx, query, args...); err != nil {
		t.Error("owned fixture cleanup SQL failed")
	}
}

func TestServiceNative_AuthAndUserCreation(t *testing.T) {
	setupURL, backendURL, authBaseURL, anonKey, serviceRoleKey := resolveAuthTestConfig(t)
	if setupURL == "" || backendURL == "" || serviceRoleKey == "" || anonKey == "" {
		t.Fatal("native service auth test requested but configuration missing")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	setupPool, err := pgxpool.New(ctx, setupURL)
	if err != nil {
		t.Fatalf("failed to connect setup pool: %v", err)
	}
	defer setupPool.Close()

	backendPool, err := pgxpool.New(ctx, backendURL)
	if err != nil {
		t.Fatalf("failed to connect backend pool: %v", err)
	}
	defer backendPool.Close()

	// Verify backendPool runs with restricted application role
	var appRole string
	if err := backendPool.QueryRow(ctx, "SELECT current_user").Scan(&appRole); err != nil || appRole != "iot_backend_app" {
		t.Fatalf("backendPool current_user = %q, want iot_backend_app", appRole)
	}

	authAdapter, err := usermanagement.NewSupabaseAuthAdapter(usermanagement.SupabaseAuthAdapterConfig{
		BaseURL:        authBaseURL,
		ServiceRoleKey: serviceRoleKey,
		Timeout:        10 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewSupabaseAuthAdapter failed: %v", err)
	}

	repo, err := usermanagement.NewPostgresRepository(backendPool)
	if err != nil {
		t.Fatalf("NewPostgresRepository failed: %v", err)
	}

	service, err := usermanagement.NewService(authAdapter, repo, 15*time.Second)
	if err != nil {
		t.Fatalf("NewService failed: %v", err)
	}

	// Track created users for owned resource cleanup
	var createdUserIDs []uuid.UUID
	var createdUserEmails []string
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		for _, email := range createdUserEmails {
			rows, err := setupPool.Query(cleanupCtx, "SELECT id FROM auth.users WHERE email = $1", email)
			if err != nil {
				t.Error("resolve owned account cleanup failed")
				continue
			}
			for rows.Next() {
				var id uuid.UUID
				if err := rows.Scan(&id); err != nil {
					t.Error("scan owned account cleanup failed")
					break
				}
				createdUserIDs = append(createdUserIDs, id)
			}
			if rows.Err() != nil {
				t.Error("read owned account cleanup failed")
			}
			rows.Close()
		}
		for _, id := range createdUserIDs {
			// Clean up via setupPool (cascade deletes profiles and roles)
			checkedCleanup(t, setupPool, cleanupCtx, "DELETE FROM public.platform_admins WHERE user_id = $1", id)
			checkedCleanup(t, setupPool, cleanupCtx, "DELETE FROM public.profiles WHERE id = $1", id)
			checkedCleanup(t, setupPool, cleanupCtx, "DELETE FROM auth.users WHERE id = $1", id)
		}
		for _, email := range createdUserEmails {
			var remaining int
			if err := setupPool.QueryRow(cleanupCtx, "SELECT count(*) FROM auth.users WHERE email = $1", email).Scan(&remaining); err != nil || remaining != 0 {
				t.Error("owned account cleanup verification failed")
			}
		}
		var remaining int
		if err := setupPool.QueryRow(cleanupCtx, `SELECT
		 (SELECT count(*) FROM auth.users WHERE id = ANY($1)) +
		 (SELECT count(*) FROM public.profiles WHERE id = ANY($1)) +
		 (SELECT count(*) FROM public.platform_admins WHERE user_id = ANY($1)) +
		 (SELECT count(*) FROM public.user_gateways WHERE user_id = ANY($1))`, createdUserIDs).Scan(&remaining); err != nil || remaining != 0 {
			t.Error("service fixture cleanup verification failed")
		}
	}()

	// 1. Seed admin actor and non-admin actor in database
	adminActorID := uuid.New()
	nonAdminActorID := uuid.New()
	createdUserIDs = append(createdUserIDs, adminActorID, nonAdminActorID)
	now := time.Now().UTC()

	_, err = setupPool.Exec(ctx, `
		INSERT INTO auth.users (id, email, raw_user_meta_data, created_at, aud, role)
		VALUES ($1, 'admin_actor@test.local', '{"full_name":"Admin Actor"}', $3, 'authenticated', 'authenticated'),
		       ($2, 'non_admin_actor@test.local', '{"full_name":"Non Admin"}', $3, 'authenticated', 'authenticated')`,
		adminActorID, nonAdminActorID, now,
	)
	if err != nil {
		t.Fatalf("failed to seed test actors: %v", err)
	}

	_, err = setupPool.Exec(ctx, "INSERT INTO public.platform_admins (user_id) VALUES ($1)", adminActorID)
	if err != nil {
		t.Fatalf("failed to grant platform_admin to adminActorID: %v", err)
	}

	t.Run("CreateUser real Gateway Auth create, confirmed account, exact profile, password login, and zero global role/membership", func(t *testing.T) {
		suffix := randomTestHex(4)
		testEmail := fmt.Sprintf("u3_native_%s@example.com", suffix)
		testPassword := "Passw0rd_" + randomTestHex(24) + "!"
		createdUserEmails = append(createdUserEmails, testEmail)
		fullName := "Nguyễn Văn A Native"

		input := usermanagement.CreateUserInput{
			FullName: fullName,
			Email:    testEmail,
			Password: testPassword,
		}

		// 1. Execute Service CreateUser (calls Envoy -> GoTrue via authAdapter, then verifies DB via repo)
		created, err := service.CreateUser(ctx, adminActorID, input)
		if err != nil {
			t.Fatalf("CreateUser failed: %v", err)
		}
		createdUserIDs = append(createdUserIDs, created.ID)

		// Assert safe success DTO
		if created.ID == uuid.Nil {
			t.Fatal("expected non-nil created.ID")
		}
		if created.Email != testEmail {
			t.Errorf("created.Email = %q, want %q", created.Email, testEmail)
		}
		if created.FullName != fullName {
			t.Errorf("created.FullName = %q, want %q", created.FullName, fullName)
		}
		if created.PlatformRole != "user" {
			t.Errorf("created.PlatformRole = %q, want 'user'", created.PlatformRole)
		}
		if created.CreatedAt.IsZero() {
			t.Error("expected non-zero created.CreatedAt")
		}

		// 2. Verify account is auto-confirmed in auth.users
		var emailConfirmedAt *time.Time
		err = setupPool.QueryRow(ctx, "SELECT email_confirmed_at FROM auth.users WHERE id = $1", created.ID).Scan(&emailConfirmedAt)
		if err != nil {
			t.Fatalf("query auth.users email_confirmed_at failed: %v", err)
		}
		if emailConfirmedAt == nil {
			t.Fatal("expected account to be confirmed (email_confirmed_at != nil), but was nil")
		}

		// 3. Verify exact profile in public.profiles (populated via trigger on_auth_user_created)
		var profileFullName string
		err = setupPool.QueryRow(ctx, "SELECT full_name FROM public.profiles WHERE id = $1", created.ID).Scan(&profileFullName)
		if err != nil {
			t.Fatalf("query public.profiles failed: %v", err)
		}
		if profileFullName != fullName {
			t.Errorf("profiles.full_name = %q, want %q", profileFullName, fullName)
		}

		// 4. Verify zero global role (not in platform_admins)
		var adminCount int
		err = setupPool.QueryRow(ctx, "SELECT COUNT(*) FROM public.platform_admins WHERE user_id = $1", created.ID).Scan(&adminCount)
		if err != nil {
			t.Fatalf("query platform_admins failed: %v", err)
		}
		if adminCount != 0 {
			t.Errorf("new user must have zero global roles, found %d platform_admins entries", adminCount)
		}

		// 5. Verify zero memberships (not in user_gateways)
		var membershipCount int
		err = setupPool.QueryRow(ctx, "SELECT COUNT(*) FROM public.user_gateways WHERE user_id = $1", created.ID).Scan(&membershipCount)
		if err != nil {
			t.Fatalf("query user_gateways failed: %v", err)
		}
		if membershipCount != 0 {
			t.Errorf("new user must have zero gateway memberships, found %d user_gateways entries", membershipCount)
		}

		// 6. Verify password login via supported Envoy path (/auth/v1/token?grant_type=password)
		loginURL := strings.TrimRight(authBaseURL, "/") + "/token?grant_type=password"
		loginPayload, _ := json.Marshal(map[string]string{
			"email":    testEmail,
			"password": testPassword,
		})
		loginReq, err := http.NewRequestWithContext(ctx, http.MethodPost, loginURL, bytes.NewReader(loginPayload))
		if err != nil {
			t.Fatalf("create login request failed: %v", err)
		}
		loginReq.Header.Set("Content-Type", "application/json")
		loginReq.Header.Set("apikey", anonKey)

		loginResp, err := http.DefaultClient.Do(loginReq)
		if err != nil {
			t.Fatalf("login request failed: %v", err)
		}
		defer func() { _ = loginResp.Body.Close() }()

		if loginResp.StatusCode != http.StatusOK {
			t.Fatalf("login returned HTTP %d, want 200 OK", loginResp.StatusCode)
		}

		var loginResult struct {
			AccessToken string `json:"access_token"`
			TokenType   string `json:"token_type"`
			User        struct {
				ID string `json:"id"`
			} `json:"user"`
		}
		if err := json.NewDecoder(io.LimitReader(loginResp.Body, 1<<20)).Decode(&loginResult); err != nil {
			t.Fatal("decode login response failed")
		}
		if loginResult.AccessToken == "" {
			t.Error("expected non-empty access_token upon login")
		}
		if loginResult.User.ID != created.ID.String() {
			t.Errorf("login user.id = %q, want %q", loginResult.User.ID, created.ID.String())
		}

		// 7. Duplicate email negative: creating again with same email returns ErrUserAlreadyExists
		_, dupErr := service.CreateUser(ctx, adminActorID, input)
		if !errors.Is(dupErr, usermanagement.ErrUserAlreadyExists) {
			t.Fatalf("expected ErrUserAlreadyExists on duplicate email, got %v", dupErr)
		}
	})

	t.Run("CreateUser negative: invalid password rejected before Auth call", func(t *testing.T) {
		email := fmt.Sprintf("invalid_pass_%s@example.com", randomTestHex(16))
		createdUserEmails = append(createdUserEmails, email)
		input := usermanagement.CreateUserInput{
			FullName: "Invalid Password User",
			Email:    email,
			Password: "123", // too short (< 6 chars)
		}

		_, err := service.CreateUser(ctx, adminActorID, input)
		if !errors.Is(err, usermanagement.ErrInvalidInput) {
			t.Fatalf("expected ErrInvalidInput for short password, got %v", err)
		}

		// Verify no user was created in auth.users
		var count int
		err = setupPool.QueryRow(ctx, "SELECT COUNT(*) FROM auth.users WHERE email = $1", input.Email).Scan(&count)
		if err != nil {
			t.Fatalf("query auth.users failed: %v", err)
		}
		if count != 0 {
			t.Fatalf("expected 0 auth.users rows for rejected password, got %d", count)
		}
	})

	t.Run("CreateUser negative: non-admin actor rejected without Auth side effects", func(t *testing.T) {
		email := fmt.Sprintf("denied_actor_%s@example.com", randomTestHex(16))
		createdUserEmails = append(createdUserEmails, email)
		input := usermanagement.CreateUserInput{
			FullName: "Denied Actor User",
			Email:    email,
			Password: "Passw0rd_" + randomTestHex(24) + "!",
		}

		_, err := service.CreateUser(ctx, nonAdminActorID, input)
		if !errors.Is(err, usermanagement.ErrActorNotAdmin) {
			t.Fatalf("expected ErrActorNotAdmin for non-admin actor, got %v", err)
		}

		// Verify no user was created in auth.users
		var count int
		err = setupPool.QueryRow(ctx, "SELECT COUNT(*) FROM auth.users WHERE email = $1", input.Email).Scan(&count)
		if err != nil {
			t.Fatalf("query auth.users failed: %v", err)
		}
		if count != 0 {
			t.Fatalf("expected 0 auth.users rows for non-admin actor, got %d", count)
		}
	})

	t.Run("CreateUser negative: profile verification failure preserves target UUID without success or delete", func(t *testing.T) {
		// Decorate repo to simulate profile verification failure
		failingRepo := &decoratedRepoForFailingProfile{
			Repository:  repo,
			injectedErr: usermanagement.ErrInconsistentProfile,
		}
		svcWithFailingProfile, err := usermanagement.NewService(authAdapter, failingRepo, 15*time.Second)
		if err != nil {
			t.Fatalf("NewService failed: %v", err)
		}

		suffix := randomTestHex(4)
		testEmail := fmt.Sprintf("profile_fail_%s@example.com", suffix)
		createdUserEmails = append(createdUserEmails, testEmail)
		input := usermanagement.CreateUserInput{
			FullName: "Profile Fail User",
			Email:    testEmail,
			Password: "Passw0rd_" + randomTestHex(24) + "!",
		}

		_, err = svcWithFailingProfile.CreateUser(ctx, adminActorID, input)
		if err == nil {
			t.Fatal("expected error on profile verification failure, got nil")
		}

		var pve *usermanagement.ProfileVerificationError
		if !errors.As(err, &pve) {
			t.Fatalf("expected *ProfileVerificationError, got %T: %v", err, err)
		}
		if pve.TargetID == uuid.Nil {
			t.Fatal("expected non-nil TargetID in ProfileVerificationError")
		}
		// Intentionally do not register the returned ID: cleanup must resolve email.

		// Assert: user exists in auth.users (not deleted by automatic compensation)
		var existsInAuth bool
		err = setupPool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM auth.users WHERE id = $1)", pve.TargetID).Scan(&existsInAuth)
		if err != nil || !existsInAuth {
			t.Fatalf("expected created user to remain in auth.users for audit/reconciliation (exists=%v, err=%v)", existsInAuth, err)
		}
	})

	t.Run("lost create response cleans up by preregistered exact email", func(t *testing.T) {
		email := "lost_service_" + randomTestHex(16) + "@example.com"
		createdUserEmails = append(createdUserEmails, email)
		svc, err := usermanagement.NewService(lostCreateResponse{authAdapter}, repo, 15*time.Second)
		if err != nil {
			t.Fatal("create lost-response service failed")
		}
		_, err = svc.CreateUser(ctx, adminActorID, usermanagement.CreateUserInput{FullName: "Lost Service Response", Email: email, Password: "Passw0rd_" + randomTestHex(24) + "!"})
		if err == nil {
			t.Fatal("lost response incorrectly returned success")
		}
		var count int
		if err := setupPool.QueryRow(ctx, "SELECT count(*) FROM auth.users WHERE email = $1", email).Scan(&count); err != nil || count != 1 {
			t.Fatal("lost response fixture did not create account")
		}
		// Outer cleanup verifies zero rows by email, without ever receiving an ID.
	})

	t.Run("Public signup disabled negative: /auth/v1/signup rejected via Envoy", func(t *testing.T) {
		signupURL := strings.TrimRight(authBaseURL, "/") + "/signup"
		signupEmail := fmt.Sprintf("public_signup_%s@example.com", randomTestHex(16))
		createdUserEmails = append(createdUserEmails, signupEmail)
		signupPayload, _ := json.Marshal(map[string]string{
			"email":    signupEmail,
			"password": "Passw0rd_" + randomTestHex(24) + "!",
		})

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, signupURL, bytes.NewReader(signupPayload))
		if err != nil {
			t.Fatalf("create signup request failed: %v", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("apikey", anonKey)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("signup request failed: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		// Public signup must be disabled (returns 422 with signup_disabled or similar denial)
		if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated {
			t.Fatalf("public signup succeeded (HTTP %d); expected public signup to be disabled", resp.StatusCode)
		}

		t.Logf("public signup rejected: HTTP %d", resp.StatusCode)
	})
}

func TestServiceNative_ListUsers(t *testing.T) {
	setupURL, backendURL, authBaseURL, _, serviceRoleKey := resolveAuthTestConfig(t)
	if setupURL == "" || backendURL == "" || serviceRoleKey == "" {
		t.Fatal("native service list test requested but configuration missing")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	setupPool, err := pgxpool.New(ctx, setupURL)
	if err != nil {
		t.Fatalf("failed to connect setup pool: %v", err)
	}
	defer setupPool.Close()

	backendPool, err := pgxpool.New(ctx, backendURL)
	if err != nil {
		t.Fatalf("failed to connect backend pool: %v", err)
	}
	defer backendPool.Close()

	// Discover an existing platform admin
	var adminActorID uuid.UUID
	err = setupPool.QueryRow(ctx, "SELECT user_id FROM public.platform_admins LIMIT 1").Scan(&adminActorID)
	if err != nil {
		t.Fatalf("query platform admin failed: %v", err)
	}

	// Non-admin actor
	nonAdminActorID := uuid.MustParse("99999999-9999-9999-9999-999999999999")

	authAdapter, err := usermanagement.NewSupabaseAuthAdapter(usermanagement.SupabaseAuthAdapterConfig{
		BaseURL:        authBaseURL,
		ServiceRoleKey: serviceRoleKey,
		Timeout:        10 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewSupabaseAuthAdapter failed: %v", err)
	}

	repo, err := usermanagement.NewPostgresRepository(backendPool)
	if err != nil {
		t.Fatalf("NewPostgresRepository failed: %v", err)
	}

	service, err := usermanagement.NewService(authAdapter, repo, 15*time.Second)
	if err != nil {
		t.Fatalf("NewService failed: %v", err)
	}

	t.Run("admin actor lists all users with stable ordering and memberships", func(t *testing.T) {
		items, err := service.ListUsers(ctx, adminActorID)
		if err != nil {
			t.Fatalf("ListUsers failed: %v", err)
		}
		if len(items) == 0 {
			t.Fatal("expected at least 1 user in native database, got 0")
		}

		// Verify stable ordering: created_at ASC, id ASC
		for i := 1; i < len(items); i++ {
			prev := items[i-1]
			curr := items[i]
			if prev.CreatedAt.After(curr.CreatedAt) {
				t.Errorf("ordering violation: items[%d].CreatedAt (%v) > items[%d].CreatedAt (%v)",
					i-1, prev.CreatedAt, i, curr.CreatedAt)
			} else if prev.CreatedAt.Equal(curr.CreatedAt) && prev.ID.String() > curr.ID.String() {
				t.Errorf("id tie-breaking violation: items[%d].ID (%s) > items[%d].ID (%s)",
					i-1, prev.ID, i, curr.ID)
			}
		}

		// Verify memberships are sorted by gateway_id ASC
		for _, item := range items {
			if item.GatewayRoles == nil {
				t.Errorf("user %s has nil GatewayRoles, want non-nil slice", item.ID)
			}
			for j := 1; j < len(item.GatewayRoles); j++ {
				if item.GatewayRoles[j-1].GatewayID > item.GatewayRoles[j].GatewayID {
					t.Errorf("user %s memberships not sorted by gateway_id ASC: %+v", item.ID, item.GatewayRoles)
				}
			}
		}
	})

	t.Run("non-admin actor is rejected with ErrActorNotAdmin", func(t *testing.T) {
		_, err := service.ListUsers(ctx, nonAdminActorID)
		if !errors.Is(err, usermanagement.ErrActorNotAdmin) {
			t.Fatalf("expected ErrActorNotAdmin, got %v", err)
		}
	})

	t.Run("capacity overflow returns ErrCapacityExceeded without partial list", func(t *testing.T) {
		boundedService, _ := usermanagement.NewService(authAdapter, repo, 15*time.Second)
		boundedService.WithUserListConfig(usermanagement.UserListConfig{
			MaxUsers:         1,
			MaxMemberships:   10000,
			MaxResponseBytes: 2 * 1024 * 1024,
		})

		items, err := boundedService.ListUsers(ctx, adminActorID)
		if !errors.Is(err, usermanagement.ErrCapacityExceeded) {
			t.Fatalf("expected ErrCapacityExceeded for MaxUsers=1, got %v", err)
		}
		if items != nil {
			t.Fatalf("expected nil items on capacity exceeded, got %+v", items)
		}
	})

	t.Run("response byte overflow returns ErrCapacityExceeded without partial list", func(t *testing.T) {
		boundedService, _ := usermanagement.NewService(authAdapter, repo, 15*time.Second)
		boundedService.WithUserListConfig(usermanagement.UserListConfig{
			MaxUsers:         1000,
			MaxMemberships:   10000,
			MaxResponseBytes: 50, // very small
		})

		items, err := boundedService.ListUsers(ctx, adminActorID)
		if !errors.Is(err, usermanagement.ErrCapacityExceeded) {
			t.Fatalf("expected ErrCapacityExceeded for small MaxResponseBytes, got %v", err)
		}
		if items != nil {
			t.Fatalf("expected nil items on byte overflow, got %+v", items)
		}
	})
}

func TestServiceNative_UpdateUser_RoleEffectsAndInvariants(t *testing.T) {
	setupURL, backendURL, authBaseURL, _, serviceRoleKey := resolveAuthTestConfig(t)
	if setupURL == "" || backendURL == "" || serviceRoleKey == "" {
		t.Fatal("native service update test requested but configuration missing")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	setupPool, err := pgxpool.New(ctx, setupURL)
	if err != nil {
		t.Fatalf("failed to connect setup pool: %v", err)
	}
	defer setupPool.Close()

	backendPool, err := pgxpool.New(ctx, backendURL)
	if err != nil {
		t.Fatalf("failed to connect backend pool: %v", err)
	}
	defer backendPool.Close()

	// Own the actor: never modify or restore an unrelated shared account.
	adminActorID := uuid.New()
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for _, query := range []string{"DELETE FROM public.platform_admins WHERE user_id = $1", "DELETE FROM public.profiles WHERE id = $1", "DELETE FROM auth.users WHERE id = $1"} {
			if _, err := setupPool.Exec(cleanupCtx, query, adminActorID); err != nil {
				t.Error("owned admin cleanup failed")
			}
		}
	}()
	if _, err := setupPool.Exec(ctx, `INSERT INTO auth.users (id,email,raw_user_meta_data,created_at,aud,role) VALUES ($1,$2,'{"full_name":"Owned Admin"}',now(),'authenticated','authenticated')`, adminActorID, "admin_patch_"+adminActorID.String()+"@example.com"); err != nil {
		t.Fatal("seed owned admin failed")
	}
	if _, err := setupPool.Exec(ctx, "INSERT INTO public.platform_admins (user_id,granted_by) VALUES ($1,$1)", adminActorID); err != nil {
		t.Fatal("grant owned admin failed")
	}

	// 2. Create unique test target user and gateway
	targetUserID := uuid.New()
	targetEmail := fmt.Sprintf("target_patch_%s@test.local", randomTestHex(4))
	testGatewayID := fmt.Sprintf("gw_patch_%s", randomTestHex(4))
	initialName := "Initial Target Name"
	now := time.Now().UTC()

	// Seed target user into auth.users and public.profiles
	_, err = setupPool.Exec(ctx, `INSERT INTO auth.users (id, email, raw_user_meta_data, created_at, aud, role) 
		VALUES ($1, $2, '{"full_name":"Initial Target Name"}', $3, 'authenticated', 'authenticated')`,
		targetUserID, targetEmail, now,
	)
	if err != nil {
		t.Fatalf("seed auth.users failed: %v", err)
	}

	_, err = setupPool.Exec(ctx, `INSERT INTO public.profiles (id, full_name) 
		VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET full_name = $2`,
		targetUserID, initialName,
	)
	if err != nil {
		t.Fatalf("seed public.profiles failed: %v", err)
	}

	// Seed gateway and target user's membership
	_, err = setupPool.Exec(ctx, `INSERT INTO public.gateways (gateway_id, name) 
		VALUES ($1, 'Patch Native Test GW')`,
		testGatewayID,
	)
	if err != nil {
		t.Fatalf("seed public.gateways failed: %v", err)
	}

	_, err = setupPool.Exec(ctx, `INSERT INTO public.user_gateways (user_id, gateway_id, role) 
		VALUES ($1, $2, 'operator')`,
		targetUserID, testGatewayID,
	)
	if err != nil {
		t.Fatalf("seed public.user_gateways failed: %v", err)
	}

	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		checkedCleanup(t, setupPool, cleanupCtx, "DELETE FROM public.user_gateways WHERE gateway_id = $1", testGatewayID)
		checkedCleanup(t, setupPool, cleanupCtx, "DELETE FROM public.gateways WHERE gateway_id = $1", testGatewayID)
		checkedCleanup(t, setupPool, cleanupCtx, "DELETE FROM public.platform_admins WHERE user_id = $1", targetUserID)
		checkedCleanup(t, setupPool, cleanupCtx, "DELETE FROM public.profiles WHERE id = $1", targetUserID)
		checkedCleanup(t, setupPool, cleanupCtx, "DELETE FROM auth.users WHERE id = $1", targetUserID)
	}()

	// 3. Initialize real Service and PlatformAdminChecker connected to backendPool (under app role iot_backend_app)
	authAdapter, err := usermanagement.NewSupabaseAuthAdapter(usermanagement.SupabaseAuthAdapterConfig{
		BaseURL:        authBaseURL,
		ServiceRoleKey: serviceRoleKey,
		Timeout:        10 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewSupabaseAuthAdapter failed: %v", err)
	}

	repo, err := usermanagement.NewPostgresRepository(backendPool)
	if err != nil {
		t.Fatalf("NewPostgresRepository failed: %v", err)
	}

	service, err := usermanagement.NewService(authAdapter, repo, 15*time.Second)
	if err != nil {
		t.Fatalf("NewService failed: %v", err)
	}

	realChecker, err := auth.NewPostgresPlatformAdminChecker(backendPool)
	if err != nil {
		t.Fatalf("NewPostgresPlatformAdminChecker failed: %v", err)
	}

	// 4. Set up test router with real PlatformAdminChecker
	deps := httpserver.DefaultTestRouterDependencies()
	deps.PlatformAdminChecker = realChecker
	deps.TokenVerifier = staticTokenVerifier{
		tokens: map[string]auth.Principal{
			"admin-token":  {UserID: adminActorID, Role: "authenticated"},
			"target-token": {UserID: targetUserID, Role: "authenticated"},
		},
	}

	router, err := httpserver.NewRouter(deps)
	if err != nil {
		t.Fatalf("NewRouter failed: %v", err)
	}

	// Helper for checking /v1/me/permissions
	checkPermissions := func() bool {
		req := httptest.NewRequest(http.MethodGet, "/v1/me/permissions", nil)
		req.Header.Set("Authorization", "Bearer target-token")
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("GET /v1/me/permissions status = %d, want 200; body=%s", rec.Code, rec.Body.String())
		}
		var res struct {
			IsPlatformAdmin bool `json:"is_platform_admin"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
			t.Fatalf("decode permissions response failed: %v", err)
		}
		return res.IsPlatformAdmin
	}

	// Helper for checking /v1/admin/gateways (PlatformAdminMiddleware)
	checkAdminMiddleware := func() int {
		req := httptest.NewRequest(http.MethodGet, "/v1/admin/gateways", nil)
		req.Header.Set("Authorization", "Bearer target-token")
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		return rec.Code
	}

	// 5. Stage A: Target is ordinary user before grant
	t.Run("Stage A: target is ordinary user before grant", func(t *testing.T) {
		if checkPermissions() {
			t.Fatal("expected target to NOT be platform admin initially")
		}
		if status := checkAdminMiddleware(); status != http.StatusForbidden {
			t.Fatalf("PlatformAdminMiddleware status = %d, want 403 Forbidden for ordinary user", status)
		}
	})

	// 6. Action 1: Grant admin and update to canonical Vietnamese name
	canonicalName := "Nguyễn Văn Hào"
	roleAdmin := "platform_admin"
	t.Run("Action 1: grant admin and set canonical name", func(t *testing.T) {
		updated, err := service.UpdateUser(ctx, adminActorID, targetUserID, usermanagement.UpdateUserInput{
			FullName:     &canonicalName,
			PlatformRole: &roleAdmin,
		})
		if err != nil {
			t.Fatalf("UpdateUser grant admin failed: %v", err)
		}
		if updated.FullName == nil || *updated.FullName != canonicalName {
			t.Fatalf("updated.FullName = %v, want %q", updated.FullName, canonicalName)
		}
		if updated.PlatformRole != "platform_admin" {
			t.Fatalf("updated.PlatformRole = %q, want platform_admin", updated.PlatformRole)
		}

		// Verify in DB that profiles.full_name is canonicalName and platform_admins has row
		var dbName string
		err = setupPool.QueryRow(ctx, "SELECT full_name FROM public.profiles WHERE id = $1", targetUserID).Scan(&dbName)
		if err != nil || dbName != canonicalName {
			t.Fatalf("DB full_name = %q (err=%v), want %q", dbName, err, canonicalName)
		}

		var grantedBy uuid.UUID
		err = setupPool.QueryRow(ctx, "SELECT granted_by FROM public.platform_admins WHERE user_id = $1", targetUserID).Scan(&grantedBy)
		if err != nil || grantedBy != adminActorID {
			t.Fatalf("platform_admins granted_by = %v (err=%v), want %v", grantedBy, err, adminActorID)
		}
	})

	// 7. Stage B: Next-request DB role effects after grant
	t.Run("Stage B: next-request DB role effects reflect grant immediately", func(t *testing.T) {
		if !checkPermissions() {
			t.Fatal("expected /v1/me/permissions to return is_platform_admin=true on next request after grant")
		}
		if status := checkAdminMiddleware(); status != http.StatusOK {
			t.Fatalf("PlatformAdminMiddleware status = %d, want 200 OK after grant", status)
		}
	})

	// 8. Action 2: Revoke admin and verify membership preservation
	roleUser := "user"
	t.Run("Action 2: revoke admin and preserve gateway memberships", func(t *testing.T) {
		updated, err := service.UpdateUser(ctx, adminActorID, targetUserID, usermanagement.UpdateUserInput{
			PlatformRole: &roleUser,
		})
		if err != nil {
			t.Fatalf("UpdateUser revoke admin failed: %v", err)
		}
		if updated.PlatformRole != "user" {
			t.Fatalf("updated.PlatformRole = %q, want user", updated.PlatformRole)
		}

		// Verify in DB: platform_admins row removed
		var adminCount int
		err = setupPool.QueryRow(ctx, "SELECT count(*) FROM public.platform_admins WHERE user_id = $1", targetUserID).Scan(&adminCount)
		if err != nil || adminCount != 0 {
			t.Fatalf("platform_admins count = %d (err=%v), want 0 after revoke", adminCount, err)
		}

		// Verify membership preservation in user_gateways
		var membershipRole string
		err = setupPool.QueryRow(ctx, "SELECT role FROM public.user_gateways WHERE user_id = $1 AND gateway_id = $2", targetUserID, testGatewayID).Scan(&membershipRole)
		if err != nil || membershipRole != "operator" {
			t.Fatalf("user_gateways role = %q (err=%v), want 'operator' preserved after role revoke", membershipRole, err)
		}
	})

	// 9. Stage C: Next-request DB role effects after revoke
	t.Run("Stage C: next-request DB role effects reflect revoke immediately", func(t *testing.T) {
		if checkPermissions() {
			t.Fatal("expected /v1/me/permissions to return is_platform_admin=false on next request after revoke")
		}
		if status := checkAdminMiddleware(); status != http.StatusForbidden {
			t.Fatalf("PlatformAdminMiddleware status = %d, want 403 Forbidden after revoke", status)
		}
	})

	// 10. Action 3: Combined rollback (self demotion forbidden with name write)
	t.Run("Action 3: atomic combined rollback on forbidden self demotion", func(t *testing.T) {
		attemptedHackedName := "Hacked Admin Name"
		_, err := service.UpdateUser(ctx, adminActorID, adminActorID, usermanagement.UpdateUserInput{
			FullName:     &attemptedHackedName,
			PlatformRole: &roleUser,
		})
		if !errors.Is(err, usermanagement.ErrSelfDemotionForbidden) {
			t.Fatalf("expected ErrSelfDemotionForbidden, got %v", err)
		}

		// Verify in DB: admin actor's full_name was NOT changed to attemptedHackedName
		var nameAfterRollback *string
		err = setupPool.QueryRow(ctx, "SELECT full_name FROM public.profiles WHERE id = $1", adminActorID).Scan(&nameAfterRollback)
		if err != nil {
			t.Fatalf("query admin actor profile failed: %v", err)
		}
		if nameAfterRollback != nil && *nameAfterRollback == attemptedHackedName {
			t.Fatalf("admin actor full_name was modified to %q despite atomic rollback!", attemptedHackedName)
		}

		// Verify admin actor is still platform_admin
		var actorAdminCount int
		err = setupPool.QueryRow(ctx, "SELECT count(*) FROM public.platform_admins WHERE user_id = $1", adminActorID).Scan(&actorAdminCount)
		if err != nil || actorAdminCount != 1 {
			t.Fatalf("admin actor removed from platform_admins despite transaction rollback: %v", err)
		}
	})

	// 11. Action 4: Self name update permitted
	t.Run("Action 4: admin self name update is permitted", func(t *testing.T) {
		newAdminName := "Admin Self Rename Verified"
		updated, err := service.UpdateUser(ctx, adminActorID, adminActorID, usermanagement.UpdateUserInput{
			FullName: &newAdminName,
		})
		if err != nil {
			t.Fatalf("self name update failed: %v", err)
		}
		if updated.FullName == nil || *updated.FullName != newAdminName {
			t.Fatalf("updated.FullName = %v, want %q", updated.FullName, newAdminName)
		}

		var dbName string
		err = setupPool.QueryRow(ctx, "SELECT full_name FROM public.profiles WHERE id = $1", adminActorID).Scan(&dbName)
		if err != nil || dbName != newAdminName {
			t.Fatalf("DB full_name = %q (err=%v), want %q", dbName, err, newAdminName)
		}
	})

	// 12. Action 5: Self platform_admin no-op
	t.Run("Action 5: admin self platform_admin is no-op preserving granted_at/by", func(t *testing.T) {
		var origGrantedBy uuid.UUID
		var origGrantedAt time.Time
		err := setupPool.QueryRow(ctx, "SELECT granted_by, granted_at FROM public.platform_admins WHERE user_id = $1", adminActorID).Scan(&origGrantedBy, &origGrantedAt)
		if err != nil {
			t.Fatalf("query admin actor platform_admins failed: %v", err)
		}

		updated, err := service.UpdateUser(ctx, adminActorID, adminActorID, usermanagement.UpdateUserInput{
			PlatformRole: &roleAdmin,
		})
		if err != nil {
			t.Fatalf("self platform_admin no-op failed: %v", err)
		}
		if updated.PlatformRole != "platform_admin" {
			t.Fatalf("updated.PlatformRole = %q, want platform_admin", updated.PlatformRole)
		}

		var newGrantedBy uuid.UUID
		var newGrantedAt time.Time
		err = setupPool.QueryRow(ctx, "SELECT granted_by, granted_at FROM public.platform_admins WHERE user_id = $1", adminActorID).Scan(&newGrantedBy, &newGrantedAt)
		if err != nil {
			t.Fatalf("query platform_admins after no-op failed: %v", err)
		}
		if newGrantedBy != origGrantedBy || !newGrantedAt.Equal(origGrantedAt) {
			t.Fatalf("no-op grant modified granted_by (%v -> %v) or granted_at (%v -> %v)", origGrantedBy, newGrantedBy, origGrantedAt, newGrantedAt)
		}
	})
}

func TestServiceNative_AssignGatewayUser_InvariantsAndReaderAccess(t *testing.T) {
	setupURL, backendURL, authBaseURL, _, serviceRoleKey := resolveAuthTestConfig(t)
	if setupURL == "" || backendURL == "" {
		t.Fatal("native database test requested but configuration missing")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	setupPool, err := pgxpool.New(ctx, setupURL)
	if err != nil {
		t.Fatalf("failed to connect setup pool: %v", err)
	}
	defer setupPool.Close()

	backendPool, err := pgxpool.New(ctx, backendURL)
	if err != nil {
		t.Fatalf("failed to connect backend pool: %v", err)
	}
	defer backendPool.Close()

	var appRole string
	if err := backendPool.QueryRow(ctx, "SELECT current_user").Scan(&appRole); err != nil || appRole != "iot_backend_app" {
		t.Fatalf("backendPool current_user = %q, want iot_backend_app", appRole)
	}

	// 1. Fixture IDs
	adminActorID := uuid.New()
	nonAdminActorID := uuid.New()
	ownerUserID := uuid.New()
	targetUserID := uuid.New()
	testGatewayID := "gw_u5_nat_" + strings.ReplaceAll(uuid.New().String()[:8], "-", "")
	gatewayEntityURN := "urn:ngsi-ld:Gateway:" + testGatewayID
	now := time.Now().UTC()

	execSetup := func(query string, args ...any) {
		t.Helper()
		if _, err := setupPool.Exec(ctx, query, args...); err != nil {
			t.Fatalf("setup SQL failed: %v\nquery: %s", err, query)
		}
	}

	// 2. Strict cleanup in defer
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		checkedCleanup(t, setupPool, cleanupCtx, "DELETE FROM public.twin_states WHERE entity_id IN (SELECT id FROM public.twin_entities WHERE entity_id = $1)", gatewayEntityURN)
		checkedCleanup(t, setupPool, cleanupCtx, "DELETE FROM public.twin_entities WHERE entity_id = $1", gatewayEntityURN)
		checkedCleanup(t, setupPool, cleanupCtx, "DELETE FROM public.user_gateways WHERE gateway_id = $1", testGatewayID)
		checkedCleanup(t, setupPool, cleanupCtx, "DELETE FROM public.gateways WHERE gateway_id = $1", testGatewayID)
		checkedCleanup(t, setupPool, cleanupCtx, "DELETE FROM public.platform_admins WHERE user_id = $1", adminActorID)
		checkedCleanup(t, setupPool, cleanupCtx, "DELETE FROM public.profiles WHERE id IN ($1, $2, $3, $4)", adminActorID, nonAdminActorID, ownerUserID, targetUserID)
		checkedCleanup(t, setupPool, cleanupCtx, "DELETE FROM auth.users WHERE id IN ($1, $2, $3, $4)", adminActorID, nonAdminActorID, ownerUserID, targetUserID)
	}()

	// 3. Seed users into auth.users (trigger on_auth_user_created automatically creates profiles)
	execSetup(`INSERT INTO auth.users (id, email, raw_user_meta_data, created_at, aud, role) 
		VALUES ($1, 'admin_u5@test.local', '{"full_name":"Admin U5"}', $5, 'authenticated', 'authenticated'),
		       ($2, 'nonadmin_u5@test.local', '{"full_name":"NonAdmin U5"}', $5, 'authenticated', 'authenticated'),
		       ($3, 'owner_u5@test.local', '{"full_name":"Owner U5"}', $5, 'authenticated', 'authenticated'),
		       ($4, 'target_u5@test.local', '{"full_name":"Target U5"}', $5, 'authenticated', 'authenticated')`,
		adminActorID, nonAdminActorID, ownerUserID, targetUserID, now,
	)

	// Make adminActorID platform_admin
	execSetup(`INSERT INTO public.platform_admins (user_id) VALUES ($1)`, adminActorID)

	// Seed Gateway, owner membership in user_gateways, twin entity and twin state
	execSetup(`INSERT INTO public.gateways (gateway_id, name, description, created_at) VALUES ($1, 'U5 Gateway', 'U5 Test Gateway', $2)`,
		testGatewayID, now)
	execSetup(`INSERT INTO public.user_gateways (user_id, gateway_id, role, created_at) VALUES ($1, $2, 'owner', $3)`,
		ownerUserID, testGatewayID, now)

	var twinEntityID uuid.UUID
	err = setupPool.QueryRow(ctx,
		`INSERT INTO public.twin_entities (entity_id, entity_type, gateway_id, name, created_at, updated_at)
		 VALUES ($1, 'Gateway', $2, 'U5 Gateway', $3, $3) RETURNING id`,
		gatewayEntityURN, testGatewayID, now,
	).Scan(&twinEntityID)
	if err != nil {
		t.Fatalf("seed twin_entities failed: %v", err)
	}

	execSetup(`INSERT INTO public.twin_states (entity_id, updated_at) VALUES ($1, $2)`, twinEntityID, now)

	// 4. Instantiate real usermanagement.Service, gateway.Repository, and digitaltwin readers
	authAdapter, err := usermanagement.NewSupabaseAuthAdapter(usermanagement.SupabaseAuthAdapterConfig{
		BaseURL:        authBaseURL,
		ServiceRoleKey: serviceRoleKey,
		Timeout:        10 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewSupabaseAuthAdapter failed: %v", err)
	}

	repo, err := usermanagement.NewPostgresRepository(backendPool)
	if err != nil {
		t.Fatalf("NewPostgresRepository failed: %v", err)
	}

	service, err := usermanagement.NewService(authAdapter, repo, 15*time.Second)
	if err != nil {
		t.Fatalf("NewService failed: %v", err)
	}

	gwRepo, err := gateway.NewPostgresRepository(backendPool)
	if err != nil {
		t.Fatalf("gateway.NewPostgresRepository failed: %v", err)
	}

	twinResolver, err := digitaltwin.NewPostgresResolver(backendPool)
	if err != nil {
		t.Fatalf("digitaltwin.NewPostgresResolver failed: %v", err)
	}

	twinReader, err := digitaltwin.NewPostgresTwinReader(backendPool)
	if err != nil {
		t.Fatalf("digitaltwin.NewPostgresTwinReader failed: %v", err)
	}

	// 5. Baseline: target user has NO membership
	t.Run("Stage 0: baseline reader access before assignment", func(t *testing.T) {
		// Gateway reader returns 0 gateways
		gwList, err := gwRepo.ListGatewaysForUser(ctx, targetUserID)
		if err != nil {
			t.Fatalf("ListGatewaysForUser failed: %v", err)
		}
		if len(gwList) != 0 {
			t.Fatalf("expected 0 gateways for target before assignment, got %d", len(gwList))
		}

		// Gateway role query returns ErrNotFound
		_, err = gwRepo.GetGatewayRole(ctx, targetUserID, testGatewayID)
		if !errors.Is(err, gateway.ErrNotFound) {
			t.Fatalf("expected ErrNotFound from GetGatewayRole before assignment, got %v", err)
		}

		// Twin resolver returns ErrNotFound
		_, err = twinResolver.ResolveGateway(ctx, targetUserID, testGatewayID)
		if !errors.Is(err, digitaltwin.ErrNotFound) {
			t.Fatalf("expected ErrNotFound from ResolveGateway before assignment, got %v", err)
		}

		// Twin reader returns 0 entities
		res, err := twinReader.ListEntities(ctx, targetUserID, digitaltwin.TwinListQuery{})
		if err != nil {
			t.Fatalf("ListEntities failed: %v", err)
		}
		if len(res.Items) != 0 {
			t.Fatalf("expected 0 twin items before assignment, got %d", len(res.Items))
		}
	})

	var createdAt1 time.Time

	// 6. Action 1: Assign operator
	t.Run("Action 1: assign operator to target user with no prior membership", func(t *testing.T) {
		err := service.AssignGatewayUser(ctx, adminActorID, testGatewayID, targetUserID, "operator")
		if err != nil {
			t.Fatalf("AssignGatewayUser operator failed: %v", err)
		}

		var role string
		err = setupPool.QueryRow(ctx,
			"SELECT role, created_at FROM public.user_gateways WHERE gateway_id = $1 AND user_id = $2",
			testGatewayID, targetUserID,
		).Scan(&role, &createdAt1)
		if err != nil {
			t.Fatalf("query user_gateways failed: %v", err)
		}
		if role != "operator" {
			t.Fatalf("role in DB = %q, want 'operator'", role)
		}
		if createdAt1.IsZero() {
			t.Fatal("expected non-zero created_at for new membership")
		}

		// Next-read reader verification
		gwList, err := gwRepo.ListGatewaysForUser(ctx, targetUserID)
		if err != nil || len(gwList) != 1 || gwList[0].GatewayID != testGatewayID || gwList[0].Role != gateway.RoleOperator {
			t.Fatalf("ListGatewaysForUser unexpected after operator assignment: %+v, err=%v", gwList, err)
		}

		gwRole, err := gwRepo.GetGatewayRole(ctx, targetUserID, testGatewayID)
		if err != nil || gwRole != gateway.RoleOperator {
			t.Fatalf("GetGatewayRole unexpected after operator assignment: role=%q, err=%v", gwRole, err)
		}

		resolved, err := twinResolver.ResolveGateway(ctx, targetUserID, testGatewayID)
		if err != nil || resolved.GatewayID != testGatewayID {
			t.Fatalf("ResolveGateway unexpected after operator assignment: resolved=%+v, err=%v", resolved, err)
		}

		twinList, err := twinReader.ListEntities(ctx, targetUserID, digitaltwin.TwinListQuery{})
		if err != nil || len(twinList.Items) != 1 || twinList.Items[0].ID != gatewayEntityURN {
			t.Fatalf("ListEntities unexpected after operator assignment: %+v, err=%v", twinList, err)
		}
	})

	// 7. Action 2: Exact no-op preserving created_at
	t.Run("Action 2: exact operator assignment no-op preserves created_at", func(t *testing.T) {
		err := service.AssignGatewayUser(ctx, adminActorID, testGatewayID, targetUserID, "operator")
		if err != nil {
			t.Fatalf("AssignGatewayUser operator no-op failed: %v", err)
		}

		var role string
		var createdAt2 time.Time
		err = setupPool.QueryRow(ctx,
			"SELECT role, created_at FROM public.user_gateways WHERE gateway_id = $1 AND user_id = $2",
			testGatewayID, targetUserID,
		).Scan(&role, &createdAt2)
		if err != nil {
			t.Fatalf("query user_gateways failed: %v", err)
		}
		if role != "operator" {
			t.Fatalf("role = %q, want 'operator'", role)
		}
		if !createdAt2.Equal(createdAt1) {
			t.Fatalf("created_at modified on exact no-op: before=%v, after=%v", createdAt1, createdAt2)
		}
	})

	// 8. Action 3: Change role to viewer
	t.Run("Action 3: change role from operator to viewer preserving created_at", func(t *testing.T) {
		err := service.AssignGatewayUser(ctx, adminActorID, testGatewayID, targetUserID, "viewer")
		if err != nil {
			t.Fatalf("AssignGatewayUser viewer failed: %v", err)
		}

		var role string
		var createdAt3 time.Time
		err = setupPool.QueryRow(ctx,
			"SELECT role, created_at FROM public.user_gateways WHERE gateway_id = $1 AND user_id = $2",
			testGatewayID, targetUserID,
		).Scan(&role, &createdAt3)
		if err != nil {
			t.Fatalf("query user_gateways failed: %v", err)
		}
		if role != "viewer" {
			t.Fatalf("role in DB = %q, want 'viewer'", role)
		}
		if !createdAt3.Equal(createdAt1) {
			t.Fatalf("created_at modified on role update: initial=%v, after=%v", createdAt1, createdAt3)
		}

		// Next-read reader verification reflects viewer
		gwList, err := gwRepo.ListGatewaysForUser(ctx, targetUserID)
		if err != nil || len(gwList) != 1 || gwList[0].Role != gateway.RoleViewer {
			t.Fatalf("ListGatewaysForUser unexpected after viewer assignment: %+v, err=%v", gwList, err)
		}

		gwRole, err := gwRepo.GetGatewayRole(ctx, targetUserID, testGatewayID)
		if err != nil || gwRole != gateway.RoleViewer {
			t.Fatalf("GetGatewayRole unexpected after viewer assignment: role=%q, err=%v", gwRole, err)
		}

		resolved, err := twinResolver.ResolveGateway(ctx, targetUserID, testGatewayID)
		if err != nil || resolved.GatewayID != testGatewayID {
			t.Fatalf("ResolveGateway unexpected after viewer assignment: resolved=%+v, err=%v", resolved, err)
		}
	})

	// 9. Action 4: Exact no-op for viewer
	t.Run("Action 4: exact viewer assignment no-op preserves created_at", func(t *testing.T) {
		err := service.AssignGatewayUser(ctx, adminActorID, testGatewayID, targetUserID, "viewer")
		if err != nil {
			t.Fatalf("AssignGatewayUser viewer no-op failed: %v", err)
		}

		var role string
		var createdAt4 time.Time
		err = setupPool.QueryRow(ctx,
			"SELECT role, created_at FROM public.user_gateways WHERE gateway_id = $1 AND user_id = $2",
			testGatewayID, targetUserID,
		).Scan(&role, &createdAt4)
		if err != nil {
			t.Fatalf("query user_gateways failed: %v", err)
		}
		if role != "viewer" {
			t.Fatalf("role = %q, want 'viewer'", role)
		}
		if !createdAt4.Equal(createdAt1) {
			t.Fatalf("created_at modified on exact viewer no-op: initial=%v, after=%v", createdAt1, createdAt4)
		}
	})

	// 10. Action 5: Denied-write snapshots
	t.Run("Action 5: denied-write snapshots verify zero side effects", func(t *testing.T) {
		type memberRow struct {
			UserID    uuid.UUID
			GatewayID string
			Role      string
			CreatedAt time.Time
		}
		captureSnapshot := func() []memberRow {
			rows, err := setupPool.Query(ctx,
				"SELECT user_id, gateway_id, role, created_at FROM public.user_gateways WHERE gateway_id = $1 ORDER BY user_id ASC",
				testGatewayID)
			if err != nil {
				t.Fatalf("capture snapshot failed: %v", err)
			}
			defer rows.Close()
			var result []memberRow
			for rows.Next() {
				var r memberRow
				if err := rows.Scan(&r.UserID, &r.GatewayID, &r.Role, &r.CreatedAt); err != nil {
					t.Fatalf("scan snapshot failed: %v", err)
				}
				result = append(result, r)
			}
			return result
		}

		initialSnapshot := captureSnapshot()
		if len(initialSnapshot) != 2 { // ownerUserID and targetUserID
			t.Fatalf("expected 2 membership rows before denied operations, got %d", len(initialSnapshot))
		}

		assertSnapshotUnchanged := func(deniedOp string) {
			current := captureSnapshot()
			if len(current) != len(initialSnapshot) {
				t.Fatalf("[%s] membership count changed: before=%d, after=%d", deniedOp, len(initialSnapshot), len(current))
			}
			for i := range current {
				if current[i].UserID != initialSnapshot[i].UserID ||
					current[i].GatewayID != initialSnapshot[i].GatewayID ||
					current[i].Role != initialSnapshot[i].Role ||
					!current[i].CreatedAt.Equal(initialSnapshot[i].CreatedAt) {
					t.Fatalf("[%s] membership row %d changed: before=%+v, after=%+v", deniedOp, i, initialSnapshot[i], current[i])
				}
			}
		}

		// 5.1 Owner body rejected
		err := service.AssignGatewayUser(ctx, adminActorID, testGatewayID, targetUserID, "owner")
		if !errors.Is(err, usermanagement.ErrOwnerRoleNotAllowed) {
			t.Fatalf("expected ErrOwnerRoleNotAllowed, got %v", err)
		}
		assertSnapshotUnchanged("owner body")

		// 5.2 Target is currently owner -> ErrOwnerTransferRequired
		err = service.AssignGatewayUser(ctx, adminActorID, testGatewayID, ownerUserID, "operator")
		if !errors.Is(err, usermanagement.ErrOwnerTransferRequired) {
			t.Fatalf("expected ErrOwnerTransferRequired for owner target, got %v", err)
		}
		assertSnapshotUnchanged("existing owner target")

		// 5.3 Non-admin actor rejected
		err = service.AssignGatewayUser(ctx, nonAdminActorID, testGatewayID, targetUserID, "operator")
		if !errors.Is(err, usermanagement.ErrActorNotAdmin) {
			t.Fatalf("expected ErrActorNotAdmin for non-admin actor, got %v", err)
		}
		assertSnapshotUnchanged("non-admin actor")

		// 5.4 Missing target user rejected
		err = service.AssignGatewayUser(ctx, adminActorID, testGatewayID, uuid.New(), "operator")
		if !errors.Is(err, usermanagement.ErrUserNotFound) {
			t.Fatalf("expected ErrUserNotFound for missing user, got %v", err)
		}
		assertSnapshotUnchanged("missing user")

		// 5.5 Missing gateway rejected
		err = service.AssignGatewayUser(ctx, adminActorID, "gw_nonexistent_999", targetUserID, "operator")
		if !errors.Is(err, usermanagement.ErrGatewayNotFound) {
			t.Fatalf("expected ErrGatewayNotFound for missing gateway, got %v", err)
		}
		assertSnapshotUnchanged("missing gateway")
	})

	// 11. Action 6: Representative rollback
	t.Run("Action 6: representative rollback on context deadline preserves state", func(t *testing.T) {
		cancelCtx, cancelOp := context.WithCancel(ctx)
		cancelOp() // canceled before execution

		err := service.AssignGatewayUser(cancelCtx, adminActorID, testGatewayID, targetUserID, "operator")
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}

		// Verify target is still viewer
		var currentRole string
		err = setupPool.QueryRow(ctx,
			"SELECT role FROM public.user_gateways WHERE gateway_id = $1 AND user_id = $2",
			testGatewayID, targetUserID,
		).Scan(&currentRole)
		if err != nil || currentRole != "viewer" {
			t.Fatalf("role after rollback = %q (err=%v), want 'viewer'", currentRole, err)
		}
	})
}

func TestServiceNative_TransferGatewayOwner_InvariantsAndConcurrency(t *testing.T) {
	setupURL, backendURL, authBaseURL, _, serviceRoleKey := resolveAuthTestConfig(t)
	if setupURL == "" || backendURL == "" {
		t.Fatal("native database test requested but configuration missing")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	setupPool, err := pgxpool.New(ctx, setupURL)
	if err != nil {
		t.Fatalf("failed to connect setup pool: %v", err)
	}
	defer setupPool.Close()

	backendPool, err := pgxpool.New(ctx, backendURL)
	if err != nil {
		t.Fatalf("failed to connect backend pool: %v", err)
	}
	defer backendPool.Close()

	var appRole string
	if err := backendPool.QueryRow(ctx, "SELECT current_user").Scan(&appRole); err != nil || appRole != "iot_backend_app" {
		t.Fatalf("backendPool current_user = %q, want iot_backend_app", appRole)
	}

	// 1. Fixture IDs
	adminActorID := uuid.New()
	nonAdminActorID := uuid.New()
	ownerUserID := uuid.New()
	targetUserID := uuid.New()
	candidate2UserID := uuid.New()
	testGatewayID := "gw_u6_nat_" + strings.ReplaceAll(uuid.New().String()[:8], "-", "")
	sensorID := "sensor_001"
	gatewayEntityURN := "urn:ngsi-ld:Gateway:" + testGatewayID
	sensorEntityURN := "urn:ngsi-ld:Sensor:" + testGatewayID + ":" + sensorID
	now := time.Now().UTC()

	execSetup := func(query string, args ...any) {
		t.Helper()
		if _, err := setupPool.Exec(ctx, query, args...); err != nil {
			t.Fatalf("setup SQL failed: %v\nquery: %s", err, query)
		}
	}

	// 2. Strict cleanup in defer
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		checkedCleanup(t, setupPool, cleanupCtx, "DROP TRIGGER IF EXISTS test_fail_after_demotion_u6_"+testGatewayID+" ON public.user_gateways; DROP FUNCTION IF EXISTS fail_after_demotion_u6_"+testGatewayID+"();")
		checkedCleanup(t, setupPool, cleanupCtx, "DELETE FROM public.twin_states WHERE entity_id IN (SELECT id FROM public.twin_entities WHERE gateway_id = $1)", testGatewayID)
		checkedCleanup(t, setupPool, cleanupCtx, "DELETE FROM public.twin_entities WHERE gateway_id = $1", testGatewayID)
		checkedCleanup(t, setupPool, cleanupCtx, "DELETE FROM public.sensors WHERE gateway_id = $1", testGatewayID)
		checkedCleanup(t, setupPool, cleanupCtx, "DELETE FROM public.gateway_mqtt_credentials WHERE gateway_id = $1", testGatewayID)
		checkedCleanup(t, setupPool, cleanupCtx, "DELETE FROM public.gateway_mqtt_credential_events WHERE gateway_id = $1", testGatewayID)
		checkedCleanup(t, setupPool, cleanupCtx, "DELETE FROM public.user_gateways WHERE gateway_id = $1", testGatewayID)
		checkedCleanup(t, setupPool, cleanupCtx, "DELETE FROM public.gateways WHERE gateway_id = $1", testGatewayID)
		checkedCleanup(t, setupPool, cleanupCtx, "DELETE FROM public.platform_admins WHERE user_id IN ($1, $2)", adminActorID, nonAdminActorID)
		checkedCleanup(t, setupPool, cleanupCtx, "DELETE FROM public.profiles WHERE id IN ($1, $2, $3, $4, $5)", adminActorID, nonAdminActorID, ownerUserID, targetUserID, candidate2UserID)
		checkedCleanup(t, setupPool, cleanupCtx, "DELETE FROM auth.users WHERE id IN ($1, $2, $3, $4, $5)", adminActorID, nonAdminActorID, ownerUserID, targetUserID, candidate2UserID)
	}()

	// 3. Seed users into auth.users (trigger on_auth_user_created automatically creates profiles)
	execSetup(`INSERT INTO auth.users (id, email, raw_user_meta_data, created_at, aud, role) 
		VALUES ($1, 'admin_u6@test.local', '{"full_name":"Admin U6"}', $6, 'authenticated', 'authenticated'),
		       ($2, 'nonadmin_u6@test.local', '{"full_name":"NonAdmin U6"}', $6, 'authenticated', 'authenticated'),
		       ($3, 'owner_u6@test.local', '{"full_name":"Owner U6"}', $6, 'authenticated', 'authenticated'),
		       ($4, 'target_u6@test.local', '{"full_name":"Target U6"}', $6, 'authenticated', 'authenticated'),
		       ($5, 'candidate2_u6@test.local', '{"full_name":"Candidate2 U6"}', $6, 'authenticated', 'authenticated')`,
		adminActorID, nonAdminActorID, ownerUserID, targetUserID, candidate2UserID, now,
	)

	// Make adminActorID platform_admin
	execSetup(`INSERT INTO public.platform_admins (user_id) VALUES ($1)`, adminActorID)

	// Seed Gateway, sensor, owner membership in user_gateways, twin entity and twin state, credentials
	execSetup(`INSERT INTO public.gateways (gateway_id, name, description, created_at) VALUES ($1, 'U6 Native GW', 'U6 Native GW Desc', $2)`,
		testGatewayID, now)
	execSetup(`INSERT INTO public.sensors (gateway_id, sensor_id, name, unit, created_at) VALUES ($1, $2, 'U6 Sensor', 'celsius', $3)`,
		testGatewayID, sensorID, now)
	execSetup(`INSERT INTO public.user_gateways (user_id, gateway_id, role, created_at) VALUES ($1, $2, 'owner', $3)`,
		ownerUserID, testGatewayID, now)

	var gwTwinEntityID uuid.UUID
	err = setupPool.QueryRow(ctx,
		`INSERT INTO public.twin_entities (entity_id, entity_type, gateway_id, name, created_at, updated_at)
		 VALUES ($1, 'Gateway', $2, 'U6 Native GW Entity', $3, $3) RETURNING id`,
		gatewayEntityURN, testGatewayID, now,
	).Scan(&gwTwinEntityID)
	if err != nil {
		t.Fatalf("seed gateway twin_entities failed: %v", err)
	}
	execSetup(`INSERT INTO public.twin_states (entity_id, updated_at) VALUES ($1, $2)`, gwTwinEntityID, now)

	var sensorTwinEntityID uuid.UUID
	err = setupPool.QueryRow(ctx,
		`INSERT INTO public.twin_entities (entity_id, entity_type, gateway_id, name, created_at, updated_at)
		 VALUES ($1, 'Sensor', $2, 'U6 Sensor Entity', $3, $3) RETURNING id`,
		sensorEntityURN, testGatewayID, now,
	).Scan(&sensorTwinEntityID)
	if err != nil {
		t.Fatalf("seed sensor twin_entities failed: %v", err)
	}
	execSetup(`INSERT INTO public.twin_states (entity_id, updated_at) VALUES ($1, $2)`, sensorTwinEntityID, now)

	operationID := uuid.New()
	idempKey := uuid.New().String()
	seedCredTx, err := setupPool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin seedCredTx failed: %v", err)
	}
	defer func() { _ = seedCredTx.Rollback(ctx) }()
	_, err = seedCredTx.Exec(ctx, `
		INSERT INTO public.gateway_mqtt_credential_events
			(operation_id, gateway_id, actor_user_id, request_id, idempotency_key, credential_version, action, status, execution_contract, created_at, updated_at)
		VALUES ($1, $2, $3, 'req-u6-sentinel', $4, 1, 'provision', 'pending', 'simplified', $5, $5)`,
		operationID, testGatewayID, adminActorID, idempKey, now)
	if err != nil {
		t.Fatalf("seed credential event failed: %v", err)
	}
	_, err = seedCredTx.Exec(ctx, `
		INSERT INTO public.gateway_mqtt_credentials
			(gateway_id, credential_version, status, last_operation_id, changed_by, updated_at)
		VALUES ($1, 1, 'provisioning', $2, $3, $4)`,
		testGatewayID, operationID, adminActorID, now)
	if err != nil {
		t.Fatalf("seed credential failed: %v", err)
	}
	if err := seedCredTx.Commit(ctx); err != nil {
		t.Fatalf("commit seed credential failed: %v", err)
	}

	// 4. Instantiate service and readers
	authAdapter, err := usermanagement.NewSupabaseAuthAdapter(usermanagement.SupabaseAuthAdapterConfig{
		BaseURL:        authBaseURL,
		ServiceRoleKey: serviceRoleKey,
		Timeout:        10 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewSupabaseAuthAdapter failed: %v", err)
	}

	repo, err := usermanagement.NewPostgresRepository(backendPool)
	if err != nil {
		t.Fatalf("NewPostgresRepository failed: %v", err)
	}

	service, err := usermanagement.NewService(authAdapter, repo, 15*time.Second)
	if err != nil {
		t.Fatalf("NewService failed: %v", err)
	}

	gwRepo, err := gateway.NewPostgresRepository(backendPool)
	if err != nil {
		t.Fatalf("gateway.NewPostgresRepository failed: %v", err)
	}

	twinResolver, err := digitaltwin.NewPostgresResolver(backendPool)
	if err != nil {
		t.Fatalf("digitaltwin.NewPostgresResolver failed: %v", err)
	}

	twinReader, err := digitaltwin.NewPostgresTwinReader(backendPool)
	if err != nil {
		t.Fatalf("digitaltwin.NewPostgresTwinReader failed: %v", err)
	}

	// 5. Part A: Real SQL failure AFTER old-owner demotion must rollback BOTH membership writes
	t.Run("Part A: real SQL failure after old-owner demotion rolls back both membership writes", func(t *testing.T) {
		type snapshotState struct {
			gwName        string
			gwDesc        *string
			sensorName    string
			sensorUnit    string
			credStatus    string
			isAdmin       bool
			ownerRole     string
			targetHasRole bool
		}

		captureSnapshot := func() snapshotState {
			var s snapshotState
			err := setupPool.QueryRow(ctx, "SELECT name, description FROM public.gateways WHERE gateway_id = $1", testGatewayID).Scan(&s.gwName, &s.gwDesc)
			if err != nil {
				t.Fatalf("query gateway failed: %v", err)
			}
			err = setupPool.QueryRow(ctx, "SELECT name, unit FROM public.sensors WHERE gateway_id = $1 AND sensor_id = $2", testGatewayID, sensorID).Scan(&s.sensorName, &s.sensorUnit)
			if err != nil {
				t.Fatalf("query sensor failed: %v", err)
			}
			err = setupPool.QueryRow(ctx, "SELECT status FROM public.gateway_mqtt_credentials WHERE gateway_id = $1", testGatewayID).Scan(&s.credStatus)
			if err != nil {
				t.Fatalf("query credential failed: %v", err)
			}
			err = setupPool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM public.platform_admins WHERE user_id = $1)", adminActorID).Scan(&s.isAdmin)
			if err != nil {
				t.Fatalf("query platform admin failed: %v", err)
			}
			err = setupPool.QueryRow(ctx, "SELECT role FROM public.user_gateways WHERE gateway_id = $1 AND user_id = $2", testGatewayID, ownerUserID).Scan(&s.ownerRole)
			if err != nil {
				t.Fatalf("query owner role failed: %v", err)
			}
			err = setupPool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM public.user_gateways WHERE gateway_id = $1 AND user_id = $2)", testGatewayID, targetUserID).Scan(&s.targetHasRole)
			if err != nil {
				t.Fatalf("query target role existence failed: %v", err)
			}
			return s
		}

		beforeSnap := captureSnapshot()
		if beforeSnap.ownerRole != "owner" || beforeSnap.targetHasRole {
			t.Fatalf("unexpected pre-state: %+v", beforeSnap)
		}

		// Install trigger that fails specifically on target owner promotion
		triggerFuncSQL := fmt.Sprintf(`CREATE OR REPLACE FUNCTION fail_after_demotion_u6_%s() RETURNS TRIGGER AS $$
		BEGIN
			IF NEW.gateway_id = '%s' AND NEW.user_id = '%s' AND NEW.role = 'owner' THEN
				RAISE EXCEPTION 'simulated sql failure during target owner promotion' USING ERRCODE = 'check_violation';
			END IF;
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;`, testGatewayID, testGatewayID, targetUserID)
		execSetup(triggerFuncSQL)

		execSetup(`CREATE TRIGGER test_fail_after_demotion_u6_` + testGatewayID + `
			BEFORE INSERT OR UPDATE ON public.user_gateways
			FOR EACH ROW EXECUTE FUNCTION fail_after_demotion_u6_` + testGatewayID + `();`)

		// Attempt transfer: demotes owner to viewer, then attempts target promotion which triggers the real SQL failure
		err := service.TransferGatewayOwner(ctx, adminActorID, testGatewayID, targetUserID)
		if err == nil {
			t.Fatal("expected error from trigger-aborted owner transfer, got nil")
		}

		// Drop trigger immediately
		execSetup(`DROP TRIGGER IF EXISTS test_fail_after_demotion_u6_` + testGatewayID + ` ON public.user_gateways;
			   DROP FUNCTION IF EXISTS fail_after_demotion_u6_` + testGatewayID + `();`)

		afterSnap := captureSnapshot()

		// Verify BOTH writes rolled back:
		// 1) old owner was NOT demoted to viewer, remains owner
		if afterSnap.ownerRole != "owner" {
			t.Fatalf("demotion write was not rolled back: owner role is %q, want 'owner'", afterSnap.ownerRole)
		}
		// 2) target was NOT inserted/promoted, has no membership
		if afterSnap.targetHasRole {
			t.Fatalf("promotion write was not rolled back: target has membership")
		}

		// Verify snapshot equivalence across all owned resource/global role/state/credential content
		if afterSnap.gwName != beforeSnap.gwName ||
			(afterSnap.gwDesc == nil && beforeSnap.gwDesc != nil) ||
			(afterSnap.gwDesc != nil && beforeSnap.gwDesc == nil) ||
			(afterSnap.gwDesc != nil && *afterSnap.gwDesc != *beforeSnap.gwDesc) ||
			afterSnap.sensorName != beforeSnap.sensorName ||
			afterSnap.sensorUnit != beforeSnap.sensorUnit ||
			afterSnap.credStatus != beforeSnap.credStatus ||
			afterSnap.isAdmin != beforeSnap.isAdmin {
			t.Fatalf("snapshot changed after rollback: before=%+v, after=%+v", beforeSnap, afterSnap)
		}
	})

	// 6. Part B: Successful transfer and reader reflection
	t.Run("Part B: successful transfer updates roles and reflects across existing readers", func(t *testing.T) {
		err := service.TransferGatewayOwner(ctx, adminActorID, testGatewayID, targetUserID)
		if err != nil {
			t.Fatalf("TransferGatewayOwner failed: %v", err)
		}

		// In DB: old owner is now viewer, target is now owner
		var oldRole string
		err = setupPool.QueryRow(ctx, "SELECT role FROM public.user_gateways WHERE gateway_id = $1 AND user_id = $2", testGatewayID, ownerUserID).Scan(&oldRole)
		if err != nil || oldRole != "viewer" {
			t.Fatalf("old owner role = %q (err=%v), want viewer", oldRole, err)
		}

		var newRole string
		err = setupPool.QueryRow(ctx, "SELECT role FROM public.user_gateways WHERE gateway_id = $1 AND user_id = $2", testGatewayID, targetUserID).Scan(&newRole)
		if err != nil || newRole != "owner" {
			t.Fatalf("new owner role = %q (err=%v), want owner", newRole, err)
		}

		var totalOwners int
		err = setupPool.QueryRow(ctx, "SELECT count(*) FROM public.user_gateways WHERE gateway_id = $1 AND role = 'owner'", testGatewayID).Scan(&totalOwners)
		if err != nil || totalOwners != 1 {
			t.Fatalf("total owners = %d, want exactly 1", totalOwners)
		}

		// Reader reflection:
		// Target has RoleOwner
		role, err := gwRepo.GetGatewayRole(ctx, targetUserID, testGatewayID)
		if err != nil || role != gateway.RoleOwner {
			t.Fatalf("target Role = %q (err=%v), want RoleOwner", role, err)
		}

		// Old owner has RoleViewer (retains read access per contract)
		roleOld, err := gwRepo.GetGatewayRole(ctx, ownerUserID, testGatewayID)
		if err != nil || roleOld != gateway.RoleViewer {
			t.Fatalf("old owner Role = %q (err=%v), want RoleViewer", roleOld, err)
		}

		// Twin resolver resolves for both
		if _, err := twinResolver.ResolveGateway(ctx, targetUserID, testGatewayID); err != nil {
			t.Fatalf("ResolveGateway failed for new owner: %v", err)
		}
		if _, err := twinResolver.ResolveGateway(ctx, ownerUserID, testGatewayID); err != nil {
			t.Fatalf("ResolveGateway failed for old owner (now viewer): %v", err)
		}

		// Twin list contains both gateway and sensor entities for both
		tListNew, err := twinReader.ListEntities(ctx, targetUserID, digitaltwin.TwinListQuery{})
		if err != nil || len(tListNew.Items) != 2 {
			t.Fatalf("ListEntities for new owner unexpected: %+v, err=%v", tListNew, err)
		}
		tListOld, err := twinReader.ListEntities(ctx, ownerUserID, digitaltwin.TwinListQuery{})
		if err != nil || len(tListOld.Items) != 2 {
			t.Fatalf("ListEntities for old owner unexpected: %+v, err=%v", tListOld, err)
		}
	})

	// 7. Part C: Coordinated concurrency test U5 versus Transfer: no demotion of newly promoted owner
	t.Run("Part C: coordinated concurrency U5 versus transfer prevents demotion of newly promoted owner", func(t *testing.T) {
		// Currently on testGatewayID: targetUserID is owner.
		// Transfer will promote candidate2UserID to owner and demote targetUserID to viewer.
		// Concurrently, U5 attempts to assign candidate2UserID as 'operator'.
		// Coordinated flow:
		// Tx1 opens a transaction, locks Gateway FOR UPDATE.
		// Goroutine 2 calls service.AssignGatewayUser for candidate2UserID as operator.
		// Goroutine 2 blocks on Gateway FOR UPDATE.
		// Tx1 demotes targetUserID to viewer, inserts/promotes candidate2UserID to owner, and commits.
		// Goroutine 2 unblocks, checks candidate2UserID's role, discovers it is now 'owner',
		// and returns ErrOwnerTransferRequired!
		// candidate2UserID is NOT demoted!

		tx1, err := backendPool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin tx1 failed: %v", err)
		}
		defer func() { _ = tx1.Rollback(ctx) }()

		// Step 1: Tx1 locks Gateway FOR UPDATE
		var gwName string
		err = tx1.QueryRow(ctx, "SELECT name FROM public.gateways WHERE gateway_id = $1 FOR UPDATE", testGatewayID).Scan(&gwName)
		if err != nil {
			t.Fatalf("tx1 lock gateway failed: %v", err)
		}

		// Step 2: Start Goroutine 2 attempting U5 AssignGatewayUser to candidate2UserID
		u5ErrCh := make(chan error, 1)
		go func() {
			u5ErrCh <- service.AssignGatewayUser(ctx, adminActorID, testGatewayID, candidate2UserID, "operator")
		}()

		// Step 3: Observe lock contention in pg_locks: U5 is waiting for Gateway row lock
		var lockWaitObserved bool
		for i := 0; i < 50; i++ {
			var ungrantedCount int
			_ = setupPool.QueryRow(ctx, "SELECT count(*) FROM pg_locks WHERE NOT granted").Scan(&ungrantedCount)
			if ungrantedCount > 0 {
				lockWaitObserved = true
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		if !lockWaitObserved {
			t.Fatal("expected U5 AssignGatewayUser to be blocked on Gateway row lock")
		}

		// Step 4: Tx1 demotes old owner (targetUserID) and promotes candidate2UserID to owner, then commits
		_, err = tx1.Exec(ctx, "UPDATE public.user_gateways SET role = 'viewer' WHERE gateway_id = $1 AND user_id = $2", testGatewayID, targetUserID)
		if err != nil {
			t.Fatalf("tx1 demote old owner failed: %v", err)
		}
		_, err = tx1.Exec(ctx, "INSERT INTO public.user_gateways (user_id, gateway_id, role) VALUES ($1, $2, 'owner')", candidate2UserID, testGatewayID)
		if err != nil {
			t.Fatalf("tx1 promote new owner failed: %v", err)
		}
		err = tx1.Commit(ctx)
		if err != nil {
			t.Fatalf("tx1 commit failed: %v", err)
		}

		// Step 5: Wait for U5 goroutine to finish
		select {
		case u5Err := <-u5ErrCh:
			if !errors.Is(u5Err, usermanagement.ErrOwnerTransferRequired) {
				t.Fatalf("expected ErrOwnerTransferRequired from U5 on newly promoted owner, got: %v", u5Err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for U5 AssignGatewayUser after lock release")
		}

		// Step 6: Verify candidate2UserID is STILL owner (no demotion occurred!)
		var finalRole string
		err = setupPool.QueryRow(ctx, "SELECT role FROM public.user_gateways WHERE gateway_id = $1 AND user_id = $2", testGatewayID, candidate2UserID).Scan(&finalRole)
		if err != nil || finalRole != "owner" {
			t.Fatalf("candidate2 role in DB = %q (err=%v), want owner", finalRole, err)
		}

		var totalOwners int
		err = setupPool.QueryRow(ctx, "SELECT count(*) FROM public.user_gateways WHERE gateway_id = $1 AND role = 'owner'", testGatewayID).Scan(&totalOwners)
		if err != nil || totalOwners != 1 {
			t.Fatalf("total owners = %d, want exactly 1", totalOwners)
		}
	})

	// 8. Part D: Two transfers serialize without two owners
	t.Run("Part D: two concurrent transfers serialize without two owners", func(t *testing.T) {
		// Currently candidate2UserID is owner.
		// Concurrently transfer to targetUserID and transfer to ownerUserID.
		errCh1 := make(chan error, 1)
		errCh2 := make(chan error, 1)

		go func() {
			errCh1 <- service.TransferGatewayOwner(ctx, adminActorID, testGatewayID, targetUserID)
		}()
		go func() {
			errCh2 <- service.TransferGatewayOwner(ctx, adminActorID, testGatewayID, ownerUserID)
		}()

		select {
		case err1 := <-errCh1:
			if err1 != nil {
				t.Fatalf("transfer 1 failed: %v", err1)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("transfer 1 timed out")
		}

		select {
		case err2 := <-errCh2:
			if err2 != nil {
				t.Fatalf("transfer 2 failed: %v", err2)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("transfer 2 timed out")
		}

		// Verify exactly one owner exists in user_gateways
		var finalOwnerCount int
		err = setupPool.QueryRow(ctx, "SELECT count(*) FROM public.user_gateways WHERE gateway_id = $1 AND role = 'owner'", testGatewayID).Scan(&finalOwnerCount)
		if err != nil || finalOwnerCount != 1 {
			t.Fatalf("expected exactly 1 owner after serialized transfers, got %d (err=%v)", finalOwnerCount, err)
		}
	})
}
