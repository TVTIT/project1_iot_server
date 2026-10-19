package httpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/auth"
	"iot-platform/internal/usermanagement"
)

type fakeUserCreator struct {
	calls int
	fn    func(ctx context.Context, actorID uuid.UUID, input usermanagement.CreateUserInput) (usermanagement.CreatedUser, error)
}

func (f *fakeUserCreator) CreateUser(ctx context.Context, actorID uuid.UUID, input usermanagement.CreateUserInput) (usermanagement.CreatedUser, error) {
	f.calls++
	if f.fn != nil {
		return f.fn(ctx, actorID, input)
	}
	return usermanagement.CreatedUser{}, nil
}

type fakeUserLister struct {
	calls int
	fn    func(ctx context.Context, actorID uuid.UUID) ([]usermanagement.UserSummary, error)
}

func (f *fakeUserLister) ListUsers(ctx context.Context, actorID uuid.UUID) ([]usermanagement.UserSummary, error) {
	f.calls++
	if f.fn != nil {
		return f.fn(ctx, actorID)
	}
	return []usermanagement.UserSummary{}, nil
}

type fakeUserUpdater struct {
	calls int
	fn    func(ctx context.Context, actorID, targetID uuid.UUID, input usermanagement.UpdateUserInput) (usermanagement.UpdatedUser, error)
}

func (f *fakeUserUpdater) UpdateUser(ctx context.Context, actorID, targetID uuid.UUID, input usermanagement.UpdateUserInput) (usermanagement.UpdatedUser, error) {
	f.calls++
	if f.fn != nil {
		return f.fn(ctx, actorID, targetID, input)
	}
	return usermanagement.UpdatedUser{}, nil
}

type fakeGatewayUserAssigner struct {
	calls int
	fn    func(ctx context.Context, actorID uuid.UUID, gatewayID string, targetID uuid.UUID, role string) error
}

func (f *fakeGatewayUserAssigner) AssignGatewayUser(ctx context.Context, actorID uuid.UUID, gatewayID string, targetID uuid.UUID, role string) error {
	f.calls++
	if f.fn != nil {
		return f.fn(ctx, actorID, gatewayID, targetID, role)
	}
	return nil
}

type fakeGatewayOwnerTransferer struct {
	calls int
	fn    func(ctx context.Context, actorID uuid.UUID, gatewayID string, targetID uuid.UUID) error
}

func (f *fakeGatewayOwnerTransferer) TransferGatewayOwner(ctx context.Context, actorID uuid.UUID, gatewayID string, targetID uuid.UUID) error {
	f.calls++
	if f.fn != nil {
		return f.fn(ctx, actorID, gatewayID, targetID)
	}
	return nil
}

func testUserManagementRouter(t *testing.T, mutate func(*RouterDependencies)) (*httptest.Server, *fakeUserCreator, *fakeUserLister, *fakeUserUpdater, *fakeGatewayUserAssigner, *fakeGatewayOwnerTransferer) {
	t.Helper()
	creator := &fakeUserCreator{}
	lister := &fakeUserLister{}
	updater := &fakeUserUpdater{}
	assigner := &fakeGatewayUserAssigner{}
	ownerTransferer := &fakeGatewayOwnerTransferer{}

	deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
	deps.UserCreator = creator
	deps.UserLister = lister
	deps.UserUpdater = updater
	deps.GatewayUserAssigner = assigner
	deps.GatewayOwnerTransferer = ownerTransferer

	if mutate != nil {
		mutate(&deps)
	}

	router, err := NewRouter(deps)
	if err != nil {
		t.Fatalf("NewRouter failed: %v", err)
	}
	server := httptest.NewServer(router)
	t.Cleanup(server.Close)
	return server, creator, lister, updater, assigner, ownerTransferer
}

// 1. POST /v1/admin/users tests
func TestCreateUserHandler(t *testing.T) {
	t.Run("happy path creates user and returns 201 with Location header", func(t *testing.T) {
		newID := uuid.New()
		now := time.Now().UTC()
		server, creator, _, _, _, _ := testUserManagementRouter(t, nil)
		creator.fn = func(_ context.Context, _ uuid.UUID, input usermanagement.CreateUserInput) (usermanagement.CreatedUser, error) {
			if input.FullName != "Nguyen Van A" || input.Email != "a@example.com" || input.Password != "password123" {
				t.Fatalf("unexpected input to creator: %+v", input)
			}
			return usermanagement.CreatedUser{
				ID:           newID,
				Email:        input.Email,
				FullName:     input.FullName,
				PlatformRole: "user",
				CreatedAt:    now,
			}, nil
		}

		payload := `{"full_name":"Nguyen Van A","email":"a@example.com","password":"password123"}`
		req, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/admin/users", strings.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer token")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusCreated)
		}
		expectedLocation := fmt.Sprintf("/v1/admin/users/%s", newID)
		if got := resp.Header.Get("Location"); got != expectedLocation {
			t.Fatalf("Location = %q, want %q", got, expectedLocation)
		}
		if got := resp.Header.Get("Cache-Control"); got != "no-store" {
			t.Fatalf("Cache-Control = %q, want no-store", got)
		}

		var res usermanagement.CreatedUser
		if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}
		if res.ID != newID || res.Email != "a@example.com" || res.PlatformRole != "user" {
			t.Fatalf("unexpected response body: %+v", res)
		}
	})

	t.Run("duplicate email returns 409 user_already_exists", func(t *testing.T) {
		server, creator, _, _, _, _ := testUserManagementRouter(t, nil)
		creator.fn = func(_ context.Context, _ uuid.UUID, _ usermanagement.CreateUserInput) (usermanagement.CreatedUser, error) {
			return usermanagement.CreatedUser{}, usermanagement.ErrUserAlreadyExists
		}

		payload := `{"full_name":"Duplicate","email":"dup@example.com","password":"password123"}`
		req, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/admin/users", strings.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer token")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusConflict {
			t.Fatalf("status = %d, want 409", resp.StatusCode)
		}
		var errEnvelope struct {
			Error struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&errEnvelope)
		if errEnvelope.Error.Code != "user_already_exists" {
			t.Fatalf("error code = %q, want user_already_exists", errEnvelope.Error.Code)
		}
	})

	t.Run("auth unavailable returns 503 service_unavailable", func(t *testing.T) {
		server, creator, _, _, _, _ := testUserManagementRouter(t, nil)
		creator.fn = func(_ context.Context, _ uuid.UUID, _ usermanagement.CreateUserInput) (usermanagement.CreatedUser, error) {
			return usermanagement.CreatedUser{}, usermanagement.ErrAuthUnavailable
		}

		payload := `{"full_name":"New User","email":"new@example.com","password":"password123"}`
		req, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/admin/users", strings.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer token")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 503", resp.StatusCode)
		}
		var errEnvelope struct {
			Error struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&errEnvelope)
		if errEnvelope.Error.Code != "service_unavailable" {
			t.Fatalf("error code = %q, want service_unavailable", errEnvelope.Error.Code)
		}
	})

	t.Run("profile verification failed returns 503 service_unavailable and never returns 201", func(t *testing.T) {
		targetID := uuid.New()
		server, creator, _, _, _, _ := testUserManagementRouter(t, nil)
		creator.fn = func(_ context.Context, _ uuid.UUID, _ usermanagement.CreateUserInput) (usermanagement.CreatedUser, error) {
			return usermanagement.CreatedUser{}, &usermanagement.ProfileVerificationError{
				TargetID: targetID,
				Cause:    usermanagement.ErrInconsistentProfile,
			}
		}

		payload := `{"full_name":"New User","email":"new@example.com","password":"password123"}`
		req, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/admin/users", strings.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer token")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode == http.StatusCreated {
			t.Fatal("must not return 201 Created on profile verification failure")
		}
		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 503", resp.StatusCode)
		}
		var errEnvelope struct {
			Error struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&errEnvelope)
		if errEnvelope.Error.Code != "service_unavailable" {
			t.Fatalf("error code = %q, want service_unavailable", errEnvelope.Error.Code)
		}
	})

	t.Run("actor not admin returns 403 forbidden", func(t *testing.T) {
		server, creator, _, _, _, _ := testUserManagementRouter(t, nil)
		creator.fn = func(_ context.Context, _ uuid.UUID, _ usermanagement.CreateUserInput) (usermanagement.CreatedUser, error) {
			return usermanagement.CreatedUser{}, usermanagement.ErrActorNotAdmin
		}

		payload := `{"full_name":"New User","email":"new@example.com","password":"password123"}`
		req, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/admin/users", strings.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer token")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("status = %d, want 403", resp.StatusCode)
		}
		var errEnvelope struct {
			Error struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&errEnvelope)
		if errEnvelope.Error.Code != "forbidden" {
			t.Fatalf("error code = %q, want forbidden", errEnvelope.Error.Code)
		}
	})

	t.Run("password is never echoed in success or error responses", func(t *testing.T) {
		secretPassword := "SuperSecretPassword123!"
		newID := uuid.New()
		server, creator, _, _, _, _ := testUserManagementRouter(t, nil)
		creator.fn = func(_ context.Context, _ uuid.UUID, input usermanagement.CreateUserInput) (usermanagement.CreatedUser, error) {
			return usermanagement.CreatedUser{
				ID:           newID,
				Email:        input.Email,
				FullName:     input.FullName,
				PlatformRole: "user",
				CreatedAt:    time.Now().UTC(),
			}, nil
		}

		// 1. Success response
		payload := fmt.Sprintf(`{"full_name":"Nguyen Van A","email":"test@example.com","password":"%s"}`, secretPassword)
		req, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/admin/users", strings.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer token")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		bodyBytes, _ := io.ReadAll(resp.Body)
		if strings.Contains(string(bodyBytes), secretPassword) {
			t.Fatalf("password leaked in success response: %s", string(bodyBytes))
		}

		// 2. Error response
		creator.fn = func(_ context.Context, _ uuid.UUID, _ usermanagement.CreateUserInput) (usermanagement.CreatedUser, error) {
			return usermanagement.CreatedUser{}, usermanagement.ErrUserAlreadyExists
		}
		reqErr, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/admin/users", strings.NewReader(payload))
		reqErr.Header.Set("Content-Type", "application/json")
		reqErr.Header.Set("Authorization", "Bearer token")

		respErr, err := http.DefaultClient.Do(reqErr)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer func() { _ = respErr.Body.Close() }()

		errBodyBytes, _ := io.ReadAll(respErr.Body)
		if strings.Contains(string(errBodyBytes), secretPassword) {
			t.Fatalf("password leaked in error response: %s", string(errBodyBytes))
		}
	})

	t.Run("outside whitespace in email is trimmed and case preserved", func(t *testing.T) {
		server, creator, _, _, _, _ := testUserManagementRouter(t, nil)
		var receivedEmail string
		creator.fn = func(_ context.Context, _ uuid.UUID, input usermanagement.CreateUserInput) (usermanagement.CreatedUser, error) {
			receivedEmail = input.Email
			return usermanagement.CreatedUser{
				ID:           uuid.New(),
				Email:        input.Email,
				FullName:     input.FullName,
				PlatformRole: "user",
				CreatedAt:    time.Now().UTC(),
			}, nil
		}

		payload := `{"full_name":"Nguyen Van A","email":"   MyUser.Account@Domain.COM   ","password":"password123"}`
		req, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/admin/users", strings.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer token")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("status = %d, want 201", resp.StatusCode)
		}
		if receivedEmail != "MyUser.Account@Domain.COM" {
			t.Fatalf("receivedEmail = %q, want %q (outside whitespace trimmed, case preserved)", receivedEmail, "MyUser.Account@Domain.COM")
		}
	})

	t.Run("strict JSON rejections", func(t *testing.T) {
		cases := []struct {
			name        string
			contentType string
			body        string
			urlSuffix   string
			wantStatus  int
			wantCode    string
		}{
			{
				name:        "missing content-type",
				contentType: "",
				body:        `{"full_name":"A","email":"a@b.com","password":"p12345"}`,
				wantStatus:  415,
				wantCode:    "unsupported_media_type",
			},
			{
				name:        "wrong media type",
				contentType: "text/plain",
				body:        `{"full_name":"A","email":"a@b.com","password":"p12345"}`,
				wantStatus:  415,
				wantCode:    "unsupported_media_type",
			},
			{
				name:        "unknown field",
				contentType: "application/json",
				body:        `{"full_name":"A","email":"a@b.com","password":"p12345","extra":"bad"}`,
				wantStatus:  400,
				wantCode:    "invalid_request",
			},
			{
				name:        "duplicate key",
				contentType: "application/json",
				body:        `{"full_name":"A","full_name":"B","email":"a@b.com","password":"p12345"}`,
				wantStatus:  400,
				wantCode:    "invalid_request",
			},
			{
				name:        "trailing json tokens",
				contentType: "application/json",
				body:        `{"full_name":"A","email":"a@b.com","password":"p12345"} {"extra":1}`,
				wantStatus:  400,
				wantCode:    "invalid_request",
			},
			{
				name:        "null non-nullable full_name",
				contentType: "application/json",
				body:        `{"full_name":null,"email":"a@b.com","password":"p12345"}`,
				wantStatus:  400,
				wantCode:    "invalid_request",
			},
			{
				name:        "query string prohibited",
				contentType: "application/json",
				body:        `{"full_name":"A","email":"a@b.com","password":"p12345"}`,
				urlSuffix:   "?test=1",
				wantStatus:  400,
				wantCode:    "invalid_request",
			},
			{
				name:        "short password",
				contentType: "application/json",
				body:        `{"full_name":"A","email":"a@b.com","password":"123"}`,
				wantStatus:  400,
				wantCode:    "invalid_request",
			},
			{
				name:        "invalid surrogate in json",
				contentType: "application/json",
				body:        `{"full_name":"\uD800","email":"a@b.com","password":"p12345"}`,
				wantStatus:  400,
				wantCode:    "invalid_request",
			},
			{
				name:        "invalid email format",
				contentType: "application/json",
				body:        `{"full_name":"A","email":"not-an-email","password":"password123"}`,
				wantStatus:  400,
				wantCode:    "invalid_request",
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				server, creator, _, _, _, _ := testUserManagementRouter(t, nil)
				req, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/admin/users"+tc.urlSuffix, strings.NewReader(tc.body))
				if tc.contentType != "" {
					req.Header.Set("Content-Type", tc.contentType)
				}
				req.Header.Set("Authorization", "Bearer token")

				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					t.Fatalf("request failed: %v", err)
				}
				defer func() { _ = resp.Body.Close() }()

				if resp.StatusCode != tc.wantStatus {
					t.Fatalf("status = %d, want %d", resp.StatusCode, tc.wantStatus)
				}
				if creator.calls != 0 {
					t.Fatalf("creator was called %d times on invalid input, want 0", creator.calls)
				}
			})
		}
	})

	t.Run("body exceeds limit returns 413", func(t *testing.T) {
		server, creator, _, _, _, _ := testUserManagementRouter(t, func(d *RouterDependencies) {
			d.AdminMaxBodyBytes = 50
		})
		largeBody := `{"full_name":"Nguyen Van A","email":"long-email-that-exceeds-limit@example.com","password":"password123"}`
		req, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/admin/users", strings.NewReader(largeBody))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer token")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusRequestEntityTooLarge {
			t.Fatalf("status = %d, want 413", resp.StatusCode)
		}
		if creator.calls != 0 {
			t.Fatalf("creator was called %d times on oversized body, want 0", creator.calls)
		}
	})
}

// 2. GET /v1/admin/users tests
func TestListUsersHandler(t *testing.T) {
	t.Run("happy path lists users", func(t *testing.T) {
		u1 := uuid.New()
		name1 := "User 1"
		now := time.Now().UTC()
		server, _, lister, _, _, _ := testUserManagementRouter(t, nil)
		lister.fn = func(_ context.Context, _ uuid.UUID) ([]usermanagement.UserSummary, error) {
			return []usermanagement.UserSummary{
				{
					ID:           u1,
					Email:        "u1@example.com",
					FullName:     &name1,
					PlatformRole: "user",
					CreatedAt:    now,
					GatewayRoles: []usermanagement.UserGatewayRole{
						{GatewayID: "gw1", Role: "viewer"},
					},
				},
			}, nil
		}

		req, _ := http.NewRequest(http.MethodGet, server.URL+"/v1/admin/users", nil)
		req.Header.Set("Authorization", "Bearer token")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200", resp.StatusCode)
		}
		var res struct {
			Items []usermanagement.UserSummary `json:"items"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}
		if len(res.Items) != 1 || res.Items[0].ID != u1 {
			t.Fatalf("unexpected items: %+v", res.Items)
		}
	})

	t.Run("query parameters prohibited returns 400", func(t *testing.T) {
		server, _, lister, _, _, _ := testUserManagementRouter(t, nil)
		req, _ := http.NewRequest(http.MethodGet, server.URL+"/v1/admin/users?limit=10", nil)
		req.Header.Set("Authorization", "Bearer token")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400", resp.StatusCode)
		}
		if lister.calls != 0 {
			t.Fatalf("lister was called on prohibited query: %d", lister.calls)
		}
	})

	t.Run("non-empty body prohibited returns 400", func(t *testing.T) {
		server, _, lister, _, _, _ := testUserManagementRouter(t, nil)
		req, _ := http.NewRequest(http.MethodGet, server.URL+"/v1/admin/users", strings.NewReader("bad"))
		req.Header.Set("Authorization", "Bearer token")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400", resp.StatusCode)
		}
		if lister.calls != 0 {
			t.Fatalf("lister was called on non-empty body: %d", lister.calls)
		}
	})

	t.Run("capacity exceeded returns 409", func(t *testing.T) {
		server, _, lister, _, _, _ := testUserManagementRouter(t, nil)
		lister.fn = func(_ context.Context, _ uuid.UUID) ([]usermanagement.UserSummary, error) {
			return nil, usermanagement.ErrCapacityExceeded
		}

		req, _ := http.NewRequest(http.MethodGet, server.URL+"/v1/admin/users", nil)
		req.Header.Set("Authorization", "Bearer token")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusConflict {
			t.Fatalf("status = %d, want 409", resp.StatusCode)
		}
	})

	t.Run("actor not admin returns 403", func(t *testing.T) {
		server, _, lister, _, _, _ := testUserManagementRouter(t, nil)
		lister.fn = func(_ context.Context, _ uuid.UUID) ([]usermanagement.UserSummary, error) {
			return nil, usermanagement.ErrActorNotAdmin
		}

		req, _ := http.NewRequest(http.MethodGet, server.URL+"/v1/admin/users", nil)
		req.Header.Set("Authorization", "Bearer token")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("status = %d, want 403", resp.StatusCode)
		}
	})

	t.Run("inconsistent profile returns 500 without leaking partial list", func(t *testing.T) {
		server, _, lister, _, _, _ := testUserManagementRouter(t, nil)
		lister.fn = func(_ context.Context, _ uuid.UUID) ([]usermanagement.UserSummary, error) {
			return nil, usermanagement.ErrInconsistentProfile
		}

		req, _ := http.NewRequest(http.MethodGet, server.URL+"/v1/admin/users", nil)
		req.Header.Set("Authorization", "Bearer token")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusInternalServerError {
			t.Fatalf("status = %d, want 500", resp.StatusCode)
		}
		var errEnvelope struct {
			Error struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			} `json:"error"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&errEnvelope); err != nil {
			t.Fatalf("decode error: %v", err)
		}
		if errEnvelope.Error.Code != "internal_error" {
			t.Errorf("error code = %q, want internal_error", errEnvelope.Error.Code)
		}
	})

	t.Run("timeout returns 503 service_unavailable", func(t *testing.T) {
		server, _, lister, _, _, _ := testUserManagementRouter(t, nil)
		lister.fn = func(_ context.Context, _ uuid.UUID) ([]usermanagement.UserSummary, error) {
			return nil, context.DeadlineExceeded
		}

		req, _ := http.NewRequest(http.MethodGet, server.URL+"/v1/admin/users", nil)
		req.Header.Set("Authorization", "Bearer token")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 503", resp.StatusCode)
		}
	})

	t.Run("empty query with question mark returns 400", func(t *testing.T) {
		server, _, lister, _, _, _ := testUserManagementRouter(t, nil)
		req, _ := http.NewRequest(http.MethodGet, server.URL+"/v1/admin/users?", nil)
		req.Header.Set("Authorization", "Bearer token")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400", resp.StatusCode)
		}
		if lister.calls != 0 {
			t.Fatalf("lister was called on trailing question mark: %d", lister.calls)
		}
	})

	t.Run("response headers contain no-store and no-cache", func(t *testing.T) {
		server, _, lister, _, _, _ := testUserManagementRouter(t, nil)
		lister.fn = func(_ context.Context, _ uuid.UUID) ([]usermanagement.UserSummary, error) {
			return []usermanagement.UserSummary{}, nil
		}

		req, _ := http.NewRequest(http.MethodGet, server.URL+"/v1/admin/users", nil)
		req.Header.Set("Authorization", "Bearer token")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.Header.Get("Cache-Control") != "no-store" {
			t.Errorf("Cache-Control = %q, want no-store", resp.Header.Get("Cache-Control"))
		}
		if resp.Header.Get("Pragma") != "no-cache" {
			t.Errorf("Pragma = %q, want no-cache", resp.Header.Get("Pragma"))
		}
	})
}

// 3. PATCH /v1/admin/users/:user_id tests
func TestUpdateUserHandler(t *testing.T) {
	targetID := uuid.New()

	t.Run("happy path updates full name and role", func(t *testing.T) {
		server, _, _, updater, _, _ := testUserManagementRouter(t, nil)
		newName := "Updated Name"
		newRole := "platform_admin"
		updater.fn = func(_ context.Context, _, target uuid.UUID, _ usermanagement.UpdateUserInput) (usermanagement.UpdatedUser, error) {
			if target != targetID {
				t.Fatalf("target = %v, want %v", target, targetID)
			}
			return usermanagement.UpdatedUser{
				ID:           targetID,
				FullName:     &newName,
				PlatformRole: newRole,
			}, nil
		}

		payload := `{"full_name":"Updated Name","platform_role":"platform_admin"}`
		req, _ := http.NewRequest(http.MethodPatch, server.URL+"/v1/admin/users/"+targetID.String(), strings.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer token")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200", resp.StatusCode)
		}
		var res usermanagement.UpdatedUser
		if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}
		if res.ID != targetID || *res.FullName != newName || res.PlatformRole != newRole {
			t.Fatalf("unexpected body: %+v", res)
		}
	})

	t.Run("self demotion returns 409 self_admin_demotion_forbidden", func(t *testing.T) {
		actorID := uuid.New()
		server, _, _, updater, _, _ := testUserManagementRouter(t, func(d *RouterDependencies) {
			d.TokenVerifier = tokenVerifierFunc(func(context.Context, string) (auth.Principal, error) {
				return auth.Principal{UserID: actorID, Role: "authenticated"}, nil
			})
		})

		payload := `{"full_name":"New Name","platform_role":"user"}`
		req, _ := http.NewRequest(http.MethodPatch, server.URL+"/v1/admin/users/"+actorID.String(), strings.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer token")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusConflict {
			t.Fatalf("status = %d, want 409", resp.StatusCode)
		}
		var errEnvelope struct {
			Error struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&errEnvelope)
		if errEnvelope.Error.Code != "self_admin_demotion_forbidden" {
			t.Fatalf("code = %q, want self_admin_demotion_forbidden", errEnvelope.Error.Code)
		}
		if updater.calls != 0 {
			t.Fatalf("updater was called on forbidden self demotion: %d", updater.calls)
		}
	})

	t.Run("malformed user_id in path returns 400", func(t *testing.T) {
		server, _, _, updater, _, _ := testUserManagementRouter(t, nil)
		payload := `{"full_name":"A"}`
		req, _ := http.NewRequest(http.MethodPatch, server.URL+"/v1/admin/users/not-a-uuid", strings.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer token")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400", resp.StatusCode)
		}
		if updater.calls != 0 {
			t.Fatalf("updater was called on invalid user_id: %d", updater.calls)
		}
	})

	t.Run("user not found returns 404", func(t *testing.T) {
		server, _, _, updater, _, _ := testUserManagementRouter(t, nil)
		updater.fn = func(_ context.Context, _, _ uuid.UUID, _ usermanagement.UpdateUserInput) (usermanagement.UpdatedUser, error) {
			return usermanagement.UpdatedUser{}, usermanagement.ErrUserNotFound
		}

		payload := `{"full_name":"A"}`
		req, _ := http.NewRequest(http.MethodPatch, server.URL+"/v1/admin/users/"+targetID.String(), strings.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer token")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("status = %d, want 404", resp.StatusCode)
		}
	})

	t.Run("actor not admin returns 403 forbidden", func(t *testing.T) {
		server, _, _, updater, _, _ := testUserManagementRouter(t, nil)
		updater.fn = func(_ context.Context, _, _ uuid.UUID, _ usermanagement.UpdateUserInput) (usermanagement.UpdatedUser, error) {
			return usermanagement.UpdatedUser{}, usermanagement.ErrActorNotAdmin
		}

		payload := `{"full_name":"New Name"}`
		req, _ := http.NewRequest(http.MethodPatch, server.URL+"/v1/admin/users/"+targetID.String(), strings.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer token")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("status = %d, want 403", resp.StatusCode)
		}
		var errEnvelope struct {
			Error struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&errEnvelope); err != nil {
			t.Fatalf("decode response failed: %v", err)
		}
		if errEnvelope.Error.Code != "forbidden" {
			t.Fatalf("code = %q, want forbidden", errEnvelope.Error.Code)
		}
	})

	t.Run("timeout returns 503 service_unavailable", func(t *testing.T) {
		server, _, _, updater, _, _ := testUserManagementRouter(t, nil)
		updater.fn = func(_ context.Context, _, _ uuid.UUID, _ usermanagement.UpdateUserInput) (usermanagement.UpdatedUser, error) {
			return usermanagement.UpdatedUser{}, context.DeadlineExceeded
		}

		payload := `{"full_name":"Timeout Name"}`
		req, _ := http.NewRequest(http.MethodPatch, server.URL+"/v1/admin/users/"+targetID.String(), strings.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer token")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 503", resp.StatusCode)
		}
	})

	t.Run("response headers contain no-store and no-cache", func(t *testing.T) {
		server, _, _, updater, _, _ := testUserManagementRouter(t, nil)
		name := "Header Test"
		updater.fn = func(_ context.Context, _, _ uuid.UUID, _ usermanagement.UpdateUserInput) (usermanagement.UpdatedUser, error) {
			return usermanagement.UpdatedUser{ID: targetID, FullName: &name, PlatformRole: "user"}, nil
		}

		payload := `{"full_name":"Header Test"}`
		req, _ := http.NewRequest(http.MethodPatch, server.URL+"/v1/admin/users/"+targetID.String(), strings.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer token")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if cc := resp.Header.Get("Cache-Control"); cc != "no-store" {
			t.Errorf("Cache-Control = %q, want no-store", cc)
		}
		if pragma := resp.Header.Get("Pragma"); pragma != "no-cache" {
			t.Errorf("Pragma = %q, want no-cache", pragma)
		}
	})
}

// 4. PUT /v1/admin/gateways/:gateway_id/users/:user_id tests
func TestAssignGatewayUserHandler(t *testing.T) {
	targetID := uuid.New()
	gwID := "gateway_001"

	t.Run("happy path assigns operator", func(t *testing.T) {
		server, _, _, _, assigner, _ := testUserManagementRouter(t, nil)
		assigner.fn = func(_ context.Context, _ uuid.UUID, gatewayID string, target uuid.UUID, role string) error {
			if gatewayID != gwID || target != targetID || role != "operator" {
				t.Fatalf("unexpected assigner call: %s, %v, %s", gatewayID, target, role)
			}
			return nil
		}

		payload := `{"role":"operator"}`
		req, _ := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/v1/admin/gateways/%s/users/%s", server.URL, gwID, targetID), strings.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer token")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200", resp.StatusCode)
		}
		var res struct {
			GatewayID string `json:"gateway_id"`
			UserID    string `json:"user_id"`
			Role      string `json:"role"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
			t.Fatalf("decode response failed: %v", err)
		}
		if res.GatewayID != gwID || res.UserID != targetID.String() || res.Role != "operator" {
			t.Fatalf("unexpected response: %+v", res)
		}
	})

	t.Run("owner role rejected with 400 invalid_role", func(t *testing.T) {
		server, _, _, _, assigner, _ := testUserManagementRouter(t, nil)
		payload := `{"role":"owner"}`
		req, _ := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/v1/admin/gateways/%s/users/%s", server.URL, gwID, targetID), strings.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer token")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400", resp.StatusCode)
		}
		var errEnvelope struct {
			Error struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&errEnvelope); err != nil {
			t.Fatalf("decode response failed: %v", err)
		}
		if errEnvelope.Error.Code != "invalid_role" {
			t.Fatalf("code = %q, want invalid_role", errEnvelope.Error.Code)
		}
		if assigner.calls != 0 {
			t.Fatalf("assigner called on owner role: %d", assigner.calls)
		}
	})

	t.Run("target is currently owner returns 409 owner_transfer_required", func(t *testing.T) {
		server, _, _, _, assigner, _ := testUserManagementRouter(t, nil)
		assigner.fn = func(_ context.Context, _ uuid.UUID, _ string, _ uuid.UUID, _ string) error {
			return usermanagement.ErrOwnerTransferRequired
		}

		payload := `{"role":"viewer"}`
		req, _ := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/v1/admin/gateways/%s/users/%s", server.URL, gwID, targetID), strings.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer token")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusConflict {
			t.Fatalf("status = %d, want 409", resp.StatusCode)
		}
		var errEnvelope struct {
			Error struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&errEnvelope); err != nil {
			t.Fatalf("decode response failed: %v", err)
		}
		if errEnvelope.Error.Code != "owner_transfer_required" {
			t.Fatalf("code = %q, want owner_transfer_required", errEnvelope.Error.Code)
		}
	})

	t.Run("reserved gateway backend_service returns 400", func(t *testing.T) {
		server, _, _, _, assigner, _ := testUserManagementRouter(t, nil)
		payload := `{"role":"operator"}`
		req, _ := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/v1/admin/gateways/backend_service/users/%s", server.URL, targetID), strings.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer token")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400", resp.StatusCode)
		}
		if assigner.calls != 0 {
			t.Fatalf("assigner called on reserved gateway: %d", assigner.calls)
		}
	})

	t.Run("happy path assigns viewer", func(t *testing.T) {
		server, _, _, _, assigner, _ := testUserManagementRouter(t, nil)
		assigner.fn = func(_ context.Context, _ uuid.UUID, _ string, _ uuid.UUID, role string) error {
			if role != "viewer" {
				t.Fatalf("role = %q, want viewer", role)
			}
			return nil
		}

		payload := `{"role":"viewer"}`
		req, _ := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/v1/admin/gateways/%s/users/%s", server.URL, gwID, targetID), strings.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer token")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200", resp.StatusCode)
		}
	})

	t.Run("invalid role returns 400 invalid_request", func(t *testing.T) {
		server, _, _, _, assigner, _ := testUserManagementRouter(t, nil)
		payload := `{"role":"superadmin"}`
		req, _ := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/v1/admin/gateways/%s/users/%s", server.URL, gwID, targetID), strings.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer token")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400", resp.StatusCode)
		}
		var errEnvelope struct {
			Error struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&errEnvelope); err != nil {
			t.Fatalf("decode response failed: %v", err)
		}
		if errEnvelope.Error.Code != "invalid_request" {
			t.Fatalf("code = %q, want invalid_request", errEnvelope.Error.Code)
		}
		if assigner.calls != 0 {
			t.Fatalf("assigner called on invalid role: %d", assigner.calls)
		}
	})

	t.Run("gateway not found returns 404 not_found", func(t *testing.T) {
		server, _, _, _, assigner, _ := testUserManagementRouter(t, nil)
		assigner.fn = func(_ context.Context, _ uuid.UUID, _ string, _ uuid.UUID, _ string) error {
			return usermanagement.ErrGatewayNotFound
		}

		payload := `{"role":"operator"}`
		req, _ := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/v1/admin/gateways/%s/users/%s", server.URL, gwID, targetID), strings.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer token")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("status = %d, want 404", resp.StatusCode)
		}
	})

	t.Run("user not found returns 404 not_found", func(t *testing.T) {
		server, _, _, _, assigner, _ := testUserManagementRouter(t, nil)
		assigner.fn = func(_ context.Context, _ uuid.UUID, _ string, _ uuid.UUID, _ string) error {
			return usermanagement.ErrUserNotFound
		}

		payload := `{"role":"operator"}`
		req, _ := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/v1/admin/gateways/%s/users/%s", server.URL, gwID, targetID), strings.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer token")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("status = %d, want 404", resp.StatusCode)
		}
	})

	t.Run("actor not admin returns 403 forbidden", func(t *testing.T) {
		server, _, _, _, assigner, _ := testUserManagementRouter(t, nil)
		assigner.fn = func(_ context.Context, _ uuid.UUID, _ string, _ uuid.UUID, _ string) error {
			return usermanagement.ErrActorNotAdmin
		}

		payload := `{"role":"operator"}`
		req, _ := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/v1/admin/gateways/%s/users/%s", server.URL, gwID, targetID), strings.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer token")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("status = %d, want 403", resp.StatusCode)
		}
		var errEnvelope struct {
			Error struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&errEnvelope); err != nil {
			t.Fatalf("decode response failed: %v", err)
		}
		if errEnvelope.Error.Code != "forbidden" {
			t.Fatalf("code = %q, want forbidden", errEnvelope.Error.Code)
		}
	})

	t.Run("timeout returns 503 service_unavailable", func(t *testing.T) {
		server, _, _, _, assigner, _ := testUserManagementRouter(t, nil)
		assigner.fn = func(_ context.Context, _ uuid.UUID, _ string, _ uuid.UUID, _ string) error {
			return context.DeadlineExceeded
		}

		payload := `{"role":"operator"}`
		req, _ := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/v1/admin/gateways/%s/users/%s", server.URL, gwID, targetID), strings.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer token")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 503", resp.StatusCode)
		}
	})

	t.Run("malformed user_id returns 400", func(t *testing.T) {
		server, _, _, _, assigner, _ := testUserManagementRouter(t, nil)
		payload := `{"role":"operator"}`
		req, _ := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/v1/admin/gateways/%s/users/not-a-uuid", server.URL, gwID), strings.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer token")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400", resp.StatusCode)
		}
		if assigner.calls != 0 {
			t.Fatalf("assigner called on malformed user_id: %d", assigner.calls)
		}
	})

	t.Run("response headers contain no-store and no-cache", func(t *testing.T) {
		server, _, _, _, assigner, _ := testUserManagementRouter(t, nil)
		assigner.fn = func(_ context.Context, _ uuid.UUID, _ string, _ uuid.UUID, _ string) error {
			return nil
		}

		payload := `{"role":"operator"}`
		req, _ := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/v1/admin/gateways/%s/users/%s", server.URL, gwID, targetID), strings.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer token")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if cc := resp.Header.Get("Cache-Control"); cc != "no-store" {
			t.Errorf("Cache-Control = %q, want no-store", cc)
		}
		if pragma := resp.Header.Get("Pragma"); pragma != "no-cache" {
			t.Errorf("Pragma = %q, want no-cache", pragma)
		}
	})
}

// 5. PUT /v1/admin/gateways/:gateway_id/owner tests
func TestTransferGatewayOwnerHandler(t *testing.T) {
	targetID := uuid.New()
	gwID := "gateway_001"

	t.Run("happy path transfers owner", func(t *testing.T) {
		server, _, _, _, _, ownerTransferer := testUserManagementRouter(t, nil)
		ownerTransferer.fn = func(_ context.Context, _ uuid.UUID, gatewayID string, target uuid.UUID) error {
			if gatewayID != gwID || target != targetID {
				t.Fatalf("unexpected ownerTransferer call: %s, %v", gatewayID, target)
			}
			return nil
		}

		payload := fmt.Sprintf(`{"user_id":"%s"}`, targetID)
		req, _ := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/v1/admin/gateways/%s/owner", server.URL, gwID), strings.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer token")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200", resp.StatusCode)
		}
		var res struct {
			GatewayID   string `json:"gateway_id"`
			OwnerUserID string `json:"owner_user_id"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&res)
		if res.GatewayID != gwID || res.OwnerUserID != targetID.String() {
			t.Fatalf("unexpected response: %+v", res)
		}
	})

	t.Run("gateway owner inconsistent returns 409", func(t *testing.T) {
		server, _, _, _, _, ownerTransferer := testUserManagementRouter(t, nil)
		ownerTransferer.fn = func(_ context.Context, _ uuid.UUID, _ string, _ uuid.UUID) error {
			return usermanagement.ErrGatewayOwnerInconsistent
		}

		payload := fmt.Sprintf(`{"user_id":"%s"}`, targetID)
		req, _ := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/v1/admin/gateways/%s/owner", server.URL, gwID), strings.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer token")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusConflict {
			t.Fatalf("status = %d, want 409", resp.StatusCode)
		}
		var errEnvelope struct {
			Error struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&errEnvelope)
		if errEnvelope.Error.Code != "gateway_owner_inconsistent" {
			t.Fatalf("code = %q, want gateway_owner_inconsistent", errEnvelope.Error.Code)
		}
	})

	t.Run("same owner no-op returns 200", func(t *testing.T) {
		server, _, _, _, _, ownerTransferer := testUserManagementRouter(t, nil)
		ownerTransferer.fn = func(_ context.Context, _ uuid.UUID, _ string, _ uuid.UUID) error {
			return nil // no-op succeeds
		}

		payload := fmt.Sprintf(`{"user_id":"%s"}`, targetID)
		req, _ := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/v1/admin/gateways/%s/owner", server.URL, gwID), strings.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer token")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200", resp.StatusCode)
		}
	})

	t.Run("gateway not found returns 404 not_found", func(t *testing.T) {
		server, _, _, _, _, ownerTransferer := testUserManagementRouter(t, nil)
		ownerTransferer.fn = func(_ context.Context, _ uuid.UUID, _ string, _ uuid.UUID) error {
			return usermanagement.ErrGatewayNotFound
		}

		payload := fmt.Sprintf(`{"user_id":"%s"}`, targetID)
		req, _ := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/v1/admin/gateways/%s/owner", server.URL, gwID), strings.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer token")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("status = %d, want 404", resp.StatusCode)
		}
		var errEnvelope struct {
			Error struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&errEnvelope)
		if errEnvelope.Error.Code != "not_found" {
			t.Fatalf("code = %q, want not_found", errEnvelope.Error.Code)
		}
	})

	t.Run("user not found returns 404 not_found", func(t *testing.T) {
		server, _, _, _, _, ownerTransferer := testUserManagementRouter(t, nil)
		ownerTransferer.fn = func(_ context.Context, _ uuid.UUID, _ string, _ uuid.UUID) error {
			return usermanagement.ErrUserNotFound
		}

		payload := fmt.Sprintf(`{"user_id":"%s"}`, targetID)
		req, _ := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/v1/admin/gateways/%s/owner", server.URL, gwID), strings.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer token")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("status = %d, want 404", resp.StatusCode)
		}
		var errEnvelope struct {
			Error struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&errEnvelope)
		if errEnvelope.Error.Code != "not_found" {
			t.Fatalf("code = %q, want not_found", errEnvelope.Error.Code)
		}
	})

	t.Run("actor not admin returns 403 forbidden", func(t *testing.T) {
		server, _, _, _, _, ownerTransferer := testUserManagementRouter(t, nil)
		ownerTransferer.fn = func(_ context.Context, _ uuid.UUID, _ string, _ uuid.UUID) error {
			return usermanagement.ErrActorNotAdmin
		}

		payload := fmt.Sprintf(`{"user_id":"%s"}`, targetID)
		req, _ := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/v1/admin/gateways/%s/owner", server.URL, gwID), strings.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer token")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("status = %d, want 403", resp.StatusCode)
		}
		var errEnvelope struct {
			Error struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&errEnvelope)
		if errEnvelope.Error.Code != "forbidden" {
			t.Fatalf("code = %q, want forbidden", errEnvelope.Error.Code)
		}
	})

	t.Run("timeout returns 503 service_unavailable", func(t *testing.T) {
		server, _, _, _, _, ownerTransferer := testUserManagementRouter(t, nil)
		ownerTransferer.fn = func(_ context.Context, _ uuid.UUID, _ string, _ uuid.UUID) error {
			return context.DeadlineExceeded
		}

		payload := fmt.Sprintf(`{"user_id":"%s"}`, targetID)
		req, _ := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/v1/admin/gateways/%s/owner", server.URL, gwID), strings.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer token")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 503", resp.StatusCode)
		}
	})

	t.Run("reserved gateway backend_service returns 400", func(t *testing.T) {
		server, _, _, _, _, ownerTransferer := testUserManagementRouter(t, nil)
		payload := fmt.Sprintf(`{"user_id":"%s"}`, targetID)
		req, _ := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/v1/admin/gateways/backend_service/owner", server.URL), strings.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer token")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400", resp.StatusCode)
		}
		if ownerTransferer.calls != 0 {
			t.Fatalf("ownerTransferer called on reserved gateway: %d", ownerTransferer.calls)
		}
	})

	t.Run("malformed user_id returns 400", func(t *testing.T) {
		server, _, _, _, _, ownerTransferer := testUserManagementRouter(t, nil)
		payload := `{"user_id":"not-a-valid-uuid"}`
		req, _ := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/v1/admin/gateways/%s/owner", server.URL, gwID), strings.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer token")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400", resp.StatusCode)
		}
		if ownerTransferer.calls != 0 {
			t.Fatalf("ownerTransferer called on malformed user_id: %d", ownerTransferer.calls)
		}
	})

	t.Run("oversized payload returns 413", func(t *testing.T) {
		server, _, _, _, _, ownerTransferer := testUserManagementRouter(t, nil)
		oversized := fmt.Sprintf(`{"user_id":"%s","padding":"%s"}`, targetID, strings.Repeat("A", 20000))
		req, _ := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/v1/admin/gateways/%s/owner", server.URL, gwID), strings.NewReader(oversized))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer token")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusRequestEntityTooLarge {
			t.Fatalf("status = %d, want 413", resp.StatusCode)
		}
		if ownerTransferer.calls != 0 {
			t.Fatalf("ownerTransferer called on oversized payload: %d", ownerTransferer.calls)
		}
	})

	t.Run("unsupported media type returns 415", func(t *testing.T) {
		server, _, _, _, _, ownerTransferer := testUserManagementRouter(t, nil)
		payload := fmt.Sprintf(`{"user_id":"%s"}`, targetID)
		req, _ := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/v1/admin/gateways/%s/owner", server.URL, gwID), strings.NewReader(payload))
		req.Header.Set("Content-Type", "text/plain")
		req.Header.Set("Authorization", "Bearer token")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusUnsupportedMediaType {
			t.Fatalf("status = %d, want 415", resp.StatusCode)
		}
		if ownerTransferer.calls != 0 {
			t.Fatalf("ownerTransferer called on unsupported media type: %d", ownerTransferer.calls)
		}
	})

	t.Run("response headers contain no-store and no-cache", func(t *testing.T) {
		server, _, _, _, _, ownerTransferer := testUserManagementRouter(t, nil)
		ownerTransferer.fn = func(_ context.Context, _ uuid.UUID, _ string, _ uuid.UUID) error {
			return nil
		}

		payload := fmt.Sprintf(`{"user_id":"%s"}`, targetID)
		req, _ := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/v1/admin/gateways/%s/owner", server.URL, gwID), strings.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer token")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if cc := resp.Header.Get("Cache-Control"); cc != "no-store" {
			t.Errorf("Cache-Control = %q, want no-store", cc)
		}
		if pragma := resp.Header.Get("Pragma"); pragma != "no-cache" {
			t.Errorf("Pragma = %q, want no-cache", pragma)
		}
	})
}

// 6. Security boundary: 401 Unauthorized, 403 Forbidden, 503 Service Unavailable
func TestUserManagementSecurityBoundary(t *testing.T) {
	endpoints := []struct {
		method string
		path   string
		body   string
	}{
		{http.MethodPost, "/v1/admin/users", `{"full_name":"A","email":"a@b.com","password":"p12345"}`},
		{http.MethodGet, "/v1/admin/users", ""},
		{http.MethodPatch, "/v1/admin/users/" + uuid.New().String(), `{"full_name":"B"}`},
		{http.MethodPut, "/v1/admin/gateways/gw1/users/" + uuid.New().String(), `{"role":"viewer"}`},
		{http.MethodPut, "/v1/admin/gateways/gw1/owner", `{"user_id":"` + uuid.New().String() + `"}`},
	}

	t.Run("missing token returns 401 and calls no dependencies", func(t *testing.T) {
		server, creator, lister, updater, assigner, ownerTransferer := testUserManagementRouter(t, nil)
		for _, ep := range endpoints {
			var r *http.Request
			if ep.body != "" {
				r, _ = http.NewRequest(ep.method, server.URL+ep.path, strings.NewReader(ep.body))
				r.Header.Set("Content-Type", "application/json")
			} else {
				r, _ = http.NewRequest(ep.method, server.URL+ep.path, nil)
			}
			// No Authorization header

			resp, err := http.DefaultClient.Do(r)
			if err != nil {
				t.Fatalf("request failed: %v", err)
			}
			_ = resp.Body.Close()

			if resp.StatusCode != http.StatusUnauthorized {
				t.Fatalf("[%s %s] status = %d, want 401", ep.method, ep.path, resp.StatusCode)
			}
		}
		if creator.calls != 0 || lister.calls != 0 || updater.calls != 0 || assigner.calls != 0 || ownerTransferer.calls != 0 {
			t.Fatalf("dependencies were invoked on 401 unauthorized requests")
		}
	})

	t.Run("non-admin user returns 403 and calls no dependencies", func(t *testing.T) {
		server, creator, lister, updater, assigner, ownerTransferer := testUserManagementRouter(t, func(d *RouterDependencies) {
			d.PlatformAdminChecker = platformAdminCheckerFunc(func(context.Context, uuid.UUID) (bool, error) {
				return false, nil // Not platform admin
			})
		})
		for _, ep := range endpoints {
			var r *http.Request
			if ep.body != "" {
				r, _ = http.NewRequest(ep.method, server.URL+ep.path, strings.NewReader(ep.body))
				r.Header.Set("Content-Type", "application/json")
			} else {
				r, _ = http.NewRequest(ep.method, server.URL+ep.path, nil)
			}
			r.Header.Set("Authorization", "Bearer valid-token-but-not-admin")

			resp, err := http.DefaultClient.Do(r)
			if err != nil {
				t.Fatalf("request failed: %v", err)
			}
			_ = resp.Body.Close()

			if resp.StatusCode != http.StatusForbidden {
				t.Fatalf("[%s %s] status = %d, want 403", ep.method, ep.path, resp.StatusCode)
			}
		}
		if creator.calls != 0 || lister.calls != 0 || updater.calls != 0 || assigner.calls != 0 || ownerTransferer.calls != 0 {
			t.Fatalf("dependencies were invoked on 403 forbidden requests")
		}
	})

	t.Run("admin checker error returns 503 and calls no dependencies", func(t *testing.T) {
		server, creator, lister, updater, assigner, ownerTransferer := testUserManagementRouter(t, func(d *RouterDependencies) {
			d.PlatformAdminChecker = platformAdminCheckerFunc(func(context.Context, uuid.UUID) (bool, error) {
				return false, errors.New("database timeout")
			})
		})
		for _, ep := range endpoints {
			var r *http.Request
			if ep.body != "" {
				r, _ = http.NewRequest(ep.method, server.URL+ep.path, strings.NewReader(ep.body))
				r.Header.Set("Content-Type", "application/json")
			} else {
				r, _ = http.NewRequest(ep.method, server.URL+ep.path, nil)
			}
			r.Header.Set("Authorization", "Bearer valid-token")

			resp, err := http.DefaultClient.Do(r)
			if err != nil {
				t.Fatalf("request failed: %v", err)
			}
			_ = resp.Body.Close()

			if resp.StatusCode != http.StatusServiceUnavailable {
				t.Fatalf("[%s %s] status = %d, want 503", ep.method, ep.path, resp.StatusCode)
			}
		}
		if creator.calls != 0 || lister.calls != 0 || updater.calls != 0 || assigner.calls != 0 || ownerTransferer.calls != 0 {
			t.Fatalf("dependencies were invoked on 503 service unavailable requests")
		}
	})

	t.Run("nil dependency returns 503 fail-closed", func(t *testing.T) {
		server, _, _, _, _, _ := testUserManagementRouter(t, func(d *RouterDependencies) {
			d.UserCreator = nil
			d.UserLister = nil
			d.UserUpdater = nil
			d.GatewayUserAssigner = nil
			d.GatewayOwnerTransferer = nil
		})
		for _, ep := range endpoints {
			var r *http.Request
			if ep.body != "" {
				r, _ = http.NewRequest(ep.method, server.URL+ep.path, strings.NewReader(ep.body))
				r.Header.Set("Content-Type", "application/json")
			} else {
				r, _ = http.NewRequest(ep.method, server.URL+ep.path, nil)
			}
			r.Header.Set("Authorization", "Bearer valid-token")

			resp, err := http.DefaultClient.Do(r)
			if err != nil {
				t.Fatalf("request failed: %v", err)
			}
			_ = resp.Body.Close()

			if resp.StatusCode != http.StatusServiceUnavailable {
				t.Fatalf("[%s %s] status = %d, want 503 on nil dependency", ep.method, ep.path, resp.StatusCode)
			}
		}
	})
}
