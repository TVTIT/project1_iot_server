package usermanagement

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestSupabaseAuthAdapter_CreateUser_Success(t *testing.T) {
	expectedUUID := uuid.New()
	expectedEmail := "test-user@example.com"
	expectedFullName := "Nguyen Van A"
	expectedPassword := "secure-password-123"
	serviceRoleKey := "test-service-role-key-999"

	var receivedMethod string
	var receivedPath string
	var receivedAuthHeader string
	var receivedAPIKeyHeader string
	var receivedContentType string
	var receivedBody map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedMethod = r.Method
		receivedPath = r.URL.Path
		receivedAuthHeader = r.Header.Get("Authorization")
		receivedAPIKeyHeader = r.Header.Get("apikey")
		receivedContentType = r.Header.Get("Content-Type")

		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "cannot read body", http.StatusBadRequest)
			return
		}
		_ = json.Unmarshal(bodyBytes, &receivedBody)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		respJSON, _ := json.Marshal(map[string]any{
			"id":         expectedUUID.String(),
			"email":      expectedEmail,
			"created_at": "2026-10-19T10:00:00Z",
		})
		_, _ = w.Write(respJSON)
	}))
	defer server.Close()

	adapter, err := NewSupabaseAuthAdapter(SupabaseAuthAdapterConfig{
		BaseURL:        server.URL,
		ServiceRoleKey: serviceRoleKey,
		HTTPClient:     server.Client(),
	})
	if err != nil {
		t.Fatalf("unexpected adapter constructor error: %v", err)
	}

	result, err := adapter.CreateUser(context.Background(), AuthAdminCreateUserInput{
		Email:    expectedEmail,
		Password: expectedPassword,
		FullName: expectedFullName,
	})
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}

	// Verify exact request format
	if receivedMethod != http.MethodPost {
		t.Errorf("expected POST method, got %s", receivedMethod)
	}
	if receivedPath != "/auth/v1/admin/users" {
		t.Errorf("expected path /auth/v1/admin/users, got %s", receivedPath)
	}
	if receivedAuthHeader != "Bearer "+serviceRoleKey {
		t.Errorf("expected Authorization header Bearer %s, got %s", serviceRoleKey, receivedAuthHeader)
	}
	if receivedAPIKeyHeader != serviceRoleKey {
		t.Errorf("expected apikey header %s, got %s", serviceRoleKey, receivedAPIKeyHeader)
	}
	if !strings.HasPrefix(receivedContentType, "application/json") {
		t.Errorf("expected Content-Type application/json, got %s", receivedContentType)
	}

	// Verify body contents
	if receivedBody["email"] != expectedEmail {
		t.Errorf("expected email %q, got %v", expectedEmail, receivedBody["email"])
	}
	if receivedBody["password"] != expectedPassword {
		t.Errorf("expected password %q, got %v", expectedPassword, receivedBody["password"])
	}
	if receivedBody["email_confirm"] != true {
		t.Errorf("expected email_confirm true, got %v", receivedBody["email_confirm"])
	}
	userMeta, ok := receivedBody["user_metadata"].(map[string]any)
	if !ok || userMeta["full_name"] != expectedFullName {
		t.Errorf("expected user_metadata.full_name %q, got %v", expectedFullName, receivedBody["user_metadata"])
	}

	// Verify returned user
	if result.ID != expectedUUID {
		t.Errorf("expected ID %s, got %s", expectedUUID, result.ID)
	}
	if result.Email != expectedEmail {
		t.Errorf("expected Email %s, got %s", expectedEmail, result.Email)
	}
	if result.CreatedAt.IsZero() {
		t.Errorf("expected non-zero CreatedAt")
	}
}

func TestSupabaseAuthAdapter_CreateUser_PasswordBytePreservation(t *testing.T) {
	// Password containing leading/trailing spaces, newlines, tabs, quotes, UTF-8 symbols
	rawPassword := "  p@$$w0rd \t\n\"'\\ \u2603  "
	var receivedRawPassword string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Password string `json:"password"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		receivedRawPassword = req.Password

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id": "` + uuid.New().String() + `", "email": "a@b.com"}`))
	}))
	defer server.Close()

	adapter, err := NewSupabaseAuthAdapter(SupabaseAuthAdapterConfig{
		BaseURL:        server.URL,
		ServiceRoleKey: "test-key",
		HTTPClient:     server.Client(),
	})
	if err != nil {
		t.Fatalf("unexpected adapter error: %v", err)
	}

	_, err = adapter.CreateUser(context.Background(), AuthAdminCreateUserInput{
		Email:    "a@b.com",
		Password: rawPassword,
		FullName: "Test",
	})
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}

	if receivedRawPassword != rawPassword {
		t.Errorf("password was mutated: expected exact bytes %q, got %q", rawPassword, receivedRawPassword)
	}
}

func TestSupabaseAuthAdapter_CreateUser_ResponseUUIDValidation(t *testing.T) {
	tests := []struct {
		name     string
		respBody string
	}{
		{"missing id", `{"email": "a@b.com"}`},
		{"empty id", `{"id": "", "email": "a@b.com"}`},
		{"invalid format id", `{"id": "not-a-valid-uuid", "email": "a@b.com"}`},
		{"nil uuid", `{"id": "00000000-0000-0000-0000-000000000000", "email": "a@b.com"}`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(tc.respBody))
			}))
			defer server.Close()

			adapter, err := NewSupabaseAuthAdapter(SupabaseAuthAdapterConfig{
				BaseURL:        server.URL,
				ServiceRoleKey: "test-key",
				HTTPClient:     server.Client(),
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			_, err = adapter.CreateUser(context.Background(), AuthAdminCreateUserInput{
				Email:    "a@b.com",
				Password: "password123",
				FullName: "Test",
			})
			if err == nil {
				t.Fatalf("expected error for %s, got nil", tc.name)
			}
			if !errors.Is(err, ErrAuthInvalidResponse) {
				t.Errorf("expected ErrAuthInvalidResponse, got %v", err)
			}
		})
	}
}

func TestSupabaseAuthAdapter_CreateUser_DuplicateEmailClassification(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		respBody   string
	}{
		{
			name:       "gotrue 422 standard message",
			statusCode: 422,
			respBody:   `{"code": 422, "msg": "A user with this email address has already been registered"}`,
		},
		{
			name:       "gotrue error_code email_exists",
			statusCode: 422,
			respBody:   `{"error_code": "email_exists", "message": "Email already exists"}`,
		},
		{
			name:       "409 conflict",
			statusCode: 409,
			respBody:   `{"code": 409, "message": "User already exists"}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.statusCode)
				_, _ = w.Write([]byte(tc.respBody))
			}))
			defer server.Close()

			adapter, err := NewSupabaseAuthAdapter(SupabaseAuthAdapterConfig{
				BaseURL:        server.URL,
				ServiceRoleKey: "test-key",
				HTTPClient:     server.Client(),
			})
			if err != nil {
				t.Fatalf("unexpected constructor error: %v", err)
			}

			_, err = adapter.CreateUser(context.Background(), AuthAdminCreateUserInput{
				Email:    "dup@example.com",
				Password: "password123",
				FullName: "Dup User",
			})
			if err == nil {
				t.Fatalf("expected duplicate email error, got nil")
			}
			if !errors.Is(err, ErrUserAlreadyExists) {
				t.Errorf("expected ErrUserAlreadyExists, got %v", err)
			}

			// Ensure raw upstream response body is NOT exposed in error
			if strings.Contains(err.Error(), "already been registered") {
				t.Errorf("error leaks raw upstream message: %v", err)
			}
		})
	}
}

func TestSupabaseAuthAdapter_CreateUser_WeakPasswordClassification(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		respBody   string
	}{
		{
			name:       "gotrue 422 password too short",
			statusCode: 422,
			respBody:   `{"code": 422, "msg": "Password should be at least 6 characters"}`,
		},
		{
			name:       "gotrue 400 weak password code",
			statusCode: 400,
			respBody:   `{"error_code": "weak_password", "msg": "Password does not meet complexity requirements"}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.statusCode)
				_, _ = w.Write([]byte(tc.respBody))
			}))
			defer server.Close()

			adapter, err := NewSupabaseAuthAdapter(SupabaseAuthAdapterConfig{
				BaseURL:        server.URL,
				ServiceRoleKey: "test-key",
				HTTPClient:     server.Client(),
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			_, err = adapter.CreateUser(context.Background(), AuthAdminCreateUserInput{
				Email:    "user@example.com",
				Password: "123",
				FullName: "User",
			})
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !errors.Is(err, ErrInvalidInput) {
				t.Errorf("expected ErrInvalidInput, got %v", err)
			}
			if strings.Contains(err.Error(), "Password should be") {
				t.Errorf("error leaks raw upstream message: %v", err)
			}
		})
	}
}

func TestSupabaseAuthAdapter_CreateUser_MalformedResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id": "incomplete-json`))
	}))
	defer server.Close()

	adapter, err := NewSupabaseAuthAdapter(SupabaseAuthAdapterConfig{
		BaseURL:        server.URL,
		ServiceRoleKey: "test-key",
		HTTPClient:     server.Client(),
	})
	if err != nil {
		t.Fatalf("unexpected constructor error: %v", err)
	}

	_, err = adapter.CreateUser(context.Background(), AuthAdminCreateUserInput{
		Email:    "user@example.com",
		Password: "password123",
		FullName: "User",
	})
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !errors.Is(err, ErrAuthInvalidResponse) {
		t.Errorf("expected ErrAuthInvalidResponse, got %v", err)
	}
}

func TestSupabaseAuthAdapter_CreateUser_OversizedResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		// Send 10KB of padding
		padding := strings.Repeat("A", 10*1024)
		_, _ = w.Write([]byte(`{"id": "` + uuid.New().String() + `", "padding": "` + padding + `"}`))
	}))
	defer server.Close()

	adapter, err := NewSupabaseAuthAdapter(SupabaseAuthAdapterConfig{
		BaseURL:        server.URL,
		ServiceRoleKey: "test-key",
		MaxBodyBytes:   1024, // 1KB limit
		HTTPClient:     server.Client(),
	})
	if err != nil {
		t.Fatalf("unexpected constructor error: %v", err)
	}

	_, err = adapter.CreateUser(context.Background(), AuthAdminCreateUserInput{
		Email:    "user@example.com",
		Password: "password123",
		FullName: "User",
	})
	if err == nil {
		t.Fatalf("expected error on oversized response, got nil")
	}
	if !errors.Is(err, ErrAuthInvalidResponse) {
		t.Errorf("expected ErrAuthInvalidResponse, got %v", err)
	}
}

func TestSupabaseAuthAdapter_CreateUser_TimeoutAndCancel(t *testing.T) {
	t.Run("context canceled before call", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		adapter, err := NewSupabaseAuthAdapter(SupabaseAuthAdapterConfig{
			BaseURL:        server.URL,
			ServiceRoleKey: "test-key",
			HTTPClient:     server.Client(),
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err = adapter.CreateUser(ctx, AuthAdminCreateUserInput{
			Email:    "user@example.com",
			Password: "password123",
			FullName: "User",
		})
		if err == nil {
			t.Fatalf("expected error, got nil")
		}
		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	})

	t.Run("server delay exceeds deadline", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			time.Sleep(100 * time.Millisecond)
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		adapter, err := NewSupabaseAuthAdapter(SupabaseAuthAdapterConfig{
			BaseURL:        server.URL,
			ServiceRoleKey: "test-key",
			HTTPClient:     server.Client(),
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()

		_, err = adapter.CreateUser(ctx, AuthAdminCreateUserInput{
			Email:    "user@example.com",
			Password: "password123",
			FullName: "User",
		})
		if err == nil {
			t.Fatalf("expected timeout error, got nil")
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("expected context.DeadlineExceeded, got %v", err)
		}
	})
}

func TestSupabaseAuthAdapter_CreateUser_RedirectsProhibited(t *testing.T) {
	var redirectTargetReached atomic.Bool

	redirectTargetServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		redirectTargetReached.Store(true)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id": "` + uuid.New().String() + `"}`))
	}))
	defer redirectTargetServer.Close()

	var authServerCalled atomic.Bool
	authServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authServerCalled.Store(true)
		// Attempt to redirect caller to another URL
		http.Redirect(w, r, redirectTargetServer.URL, http.StatusTemporaryRedirect)
	}))
	defer authServer.Close()

	adapter, err := NewSupabaseAuthAdapter(SupabaseAuthAdapterConfig{
		BaseURL:        authServer.URL,
		ServiceRoleKey: "secret-key-123",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, err = adapter.CreateUser(context.Background(), AuthAdminCreateUserInput{
		Email:    "user@example.com",
		Password: "password123",
		FullName: "User",
	})
	if err == nil {
		t.Fatalf("expected redirect error, got nil")
	}
	if !authServerCalled.Load() {
		t.Errorf("auth server was never called")
	}

	if redirectTargetReached.Load() {
		t.Errorf("redirect target was reached; redirects MUST be prohibited to prevent leaking credentials")
	}
}

func TestSupabaseAuthAdapter_CreateUser_TransportErrors(t *testing.T) {
	// Point to closed/invalid server
	adapter, err := NewSupabaseAuthAdapter(SupabaseAuthAdapterConfig{
		BaseURL:        "http://127.0.0.1:54321", // non-listening port
		ServiceRoleKey: "secret-key-123",
		Timeout:        500 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, err = adapter.CreateUser(context.Background(), AuthAdminCreateUserInput{
		Email:    "user@example.com",
		Password: "password123",
		FullName: "User",
	})
	if err == nil {
		t.Fatalf("expected transport error, got nil")
	}
	if !errors.Is(err, ErrAuthUnavailable) {
		t.Errorf("expected ErrAuthUnavailable, got %v", err)
	}
}

func TestSupabaseAuthAdapter_SecretSafety(t *testing.T) {
	secretKey := "super-secret-service-role-key-xyz"
	userPassword := "user-plain-password-987"

	failureCases := []struct {
		name       string
		statusCode int
		body       string
	}{
		{"401 unauthorized", 401, `{"message": "upstream-secret-invalid-key"}`},
		{"403 forbidden", 403, `{"message": "upstream-secret-forbidden-detail"}`},
		{"500 internal server error", 500, `{"message": "upstream-gotrue-db-crash-dump"}`},
		{"502 bad gateway", 502, `upstream-gateway-unreachable-internal-ip`},
		{"503 unavailable", 503, `upstream-server-overloaded-internal-trace`},
	}

	for _, tc := range failureCases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.statusCode)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()

			adapter, err := NewSupabaseAuthAdapter(SupabaseAuthAdapterConfig{
				BaseURL:        server.URL,
				ServiceRoleKey: secretKey,
				HTTPClient:     server.Client(),
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			_, err = adapter.CreateUser(context.Background(), AuthAdminCreateUserInput{
				Email:    "user@example.com",
				Password: userPassword,
				FullName: "User",
			})
			if err == nil {
				t.Fatalf("expected error, got nil")
			}

			errMsg := err.Error()
			if strings.Contains(errMsg, secretKey) {
				t.Errorf("error leaks service role key: %s", errMsg)
			}
			if strings.Contains(errMsg, userPassword) {
				t.Errorf("error leaks user password: %s", errMsg)
			}
			if strings.Contains(errMsg, tc.body) {
				t.Errorf("error leaks raw upstream body: %s", errMsg)
			}
		})
	}
}

func TestNewSupabaseAuthAdapter_Validation(t *testing.T) {
	tests := []struct {
		name        string
		cfg         SupabaseAuthAdapterConfig
		shouldErr   bool
		errContains string
	}{
		{
			name:        "empty base url",
			cfg:         SupabaseAuthAdapterConfig{BaseURL: "", ServiceRoleKey: "key"},
			shouldErr:   true,
			errContains: "BaseURL is required",
		},
		{
			name:        "invalid base url scheme",
			cfg:         SupabaseAuthAdapterConfig{BaseURL: "ftp://example.com", ServiceRoleKey: "key"},
			shouldErr:   true,
			errContains: "BaseURL must be a valid HTTP or HTTPS URL",
		},
		{
			name:        "empty service role key",
			cfg:         SupabaseAuthAdapterConfig{BaseURL: "http://example.com", ServiceRoleKey: ""},
			shouldErr:   true,
			errContains: "ServiceRoleKey is required",
		},
		{
			name:      "valid minimal config applies defaults",
			cfg:       SupabaseAuthAdapterConfig{BaseURL: "http://example.com", ServiceRoleKey: "key"},
			shouldErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			adapter, err := NewSupabaseAuthAdapter(tc.cfg)
			if tc.shouldErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if !strings.Contains(err.Error(), tc.errContains) {
					t.Errorf("expected error containing %q, got %v", tc.errContains, err)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if adapter.maxBodyBytes != defaultAuthAdminMaxBodyBytes {
					t.Errorf("expected default maxBodyBytes %d, got %d", defaultAuthAdminMaxBodyBytes, adapter.maxBodyBytes)
				}
				if adapter.client == nil {
					t.Errorf("expected non-nil client")
				}
			}
		})
	}
}

func TestSupabaseAuthAdapter_BaseURLNormalization(t *testing.T) {
	var pathsCalled []string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pathsCalled = append(pathsCalled, r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id": "` + uuid.New().String() + `", "email": "a@b.com"}`))
	}))
	defer server.Close()

	baseURLs := []string{
		server.URL,
		server.URL + "/",
		server.URL + "/auth/v1",
		server.URL + "/auth/v1/",
	}

	for _, bu := range baseURLs {
		pathsCalled = nil
		adapter, err := NewSupabaseAuthAdapter(SupabaseAuthAdapterConfig{
			BaseURL:        bu,
			ServiceRoleKey: "key",
			HTTPClient:     server.Client(),
		})
		if err != nil {
			t.Fatalf("unexpected error for %q: %v", bu, err)
		}

		_, err = adapter.CreateUser(context.Background(), AuthAdminCreateUserInput{
			Email:    "a@b.com",
			Password: "password",
			FullName: "User",
		})
		if err != nil {
			t.Fatalf("unexpected error for %q: %v", bu, err)
		}

		if len(pathsCalled) != 1 || pathsCalled[0] != "/auth/v1/admin/users" {
			t.Errorf("for base URL %q, expected path /auth/v1/admin/users, got %v", bu, pathsCalled)
		}
	}
}
