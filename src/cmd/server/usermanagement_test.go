package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"iot-platform/internal/auth"
	"iot-platform/internal/config"
	"iot-platform/internal/gateway"
	"iot-platform/internal/httpserver"
	"iot-platform/internal/usermanagement"
)

func validUserManagementTestConfig() config.Config {
	cfg := validTestConfig()
	cfg.UserManagement = config.UserManagementConfig{
		AuthBaseURL:      "http://127.0.0.1:8000",
		ServiceRoleKey:   "test-service-role-key-12345",
		OperationTimeout: 5 * time.Second,
		MaxBodyBytes:     16384,
		UserList: usermanagement.UserListConfig{
			MaxUsers:         1000,
			MaxMemberships:   10000,
			MaxResponseBytes: 2097152,
		},
	}
	return cfg
}

func TestComposeUserManagementDependencies_Valid(t *testing.T) {
	pool := testPool(t)
	cfg := validUserManagementTestConfig()

	deps, err := composeUserManagementDependencies(cfg, pool)
	if err != nil {
		t.Fatalf("composeUserManagementDependencies failed: %v", err)
	}

	if deps.UserCreator == nil {
		t.Fatal("expected non-nil UserCreator")
	}
	if deps.UserLister == nil {
		t.Fatal("expected non-nil UserLister")
	}
	if deps.UserUpdater == nil {
		t.Fatal("expected non-nil UserUpdater")
	}
	if deps.GatewayUserAssigner == nil {
		t.Fatal("expected non-nil GatewayUserAssigner")
	}
	if deps.GatewayOwnerTransferer == nil {
		t.Fatal("expected non-nil GatewayOwnerTransferer")
	}

	verifier, err := auth.NewHS256Verifier(auth.VerifierConfig{
		Secret:   strings.Repeat("s", 32),
		Issuer:   "https://auth.example.com",
		Audience: "authenticated",
	})
	if err != nil {
		t.Fatalf("create test verifier: %v", err)
	}

	adminChecker, err := auth.NewPostgresPlatformAdminChecker(pool)
	if err != nil {
		t.Fatalf("create platform admin checker: %v", err)
	}
	gatewayRepo, err := gateway.NewPostgresRepository(pool)
	if err != nil {
		t.Fatalf("create gateway repo: %v", err)
	}
	gatewayService, err := gateway.NewService(gatewayRepo, cfg.AuthorizationTimeout)
	if err != nil {
		t.Fatalf("create gateway service: %v", err)
	}
	provisioningRepo, err := gateway.NewPostgresProvisioningRepository(pool)
	if err != nil {
		t.Fatalf("create provisioning repo: %v", err)
	}
	provisioningService, err := gateway.NewProvisioningService(provisioningRepo, cfg.AuthorizationTimeout)
	if err != nil {
		t.Fatalf("create provisioning service: %v", err)
	}
	ownerSensorRepo, err := gateway.NewPostgresOwnerSensorProvisioningRepository(pool)
	if err != nil {
		t.Fatalf("create owner sensor repo: %v", err)
	}
	ownerSensorService, err := gateway.NewOwnerSensorProvisioningService(ownerSensorRepo, cfg.AuthorizationTimeout)
	if err != nil {
		t.Fatalf("create owner sensor service: %v", err)
	}
	adminGatewayRepo, err := gateway.NewPostgresAdminGatewayRepository(pool)
	if err != nil {
		t.Fatalf("create admin gateway repo: %v", err)
	}
	adminGatewayService, err := gateway.NewAdminGatewayService(adminGatewayRepo, cfg.AuthorizationTimeout, 100, 200)
	if err != nil {
		t.Fatalf("create admin gateway service: %v", err)
	}

	routerDeps := httpserver.RouterDependencies{
		ReadinessChecker:           dummyReadinessChecker{},
		ReadinessTimeout:           time.Second,
		AuthorizationTimeout:       cfg.AuthorizationTimeout,
		TokenVerifier:              verifier,
		PlatformAdminChecker:       adminChecker,
		GatewayReader:              gatewayService,
		SensorReader:               gatewayService,
		GatewayProvisioner:         provisioningService,
		SensorProvisioner:          provisioningService,
		OwnerSensorProvisioner:     ownerSensorService,
		AdminGatewayLister:         adminGatewayService,
		AdminInventoryDefaultLimit: 100,
		AdminInventoryMaxLimit:     200,
		AdminMaxBodyBytes:          16384,
	}

	stage4Deps, err := composeStage4Dependencies(cfg, pool, dummyGatewayProvisioner{})
	if err != nil {
		t.Fatalf("composeStage4Dependencies failed: %v", err)
	}
	stage4Deps.apply(&routerDeps)

	deps.apply(&routerDeps)

	router, err := httpserver.NewRouter(routerDeps)
	if err != nil {
		t.Fatalf("NewRouter failed: %v", err)
	}
	if router == nil {
		t.Fatal("expected non-nil HTTP router handler")
	}
}

func TestComposeUserManagementDependencies_OverridePropagation(t *testing.T) {
	pool := testPool(t)
	cfg := validUserManagementTestConfig()
	cfg.UserManagement.OperationTimeout = 3 * time.Second
	cfg.UserManagement.MaxBodyBytes = 8192
	cfg.UserManagement.UserList.MaxUsers = 500
	cfg.UserManagement.UserList.MaxMemberships = 2000
	cfg.UserManagement.UserList.MaxResponseBytes = 1048576

	deps, err := composeUserManagementDependencies(cfg, pool)
	if err != nil {
		t.Fatalf("composeUserManagementDependencies failed: %v", err)
	}

	if deps.UserCreator == nil || deps.UserLister == nil || deps.UserUpdater == nil ||
		deps.GatewayUserAssigner == nil || deps.GatewayUserRemover == nil || deps.GatewayOwnerTransferer == nil {
		t.Fatal("expected non-nil handlers with custom overrides")
	}
}

func TestComposeUserManagementDependencies_WithoutServiceRoleKey(t *testing.T) {
	pool := testPool(t)
	cfg := validUserManagementTestConfig()
	cfg.UserManagement.ServiceRoleKey = "" // unconfigured service role key

	deps, err := composeUserManagementDependencies(cfg, pool)
	if err != nil {
		t.Fatalf("composeUserManagementDependencies should succeed without service role key: %v", err)
	}

	if deps.UserCreator == nil {
		t.Fatal("expected non-nil UserCreator")
	}
	if deps.UserLister == nil {
		t.Fatal("expected non-nil UserLister")
	}

	if deps.UserUpdater == nil {
		t.Fatal("expected non-nil UserUpdater")
	}
	if deps.GatewayUserAssigner == nil {
		t.Fatal("expected non-nil GatewayUserAssigner")
	}
	if deps.GatewayUserRemover == nil {
		t.Fatal("expected non-nil GatewayUserRemover without Auth credentials")
	}
	if deps.GatewayOwnerTransferer == nil {
		t.Fatal("expected non-nil GatewayOwnerTransferer")
	}

	// Calling CreateUser on unavailable client must fail-closed with ErrAuthUnavailable
	_, createErr := unavailableAuthAdminClient{}.CreateUser(context.Background(), usermanagement.AuthAdminCreateUserInput{
		FullName: "Test User",
		Email:    "test@example.com",
		Password: "password1234",
	})
	if createErr == nil {
		t.Fatal("expected CreateUser to fail when auth admin is unconfigured")
	}
	if !strings.Contains(createErr.Error(), "auth") && !strings.Contains(createErr.Error(), "unavailable") {
		t.Fatalf("expected auth unavailable error, got: %v", createErr)
	}
}

func TestComposeUserManagementDependencies_FailClosed(t *testing.T) {
	cfg := validUserManagementTestConfig()

	// 1. Nil pool
	if _, err := composeUserManagementDependencies(cfg, nil); err == nil {
		t.Fatal("expected error with nil pool")
	}

	// 2. Invalid config
	pool := testPool(t)
	invalidConfigs := []struct {
		name   string
		modify func(*config.Config)
	}{
		{
			name: "invalid operation timeout",
			modify: func(c *config.Config) {
				c.UserManagement.OperationTimeout = 0
			},
		},
		{
			name: "operation timeout too large",
			modify: func(c *config.Config) {
				c.UserManagement.OperationTimeout = 120 * time.Second
			},
		},
		{
			name: "max body bytes zero",
			modify: func(c *config.Config) {
				c.UserManagement.MaxBodyBytes = 0
			},
		},
		{
			name: "invalid auth base URL",
			modify: func(c *config.Config) {
				c.UserManagement.AuthBaseURL = "ftp://invalid-scheme"
			},
		},
		{
			name: "invalid user list max users",
			modify: func(c *config.Config) {
				c.UserManagement.UserList.MaxUsers = 0
			},
		},
		{
			name: "max memberships less than max users",
			modify: func(c *config.Config) {
				c.UserManagement.UserList.MaxUsers = 5000
				c.UserManagement.UserList.MaxMemberships = 1000
			},
		},
	}

	for _, tc := range invalidConfigs {
		t.Run(tc.name, func(t *testing.T) {
			testCfg := validUserManagementTestConfig()
			tc.modify(&testCfg)
			if _, err := composeUserManagementDependencies(testCfg, pool); err == nil {
				t.Fatalf("expected error for %s", tc.name)
			}
		})
	}
}

type mockAdminChecker struct {
	isAdmin bool
	err     error
}

func (m mockAdminChecker) IsPlatformAdmin(_ context.Context, _ uuid.UUID) (bool, error) {
	return m.isAdmin, m.err
}

func TestUserManagementRoutes_MiddlewareAndSecurityBoundary(t *testing.T) {
	jwtSecret := strings.Repeat("k", 32)
	issuer := "https://auth.example.com"
	audience := "authenticated"

	verifier, err := auth.NewHS256Verifier(auth.VerifierConfig{
		Secret:   jwtSecret,
		Issuer:   issuer,
		Audience: audience,
	})
	if err != nil {
		t.Fatalf("create verifier: %v", err)
	}

	makeToken := func(userID uuid.UUID, role string, expOffset time.Duration) string {
		claims := jwt.MapClaims{
			"sub":  userID.String(),
			"role": role,
			"iss":  issuer,
			"aud":  audience,
			"exp":  time.Now().Add(expOffset).Unix(),
			"iat":  time.Now().Add(-time.Minute).Unix(),
		}
		tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
		s, err := tok.SignedString([]byte(jwtSecret))
		if err != nil {
			panic(err)
		}
		return s
	}

	type endpoint struct {
		method string
		path   string
		body   string
	}
	targetUserID := uuid.New().String()
	endpoints := []endpoint{
		{method: http.MethodPost, path: "/v1/admin/users", body: `{"full_name":"Test","email":"t@ex.com","password":"pw123456"}`},
		{method: http.MethodGet, path: "/v1/admin/users", body: ""},
		{method: http.MethodPatch, path: fmt.Sprintf("/v1/admin/users/%s", targetUserID), body: `{"full_name":"New Name"}`},
		{method: http.MethodPut, path: fmt.Sprintf("/v1/admin/gateways/gw-001/users/%s", targetUserID), body: `{"role":"operator"}`},
		{method: http.MethodPut, path: "/v1/admin/gateways/gw-001/owner", body: fmt.Sprintf(`{"user_id":"%s"}`, targetUserID)},
	}

	// 1. Missing or invalid JWT -> 401 Unauthorized
	t.Run("missing and invalid JWT returns 401", func(t *testing.T) {
		routerDeps := httpserver.DefaultTestRouterDependencies()
		routerDeps.TokenVerifier = verifier
		routerDeps.PlatformAdminChecker = mockAdminChecker{isAdmin: true}

		router, err := httpserver.NewRouter(routerDeps)
		if err != nil {
			t.Fatalf("NewRouter failed: %v", err)
		}
		server := httptest.NewServer(router)
		defer server.Close()

		for _, ep := range endpoints {
			// No token
			var bodyReader io.Reader
			if ep.body != "" {
				bodyReader = strings.NewReader(ep.body)
			}
			req, _ := http.NewRequest(ep.method, server.URL+ep.path, bodyReader)
			if ep.body != "" {
				req.Header.Set("Content-Type", "application/json")
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("[%s %s] request failed: %v", ep.method, ep.path, err)
			}
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusUnauthorized {
				t.Fatalf("[%s %s] status = %d, want 401 on missing token", ep.method, ep.path, resp.StatusCode)
			}

			// Expired token
			expiredToken := makeToken(uuid.New(), "authenticated", -time.Hour)
			if ep.body != "" {
				bodyReader = strings.NewReader(ep.body)
			}
			reqExp, _ := http.NewRequest(ep.method, server.URL+ep.path, bodyReader)
			if ep.body != "" {
				reqExp.Header.Set("Content-Type", "application/json")
			}
			reqExp.Header.Set("Authorization", "Bearer "+expiredToken)
			respExp, err := http.DefaultClient.Do(reqExp)
			if err != nil {
				t.Fatalf("[%s %s] request failed: %v", ep.method, ep.path, err)
			}
			_ = respExp.Body.Close()
			if respExp.StatusCode != http.StatusUnauthorized {
				t.Fatalf("[%s %s] status = %d, want 401 on expired token", ep.method, ep.path, respExp.StatusCode)
			}
		}
	})

	// 2. JWT with role "service_role" -> 401 Unauthorized (verifier requires role "authenticated")
	t.Run("service_role JWT is rejected", func(t *testing.T) {
		routerDeps := httpserver.DefaultTestRouterDependencies()
		routerDeps.TokenVerifier = verifier
		routerDeps.PlatformAdminChecker = mockAdminChecker{isAdmin: true}

		router, err := httpserver.NewRouter(routerDeps)
		if err != nil {
			t.Fatalf("NewRouter failed: %v", err)
		}
		server := httptest.NewServer(router)
		defer server.Close()

		serviceRoleToken := makeToken(uuid.New(), "service_role", time.Hour)
		for _, ep := range endpoints {
			var bodyReader io.Reader
			if ep.body != "" {
				bodyReader = strings.NewReader(ep.body)
			}
			req, _ := http.NewRequest(ep.method, server.URL+ep.path, bodyReader)
			if ep.body != "" {
				req.Header.Set("Content-Type", "application/json")
			}
			req.Header.Set("Authorization", "Bearer "+serviceRoleToken)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("[%s %s] request failed: %v", ep.method, ep.path, err)
			}
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusUnauthorized {
				t.Fatalf("[%s %s] status = %d, want 401 for service_role token", ep.method, ep.path, resp.StatusCode)
			}
		}
	})

	// 3. Authenticated JWT but non-admin in database -> 403 Forbidden
	t.Run("authenticated non-admin user returns 403", func(t *testing.T) {
		routerDeps := httpserver.DefaultTestRouterDependencies()
		routerDeps.TokenVerifier = verifier
		routerDeps.PlatformAdminChecker = mockAdminChecker{isAdmin: false} // not an admin

		router, err := httpserver.NewRouter(routerDeps)
		if err != nil {
			t.Fatalf("NewRouter failed: %v", err)
		}
		server := httptest.NewServer(router)
		defer server.Close()

		userToken := makeToken(uuid.New(), "authenticated", time.Hour)
		for _, ep := range endpoints {
			var bodyReader io.Reader
			if ep.body != "" {
				bodyReader = strings.NewReader(ep.body)
			}
			req, _ := http.NewRequest(ep.method, server.URL+ep.path, bodyReader)
			if ep.body != "" {
				req.Header.Set("Content-Type", "application/json")
			}
			req.Header.Set("Authorization", "Bearer "+userToken)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("[%s %s] request failed: %v", ep.method, ep.path, err)
			}
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusForbidden {
				t.Fatalf("[%s %s] status = %d, want 403 for non-admin", ep.method, ep.path, resp.StatusCode)
			}
		}
	})

	// 4. DB admin check error/timeout -> 503 Service Unavailable
	t.Run("admin checker error returns 503", func(t *testing.T) {
		routerDeps := httpserver.DefaultTestRouterDependencies()
		routerDeps.TokenVerifier = verifier
		routerDeps.PlatformAdminChecker = mockAdminChecker{isAdmin: false, err: context.DeadlineExceeded}

		router, err := httpserver.NewRouter(routerDeps)
		if err != nil {
			t.Fatalf("NewRouter failed: %v", err)
		}
		server := httptest.NewServer(router)
		defer server.Close()

		userToken := makeToken(uuid.New(), "authenticated", time.Hour)
		for _, ep := range endpoints {
			var bodyReader io.Reader
			if ep.body != "" {
				bodyReader = strings.NewReader(ep.body)
			}
			req, _ := http.NewRequest(ep.method, server.URL+ep.path, bodyReader)
			if ep.body != "" {
				req.Header.Set("Content-Type", "application/json")
			}
			req.Header.Set("Authorization", "Bearer "+userToken)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("[%s %s] request failed: %v", ep.method, ep.path, err)
			}
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusServiceUnavailable {
				t.Fatalf("[%s %s] status = %d, want 503 on checker error", ep.method, ep.path, resp.StatusCode)
			}
		}
	})

	// 5. Response headers & secret safety
	t.Run("response headers and secret safety", func(t *testing.T) {
		routerDeps := httpserver.DefaultTestRouterDependencies()
		routerDeps.TokenVerifier = verifier
		routerDeps.PlatformAdminChecker = mockAdminChecker{isAdmin: false}

		router, err := httpserver.NewRouter(routerDeps)
		if err != nil {
			t.Fatalf("NewRouter failed: %v", err)
		}
		server := httptest.NewServer(router)
		defer server.Close()

		userToken := makeToken(uuid.New(), "authenticated", time.Hour)
		for _, ep := range endpoints {
			var bodyReader io.Reader
			if ep.body != "" {
				bodyReader = strings.NewReader(ep.body)
			}
			req, _ := http.NewRequest(ep.method, server.URL+ep.path, bodyReader)
			if ep.body != "" {
				req.Header.Set("Content-Type", "application/json")
			}
			req.Header.Set("Authorization", "Bearer "+userToken)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("[%s %s] request failed: %v", ep.method, ep.path, err)
			}
			defer func(b io.ReadCloser) { _ = b.Close() }(resp.Body)

			if cc := resp.Header.Get("Cache-Control"); cc != "no-store" {
				t.Errorf("[%s %s] Cache-Control = %q, want no-store", ep.method, ep.path, cc)
			}
			if ep.method != http.MethodGet {
				if pragma := resp.Header.Get("Pragma"); pragma != "no-cache" {
					t.Errorf("[%s %s] Pragma = %q, want no-cache", ep.method, ep.path, pragma)
				}
			}

			body, _ := io.ReadAll(resp.Body)
			var errEnv struct {
				Error struct {
					Code      string `json:"code"`
					Message   string `json:"message"`
					RequestID string `json:"request_id"`
				} `json:"error"`
			}
			if err := json.Unmarshal(body, &errEnv); err != nil {
				t.Fatalf("[%s %s] failed to unmarshal error envelope: %v", ep.method, ep.path, err)
			}
			if errEnv.Error.Code == "" || errEnv.Error.Message == "" {
				t.Errorf("[%s %s] error envelope missing code or message: %+v", ep.method, ep.path, errEnv)
			}
		}
	})
}
