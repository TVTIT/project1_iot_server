package config

import (
	"strings"
	"testing"
	"time"
)

func TestUserManagementConfig_Defaults(t *testing.T) {
	values := validConfigValues()
	cfg, err := Load(mapLookup(values))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	um := cfg.UserManagement
	if um.AuthBaseURL != "http://supabase-envoy:8000" {
		t.Errorf("AuthBaseURL = %q, want default http://supabase-envoy:8000", um.AuthBaseURL)
	}
	if um.ServiceRoleKey != "" {
		t.Errorf("ServiceRoleKey = %q, want empty default", um.ServiceRoleKey)
	}
	if um.OperationTimeout != 10*time.Second {
		t.Errorf("OperationTimeout = %s, want 10s", um.OperationTimeout)
	}
	if um.MaxBodyBytes != 16384 {
		t.Errorf("MaxBodyBytes = %d, want 16384", um.MaxBodyBytes)
	}
	if um.UserList.MaxUsers != 1000 {
		t.Errorf("UserList.MaxUsers = %d, want 1000", um.UserList.MaxUsers)
	}
	if um.UserList.MaxMemberships != 10000 {
		t.Errorf("UserList.MaxMemberships = %d, want 10000", um.UserList.MaxMemberships)
	}
	if um.UserList.MaxResponseBytes != 2097152 {
		t.Errorf("UserList.MaxResponseBytes = %d, want 2097152 (2 MiB)", um.UserList.MaxResponseBytes)
	}
}

func TestUserManagementConfig_CustomValid(t *testing.T) {
	values := validConfigValues()
	values["AUTH_ADMIN_BASE_URL"] = "http://envoy.local:8000/auth/v1"
	values["SUPABASE_SERVICE_ROLE_KEY"] = "my-service-role-key-test"
	values["AUTH_ADMIN_TIMEOUT"] = "5s"
	values["AUTH_ADMIN_MAX_BODY_BYTES"] = "32768"
	values["USER_LIST_MAX_USERS"] = "500"
	values["USER_LIST_MAX_MEMBERSHIPS"] = "5000"
	values["USER_LIST_MAX_RESPONSE_BYTES"] = "1048576"

	cfg, err := Load(mapLookup(values))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	um := cfg.UserManagement
	if um.AuthBaseURL != "http://envoy.local:8000/auth/v1" {
		t.Errorf("AuthBaseURL = %q, want http://envoy.local:8000/auth/v1", um.AuthBaseURL)
	}
	if um.ServiceRoleKey != "my-service-role-key-test" {
		t.Errorf("ServiceRoleKey = %q, want my-service-role-key-test", um.ServiceRoleKey)
	}
	if um.OperationTimeout != 5*time.Second {
		t.Errorf("OperationTimeout = %s, want 5s", um.OperationTimeout)
	}
	if um.MaxBodyBytes != 32768 {
		t.Errorf("MaxBodyBytes = %d, want 32768", um.MaxBodyBytes)
	}
	if um.UserList.MaxUsers != 500 {
		t.Errorf("UserList.MaxUsers = %d, want 500", um.UserList.MaxUsers)
	}
	if um.UserList.MaxMemberships != 5000 {
		t.Errorf("UserList.MaxMemberships = %d, want 5000", um.UserList.MaxMemberships)
	}
	if um.UserList.MaxResponseBytes != 1048576 {
		t.Errorf("UserList.MaxResponseBytes = %d, want 1048576", um.UserList.MaxResponseBytes)
	}
}

func TestUserManagementConfig_FallbackKeys(t *testing.T) {
	// Fallback to SUPABASE_URL and SERVICE_ROLE_KEY
	values := validConfigValues()
	values["SUPABASE_URL"] = "https://supabase.internal:8443"
	values["SERVICE_ROLE_KEY"] = "fallback-service-key"

	cfg, err := Load(mapLookup(values))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	um := cfg.UserManagement
	if um.AuthBaseURL != "https://supabase.internal:8443" {
		t.Errorf("AuthBaseURL = %q, want fallback https://supabase.internal:8443", um.AuthBaseURL)
	}
	if um.ServiceRoleKey != "fallback-service-key" {
		t.Errorf("ServiceRoleKey = %q, want fallback fallback-service-key", um.ServiceRoleKey)
	}

	// AUTH_ADMIN_BASE_URL takes precedence over SUPABASE_URL
	values["AUTH_ADMIN_BASE_URL"] = "http://override.local:8000"
	// SUPABASE_SERVICE_ROLE_KEY takes precedence over SERVICE_ROLE_KEY
	values["SUPABASE_SERVICE_ROLE_KEY"] = "override-service-key"

	cfg, err = Load(mapLookup(values))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.UserManagement.AuthBaseURL != "http://override.local:8000" {
		t.Errorf("AuthBaseURL = %q, want override", cfg.UserManagement.AuthBaseURL)
	}
	if cfg.UserManagement.ServiceRoleKey != "override-service-key" {
		t.Errorf("ServiceRoleKey = %q, want override", cfg.UserManagement.ServiceRoleKey)
	}
}

func TestUserManagementConfig_URLValidation(t *testing.T) {
	tests := []struct {
		name string
		url  string
	}{
		{name: "empty host", url: "http:///path"},
		{name: "relative url", url: "/auth/v1"},
		{name: "ftp scheme", url: "ftp://envoy:8000"},
		{name: "url with userinfo", url: "http://user:pass@envoy:8000"},
		{name: "invalid syntax", url: "http://[invalid-ipv6"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			values := validConfigValues()
			values["AUTH_ADMIN_BASE_URL"] = tt.url
			if _, err := Load(mapLookup(values)); err == nil {
				t.Fatalf("Load() error = nil for invalid URL %q", tt.url)
			}
		})
	}
}

func TestUserManagementConfig_BoundsValidation(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value string
	}{
		{name: "timeout too short", key: "AUTH_ADMIN_TIMEOUT", value: "50ms"},
		{name: "timeout too long", key: "AUTH_ADMIN_TIMEOUT", value: "61s"},
		{name: "timeout negative", key: "AUTH_ADMIN_TIMEOUT", value: "-1s"},
		{name: "timeout zero", key: "AUTH_ADMIN_TIMEOUT", value: "0s"},
		{name: "timeout invalid syntax", key: "AUTH_ADMIN_TIMEOUT", value: "fast"},
		{name: "max users zero", key: "USER_LIST_MAX_USERS", value: "0"},
		{name: "max users negative", key: "USER_LIST_MAX_USERS", value: "-10"},
		{name: "max users too large", key: "USER_LIST_MAX_USERS", value: "10001"},
		{name: "max memberships zero", key: "USER_LIST_MAX_MEMBERSHIPS", value: "0"},
		{name: "max memberships negative", key: "USER_LIST_MAX_MEMBERSHIPS", value: "-1"},
		{name: "max memberships too large", key: "USER_LIST_MAX_MEMBERSHIPS", value: "100001"},
		{name: "memberships less than users", key: "USER_LIST_MAX_MEMBERSHIPS", value: "500"}, // default users is 1000
		{name: "max response bytes too small", key: "USER_LIST_MAX_RESPONSE_BYTES", value: "512"},
		{name: "max response bytes too large", key: "USER_LIST_MAX_RESPONSE_BYTES", value: "10485761"},
		{name: "max body bytes zero", key: "AUTH_ADMIN_MAX_BODY_BYTES", value: "0"},
		{name: "max body bytes too large", key: "AUTH_ADMIN_MAX_BODY_BYTES", value: "1048577"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			values := validConfigValues()
			values[tt.key] = tt.value
			if _, err := Load(mapLookup(values)); err == nil {
				t.Fatalf("Load() error = nil for %s=%q", tt.key, tt.value)
			}
		})
	}
}

func TestUserManagementConfig_SecretRedaction(t *testing.T) {
	secret := "super-confidential-service-role-key-998877"
	values := validConfigValues()
	values["SUPABASE_SERVICE_ROLE_KEY"] = secret

	cfg, err := Load(mapLookup(values))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	str := cfg.UserManagement.String()
	if strings.Contains(str, secret) {
		t.Fatalf("UserManagementConfig.String() leaked secret: %s", str)
	}
	if !strings.Contains(str, "[REDACTED]") {
		t.Fatalf("UserManagementConfig.String() missing [REDACTED]: %s", str)
	}

	gostr := cfg.UserManagement.GoString()
	if strings.Contains(gostr, secret) {
		t.Fatalf("UserManagementConfig.GoString() leaked secret: %s", gostr)
	}
}
