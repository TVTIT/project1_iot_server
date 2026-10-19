package config

import (
	"fmt"
	"net/url"
	"time"

	"iot-platform/internal/usermanagement"
)

// UserManagementConfig holds configuration for admin user management and auth adapter.
type UserManagementConfig struct {
	AuthBaseURL      string
	ServiceRoleKey   string
	OperationTimeout time.Duration
	MaxBodyBytes     int64
	UserList         usermanagement.UserListConfig
}

// String prevents accidental secret disclosure by diagnostic formatting.
func (c UserManagementConfig) String() string {
	return fmt.Sprintf("UserManagementConfig{AuthBaseURL:%s, OperationTimeout:%s, MaxBodyBytes:%d, UserListMaxUsers:%d, UserListMaxMemberships:%d, UserListMaxResponseBytes:%d, ServiceRoleKey:[REDACTED]}",
		c.AuthBaseURL, c.OperationTimeout, c.MaxBodyBytes, c.UserList.MaxUsers, c.UserList.MaxMemberships, c.UserList.MaxResponseBytes)
}

// GoString prevents accidental secret disclosure by diagnostic formatting.
func (c UserManagementConfig) GoString() string {
	return c.String()
}

// DefaultUserManagementConfig returns safe operational defaults.
func DefaultUserManagementConfig(adminMaxBodyBytes int) UserManagementConfig {
	maxBody := int64(adminMaxBodyBytes)
	if maxBody <= 0 {
		maxBody = 16384
	}
	return UserManagementConfig{
		AuthBaseURL:      "http://supabase-envoy:8000",
		ServiceRoleKey:   "",
		OperationTimeout: 10 * time.Second,
		MaxBodyBytes:     maxBody,
		UserList:         usermanagement.DefaultUserListConfig(),
	}
}

// ValidateUserManagementConfig validates bounds and formats of UserManagementConfig.
func ValidateUserManagementConfig(cfg UserManagementConfig) error {
	if cfg.AuthBaseURL != "" {
		parsedURL, err := url.Parse(cfg.AuthBaseURL)
		if err != nil || (parsedURL.Scheme != "http" && parsedURL.Scheme != "https") || parsedURL.Host == "" || parsedURL.User != nil {
			return fmt.Errorf("auth base URL must be a valid HTTP or HTTPS URL without userinfo")
		}
	}
	if cfg.OperationTimeout < 100*time.Millisecond || cfg.OperationTimeout > 60*time.Second {
		return fmt.Errorf("operation timeout must be between 100ms and 60s")
	}
	if cfg.MaxBodyBytes < 1 || cfg.MaxBodyBytes > 1048576 {
		return fmt.Errorf("max body bytes must be between 1 and 1048576")
	}
	if cfg.UserList.MaxUsers < 1 || cfg.UserList.MaxUsers > 10000 {
		return fmt.Errorf("user list max users must be between 1 and 10000")
	}
	if cfg.UserList.MaxMemberships < 1 || cfg.UserList.MaxMemberships > 100000 {
		return fmt.Errorf("user list max memberships must be between 1 and 100000")
	}
	if cfg.UserList.MaxMemberships < cfg.UserList.MaxUsers {
		return fmt.Errorf("user list max memberships (%d) must be greater than or equal to max users (%d)",
			cfg.UserList.MaxMemberships, cfg.UserList.MaxUsers)
	}
	if cfg.UserList.MaxResponseBytes < 1024 || cfg.UserList.MaxResponseBytes > 10485760 {
		return fmt.Errorf("user list max response bytes must be between 1024 and 10485760")
	}
	return nil
}

func loadUserManagement(lookup LookupFunc, adminMaxBodyBytes int) (UserManagementConfig, error) {
	cfg := DefaultUserManagementConfig(adminMaxBodyBytes)

	// AuthBaseURL: AUTH_ADMIN_BASE_URL takes precedence over SUPABASE_URL
	if base, ok := nonEmpty(lookup, "AUTH_ADMIN_BASE_URL"); ok {
		cfg.AuthBaseURL = base
	} else if base, ok := nonEmpty(lookup, "SUPABASE_URL"); ok {
		cfg.AuthBaseURL = base
	}

	// ServiceRoleKey: SUPABASE_SERVICE_ROLE_KEY takes precedence over SERVICE_ROLE_KEY
	if key, ok := nonEmpty(lookup, "SUPABASE_SERVICE_ROLE_KEY"); ok {
		cfg.ServiceRoleKey = key
	} else if key, ok := nonEmpty(lookup, "SERVICE_ROLE_KEY"); ok {
		cfg.ServiceRoleKey = key
	}

	// OperationTimeout: AUTH_ADMIN_TIMEOUT or USER_MANAGEMENT_TIMEOUT
	timeoutKey := "AUTH_ADMIN_TIMEOUT"
	if _, ok := nonEmpty(lookup, "AUTH_ADMIN_TIMEOUT"); !ok {
		if _, ok2 := nonEmpty(lookup, "USER_MANAGEMENT_TIMEOUT"); ok2 {
			timeoutKey = "USER_MANAGEMENT_TIMEOUT"
		}
	}
	var err error
	if cfg.OperationTimeout, err = boundedDuration(lookup, timeoutKey, cfg.OperationTimeout, 100*time.Millisecond, 60*time.Second); err != nil {
		return UserManagementConfig{}, err
	}

	// MaxBodyBytes: AUTH_ADMIN_MAX_BODY_BYTES or default adminMaxBodyBytes
	if _, ok := nonEmpty(lookup, "AUTH_ADMIN_MAX_BODY_BYTES"); ok {
		maxBody, err := integer(lookup, "AUTH_ADMIN_MAX_BODY_BYTES", int(cfg.MaxBodyBytes), 1, 1048576)
		if err != nil {
			return UserManagementConfig{}, err
		}
		cfg.MaxBodyBytes = int64(maxBody)
	}

	// UserList bounds
	if cfg.UserList.MaxUsers, err = integer(lookup, "USER_LIST_MAX_USERS", cfg.UserList.MaxUsers, 1, 10000); err != nil {
		return UserManagementConfig{}, err
	}
	if cfg.UserList.MaxMemberships, err = integer(lookup, "USER_LIST_MAX_MEMBERSHIPS", cfg.UserList.MaxMemberships, 1, 100000); err != nil {
		return UserManagementConfig{}, err
	}
	if cfg.UserList.MaxResponseBytes, err = integer(lookup, "USER_LIST_MAX_RESPONSE_BYTES", cfg.UserList.MaxResponseBytes, 1024, 10485760); err != nil {
		return UserManagementConfig{}, err
	}

	if err := ValidateUserManagementConfig(cfg); err != nil {
		return UserManagementConfig{}, err
	}
	return cfg, nil
}
