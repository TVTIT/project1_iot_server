package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"iot-platform/internal/auth"
	"iot-platform/internal/config"
	"iot-platform/internal/httpserver"
	"iot-platform/internal/usermanagement"
)

// userManagementDependencies holds the instantiated User Management HTTP dependencies.
type userManagementDependencies struct {
	UserCreator            usermanagement.UserCreator
	UserLister             usermanagement.UserLister
	UserUpdater            usermanagement.UserUpdater
	GatewayUserAssigner    usermanagement.GatewayUserAssigner
	GatewayUserRemover     usermanagement.GatewayUserRemover
	GatewayOwnerTransferer usermanagement.GatewayOwnerTransferer
}

// apply copies the User Management dependencies into the destination RouterDependencies struct.
func (u userManagementDependencies) apply(deps *httpserver.RouterDependencies) {
	if deps == nil {
		return
	}
	deps.UserCreator = u.UserCreator
	deps.UserLister = u.UserLister
	deps.UserUpdater = u.UserUpdater
	deps.GatewayUserAssigner = u.GatewayUserAssigner
	deps.GatewayUserRemover = u.GatewayUserRemover
	deps.GatewayOwnerTransferer = u.GatewayOwnerTransferer
}

// unavailableAuthAdminClient is an AuthAdminClient implementation that fails closed
// with ErrAuthUnavailable when no upstream Auth service role key is configured.
type unavailableAuthAdminClient struct{}

func (unavailableAuthAdminClient) CreateUser(_ context.Context, _ usermanagement.AuthAdminCreateUserInput) (usermanagement.AuthAdminUser, error) {
	return usermanagement.AuthAdminUser{}, usermanagement.ErrAuthUnavailable
}

// composeUserManagementDependencies instantiates the real app-role pgxpool repository,
// Auth adapter, and domain service from validated configuration and a bounded database pool.
// It fails closed at startup on invalid configuration, nil dependencies, or adapter creation errors.
func composeUserManagementDependencies(
	cfg config.Config,
	pool *pgxpool.Pool,
) (userManagementDependencies, error) {
	if auth.IsNilDependency(pool) {
		return userManagementDependencies{}, fmt.Errorf("database pool is required")
	}
	if err := config.ValidateUserManagementConfig(cfg.UserManagement); err != nil {
		return userManagementDependencies{}, fmt.Errorf("invalid user management config: %w", err)
	}

	repo, err := usermanagement.NewPostgresRepository(pool)
	if err != nil {
		return userManagementDependencies{}, fmt.Errorf("create user management repository: %w", err)
	}

	var authClient usermanagement.AuthAdminClient
	if strings.TrimSpace(cfg.UserManagement.ServiceRoleKey) != "" {
		adapterCfg := usermanagement.SupabaseAuthAdapterConfig{
			BaseURL:        cfg.UserManagement.AuthBaseURL,
			ServiceRoleKey: cfg.UserManagement.ServiceRoleKey,
			Timeout:        cfg.UserManagement.OperationTimeout,
			MaxBodyBytes:   cfg.UserManagement.MaxBodyBytes,
		}
		adapter, err := usermanagement.NewSupabaseAuthAdapter(adapterCfg)
		if err != nil {
			return userManagementDependencies{}, fmt.Errorf("create supabase auth adapter: %w", err)
		}
		authClient = adapter
	} else {
		// When ServiceRoleKey is not provided (e.g. legacy test harnesses without Auth admin credential),
		// use unavailableAuthAdminClient to fail closed on CreateUser without breaking server startup
		// or disabling the database-backed membership and list endpoints.
		authClient = unavailableAuthAdminClient{}
	}

	svc, err := usermanagement.NewService(authClient, repo, cfg.UserManagement.OperationTimeout)
	if err != nil {
		return userManagementDependencies{}, fmt.Errorf("create user management service: %w", err)
	}
	svc.WithUserListConfig(cfg.UserManagement.UserList)

	return userManagementDependencies{
		UserCreator:            svc,
		UserLister:             svc,
		UserUpdater:            svc,
		GatewayUserAssigner:    svc,
		GatewayUserRemover:     svc,
		GatewayOwnerTransferer: svc,
	}, nil
}

// populateUserManagementDependencies composes the User Management dependencies and applies
// them to the target RouterDependencies struct.
func populateUserManagementDependencies(
	deps *httpserver.RouterDependencies,
	cfg config.Config,
	pool *pgxpool.Pool,
) error {
	if deps == nil {
		return fmt.Errorf("router dependencies cannot be nil")
	}
	u, err := composeUserManagementDependencies(cfg, pool)
	if err != nil {
		return err
	}
	u.apply(deps)
	return nil
}
