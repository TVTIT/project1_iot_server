package gateway

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

var (
	// ErrAdminForbidden marks a non-admin attempting an admin-only operation.
	ErrAdminForbidden = errors.New("platform admin required")

	// ErrAdminLookupFailed marks database infrastructure failure during admin authorization.
	ErrAdminLookupFailed = errors.New("admin lookup failed")

	// ErrInventoryUnavailable marks transport or server availability failure during inventory reads.
	ErrInventoryUnavailable = errors.New("inventory unavailable")

	// ErrInvalidInventoryQuery marks query parameters violating the pagination contract.
	ErrInvalidInventoryQuery = errors.New("invalid inventory query")
)

// AdminGateway represents a Gateway in the global administrative inventory.
// It deliberately does not contain a membership role or sensitive secrets.
type AdminGateway struct {
	GatewayID   string
	Name        string
	Description *string
	CreatedAt   *time.Time
}

// AdminGatewayInventoryPage represents a paginated slice of the global Gateway inventory.
type AdminGatewayInventoryPage struct {
	Items      []AdminGateway
	NextCursor *string
}

// AdminGatewayInventoryQuery specifies keyset pagination bounds.
type AdminGatewayInventoryQuery struct {
	Limit int
	After string
}

// AdminGatewayRepository provides platform admin inventory access.
type AdminGatewayRepository interface {
	ListAdminGateways(ctx context.Context, actor uuid.UUID, query AdminGatewayInventoryQuery) (AdminGatewayInventoryPage, error)
}

// AdminGatewayService validates and bounds admin inventory queries.
type AdminGatewayService struct {
	repository   AdminGatewayRepository
	timeout      time.Duration
	defaultLimit int
	maxLimit     int
}

// NewAdminGatewayService creates a validated service for admin gateway inventory.
func NewAdminGatewayService(repository AdminGatewayRepository, timeout time.Duration, defaultLimit, maxLimit int) (*AdminGatewayService, error) {
	if isNilDependency(repository) {
		return nil, fmt.Errorf("admin gateway repository is required")
	}
	if timeout <= 0 {
		return nil, fmt.Errorf("admin inventory timeout must be positive")
	}
	if defaultLimit <= 0 {
		return nil, fmt.Errorf("admin inventory default limit must be positive")
	}
	if maxLimit <= 0 || defaultLimit > maxLimit {
		return nil, fmt.Errorf("admin inventory max limit must be at least default limit")
	}
	return &AdminGatewayService{
		repository:   repository,
		timeout:      timeout,
		defaultLimit: defaultLimit,
		maxLimit:     maxLimit,
	}, nil
}

// ListAdminGatewayInventory validates caller and query bounds before delegating.
func (s *AdminGatewayService) ListAdminGatewayInventory(ctx context.Context, actor uuid.UUID, query AdminGatewayInventoryQuery) (AdminGatewayInventoryPage, error) {
	if actor == uuid.Nil {
		return AdminGatewayInventoryPage{}, ErrInvalidUser
	}
	if query.After != "" {
		if err := ValidateGatewayID(query.After); err != nil {
			return AdminGatewayInventoryPage{}, err
		}
	}
	if query.Limit == 0 {
		query.Limit = s.defaultLimit
	} else if query.Limit < 1 || query.Limit > s.maxLimit {
		return AdminGatewayInventoryPage{}, ErrInvalidInventoryQuery
	}

	c, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	page, err := s.repository.ListAdminGateways(c, actor, query)
	if err == nil {
		err = c.Err()
	}
	if err != nil {
		return AdminGatewayInventoryPage{}, err
	}
	return page, nil
}
