package gateway

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// ErrInvalidUser marks an absent authenticated identity.
var ErrInvalidUser = errors.New("invalid user identity")

// Service validates and bounds membership-scoped reads.
type Service struct {
	repository Repository
	timeout    time.Duration
}

// NewService requires a repository and positive use-case timeout.
func NewService(repository Repository, timeout time.Duration) (*Service, error) {
	if isNilDependency(repository) {
		return nil, fmt.Errorf("gateway repository is required")
	}
	if timeout <= 0 {
		return nil, fmt.Errorf("gateway read timeout must be positive")
	}
	return &Service{repository: repository, timeout: timeout}, nil
}

// ListGatewaysForUser lists only assigned Gateways.
func (s *Service) ListGatewaysForUser(ctx context.Context, userID uuid.UUID) ([]Gateway, error) {
	if userID == uuid.Nil {
		return nil, ErrInvalidUser
	}
	c, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	items, err := s.repository.ListGatewaysForUser(c, userID)
	if err == nil {
		err = c.Err()
	}
	if err != nil {
		return nil, err
	}
	return items, nil
}

// ListSensorsForUserAndGateway validates identity and parent before querying.
func (s *Service) ListSensorsForUserAndGateway(ctx context.Context, userID uuid.UUID, id string) ([]Sensor, error) {
	if userID == uuid.Nil {
		return nil, ErrInvalidUser
	}
	if err := ValidateGatewayID(id); err != nil {
		return nil, err
	}
	c, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	items, err := s.repository.ListSensorsForUserAndGateway(c, userID, id)
	if err == nil {
		err = c.Err()
	}
	if err != nil {
		return nil, err
	}
	return items, nil
}
