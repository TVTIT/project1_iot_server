package gateway

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

type adminGatewayRepositoryFunc func(context.Context, uuid.UUID, AdminGatewayInventoryQuery) (AdminGatewayInventoryPage, error)

func (f adminGatewayRepositoryFunc) ListAdminGateways(ctx context.Context, actor uuid.UUID, query AdminGatewayInventoryQuery) (AdminGatewayInventoryPage, error) {
	return f(ctx, actor, query)
}

func TestAdminGatewayServiceValidationAndBounds(t *testing.T) {
	actor := uuid.New()
	validRepo := adminGatewayRepositoryFunc(func(_ context.Context, _ uuid.UUID, _ AdminGatewayInventoryQuery) (AdminGatewayInventoryPage, error) {
		return AdminGatewayInventoryPage{Items: []AdminGateway{}}, nil
	})

	// Dependency validation
	if _, err := NewAdminGatewayService(nil, time.Second, 100, 200); err == nil {
		t.Fatal("expected error on nil repository")
	}
	if _, err := NewAdminGatewayService(validRepo, 0, 100, 200); err == nil {
		t.Fatal("expected error on zero timeout")
	}
	if _, err := NewAdminGatewayService(validRepo, time.Second, 0, 200); err == nil {
		t.Fatal("expected error on non-positive default limit")
	}
	if _, err := NewAdminGatewayService(validRepo, time.Second, 100, 50); err == nil {
		t.Fatal("expected error on max limit < default limit")
	}

	svc, err := NewAdminGatewayService(validRepo, 2*time.Second, 100, 200)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	// Actor validation
	if _, err := svc.ListAdminGatewayInventory(context.Background(), uuid.Nil, AdminGatewayInventoryQuery{Limit: 10}); !errors.Is(err, ErrInvalidUser) {
		t.Fatalf("expected ErrInvalidUser on nil actor, got %v", err)
	}

	// After validation
	if _, err := svc.ListAdminGatewayInventory(context.Background(), actor, AdminGatewayInventoryQuery{Limit: 10, After: "invalid*id"}); !errors.Is(err, ErrInvalidGatewayID) {
		t.Fatalf("expected ErrInvalidGatewayID on invalid After, got %v", err)
	}
	if _, err := svc.ListAdminGatewayInventory(context.Background(), actor, AdminGatewayInventoryQuery{Limit: 10, After: "backend_service"}); !errors.Is(err, ErrInvalidGatewayID) {
		t.Fatalf("expected ErrInvalidGatewayID on reserved After, got %v", err)
	}

	// Limit out of bounds
	if _, err := svc.ListAdminGatewayInventory(context.Background(), actor, AdminGatewayInventoryQuery{Limit: 201}); !errors.Is(err, ErrInvalidInventoryQuery) {
		t.Fatalf("expected ErrInvalidInventoryQuery on limit > max, got %v", err)
	}
	if _, err := svc.ListAdminGatewayInventory(context.Background(), actor, AdminGatewayInventoryQuery{Limit: -5}); !errors.Is(err, ErrInvalidInventoryQuery) {
		t.Fatalf("expected ErrInvalidInventoryQuery on negative limit, got %v", err)
	}
}

func TestAdminGatewayServiceQueryNormalizationAndExecution(t *testing.T) {
	actor := uuid.New()
	var capturedQuery AdminGatewayInventoryQuery
	calls := 0

	repo := adminGatewayRepositoryFunc(func(_ context.Context, a uuid.UUID, q AdminGatewayInventoryQuery) (AdminGatewayInventoryPage, error) {
		calls++
		if a != actor {
			t.Fatalf("expected actor %s, got %s", actor, a)
		}
		capturedQuery = q
		cursor := "gw-002"
		return AdminGatewayInventoryPage{
			Items: []AdminGateway{
				{GatewayID: "gw-001", Name: "Gateway 1"},
				{GatewayID: "gw-002", Name: "Gateway 2"},
			},
			NextCursor: &cursor,
		}, nil
	})

	svc, err := NewAdminGatewayService(repo, time.Second, 100, 200)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// When limit is 0, defaultLimit should be used
	page, err := svc.ListAdminGatewayInventory(context.Background(), actor, AdminGatewayInventoryQuery{Limit: 0, After: "gw-000"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected 1 call, got %d", calls)
	}
	if capturedQuery.Limit != 100 {
		t.Fatalf("expected default limit 100, got %d", capturedQuery.Limit)
	}
	if capturedQuery.After != "gw-000" {
		t.Fatalf("expected After gw-000, got %s", capturedQuery.After)
	}
	if len(page.Items) != 2 || page.NextCursor == nil || *page.NextCursor != "gw-002" {
		t.Fatalf("unexpected page returned: %+v", page)
	}
}
