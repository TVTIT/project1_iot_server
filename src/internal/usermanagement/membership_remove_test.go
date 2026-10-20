package usermanagement_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/usermanagement"
)

type removeRepository struct {
	*mockRepository
	fn func(context.Context, uuid.UUID, string, uuid.UUID) error
}

func (r removeRepository) RemoveGatewayUser(ctx context.Context, actor uuid.UUID, gw string, target uuid.UUID) error {
	return r.fn(ctx, actor, gw, target)
}

func TestServiceRemoveGatewayUser(t *testing.T) {
	actor, target := uuid.New(), uuid.New()
	for _, cause := range []error{nil, usermanagement.ErrOwnerTransferRequired, usermanagement.ErrActorNotAdmin, usermanagement.ErrGatewayNotFound, context.Canceled, context.DeadlineExceeded, errors.New("sql failure")} {
		authClient := &mockAuthAdminClient{}
		calls := 0
		repo := removeRepository{mockRepository: &mockRepository{}, fn: func(ctx context.Context, a uuid.UUID, gw string, u uuid.UUID) error {
			calls++
			deadline, ok := ctx.Deadline()
			if a != actor || u != target || gw != "gateway_remove" || !ok || time.Until(deadline) > time.Second {
				t.Error("wrong identity or missing operation budget")
			}
			return cause
		}}
		svc, err := usermanagement.NewService(authClient, repo, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if err := svc.RemoveGatewayUser(context.Background(), actor, "gateway_remove", target); !errors.Is(err, cause) {
			t.Errorf("got %v, want %v", err, cause)
		}
		if calls != 1 || authClient.calls != 0 {
			t.Error("incorrect delegation or unexpected Auth call")
		}
	}
	for _, tc := range []struct {
		actor  uuid.UUID
		gw     string
		target uuid.UUID
	}{
		{uuid.Nil, "gateway_remove", target}, {actor, "gateway_remove", uuid.Nil}, {actor, "", target},
		{actor, "bad/id", target}, {actor, "backend_service", target},
	} {
		repo := removeRepository{mockRepository: &mockRepository{}, fn: func(context.Context, uuid.UUID, string, uuid.UUID) error {
			t.Error("invalid input reached repository")
			return nil
		}}
		svc, err := usermanagement.NewService(&mockAuthAdminClient{}, repo, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if err := svc.RemoveGatewayUser(context.Background(), tc.actor, tc.gw, tc.target); !errors.Is(err, usermanagement.ErrInvalidInput) {
			t.Errorf("invalid input: %v", err)
		}
	}
	t.Run("cancel and deadline", func(t *testing.T) {
		for _, timeout := range []bool{false, true} {
			calls := 0
			repo := removeRepository{mockRepository: &mockRepository{}, fn: func(ctx context.Context, _ uuid.UUID, _ string, _ uuid.UUID) error {
				calls++
				<-ctx.Done()
				return ctx.Err()
			}}
			svc, err := usermanagement.NewService(&mockAuthAdminClient{}, repo, time.Millisecond)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			if !timeout {
				cancel()
			}
			want := context.Canceled
			if timeout {
				want = context.DeadlineExceeded
			}
			if err := svc.RemoveGatewayUser(ctx, actor, "gateway_remove", target); !errors.Is(err, want) {
				t.Errorf("got %v, want %v", err, want)
			}
			cancel()
			if !timeout && calls != 0 {
				t.Error("cancelled call reached repository")
			}
		}
	})
}
