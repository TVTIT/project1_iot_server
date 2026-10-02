package gateway

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

type serviceRepository struct {
	Repository
	calls int
	user  uuid.UUID
	id    string
	read  func(context.Context) error
}

func (r *serviceRepository) ListGatewaysForUser(c context.Context, u uuid.UUID) ([]Gateway, error) {
	r.calls++
	r.user = u
	return nil, r.read(c)
}
func (r *serviceRepository) ListSensorsForUserAndGateway(c context.Context, u uuid.UUID, id string) ([]Sensor, error) {
	r.calls++
	r.user = u
	r.id = id
	return nil, r.read(c)
}
func TestReadService(t *testing.T) {
	var typedNil *serviceRepository
	for _, r := range []Repository{nil, typedNil} {
		if _, err := NewService(r, time.Second); err == nil {
			t.Fatal("accepted nil repo")
		}
	}
	r := &serviceRepository{read: func(context.Context) error { return nil }}
	for _, timeout := range []time.Duration{0, -time.Second} {
		if _, err := NewService(r, timeout); err == nil {
			t.Fatal("accepted invalid timeout")
		}
	}
	s, err := NewService(r, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.ListGatewaysForUser(context.Background(), uuid.Nil); !errors.Is(err, ErrInvalidUser) {
		t.Fatal("accepted zero user")
	}
	if _, err := s.ListSensorsForUserAndGateway(context.Background(), uuid.Nil, "valid"); !errors.Is(err, ErrInvalidUser) {
		t.Fatal("accepted zero sensor caller")
	}
	if _, err := s.ListSensorsForUserAndGateway(context.Background(), uuid.New(), "../bad"); !errors.Is(err, ErrInvalidGatewayID) {
		t.Fatal("accepted bad ID")
	}
	if r.calls != 0 {
		t.Fatal("invalid input called repository")
	}
	user := uuid.New()
	r.read = func(c context.Context) error {
		if _, ok := c.Deadline(); !ok {
			t.Error("no deadline")
		}
		return nil
	}
	if _, err := s.ListGatewaysForUser(context.Background(), user); err != nil || r.user != user {
		t.Fatal("caller mismatch")
	}
	if _, err := s.ListSensorsForUserAndGateway(context.Background(), user, "valid"); err != nil || r.user != user || r.id != "valid" {
		t.Fatal("sensor caller mismatch")
	}
	for _, cause := range []error{ErrNotFound, errors.New("database failure")} {
		r.read = func(context.Context) error { return cause }
		if _, err := s.ListGatewaysForUser(context.Background(), user); !errors.Is(err, cause) {
			t.Fatal("gateway error swallowed")
		}
		if _, err := s.ListSensorsForUserAndGateway(context.Background(), user, "valid"); !errors.Is(err, cause) {
			t.Fatal("sensor error swallowed")
		}
	}
	r.read = func(c context.Context) error { <-c.Done(); return c.Err() }
	short, err := NewService(r, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := short.ListGatewaysForUser(context.Background(), user); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("no timeout")
	}
	if _, err := short.ListSensorsForUserAndGateway(context.Background(), user, "valid"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("no sensor timeout")
	}
	c, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.ListGatewaysForUser(c, user); !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation lost")
	}
	// A dependency returning success after cancellation must not escape the deadline.
	r.read = func(c context.Context) error { <-c.Done(); return nil }
	if _, err := short.ListGatewaysForUser(context.Background(), user); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("late success accepted")
	}
	if _, err := short.ListSensorsForUserAndGateway(context.Background(), user, "valid"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("late sensor success accepted")
	}
}
