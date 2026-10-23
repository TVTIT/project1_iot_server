package digitaltwin_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"iot-platform/internal/digitaltwin"
)

type controlUnavailableDB struct{}

func (controlUnavailableDB) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) {
	return nil, context.DeadlineExceeded
}

func TestControlRepositoryBounds(t *testing.T) {
	config := digitaltwin.DefaultControlConfig()
	var typedNil *pgxpool.Pool
	for _, db := range []digitaltwin.TxDatabase{nil, typedNil} {
		if _, err := digitaltwin.NewPostgresControlRepository(db, config); !errors.Is(err, digitaltwin.ErrInvalidInput) {
			t.Fatalf("nil DB %v", err)
		}
	}
	for _, mutate := range []func(*digitaltwin.ControlConfig){
		func(c *digitaltwin.ControlConfig) { c.TTL = 0 }, func(c *digitaltwin.ControlConfig) { c.TTL = 25 * time.Hour },
		func(c *digitaltwin.ControlConfig) { c.TTL = time.Second + time.Nanosecond },
		func(c *digitaltwin.ControlConfig) { c.DBTimeout = 0 }, func(c *digitaltwin.ControlConfig) { c.DBTimeout = 2 * time.Minute },
		func(c *digitaltwin.ControlConfig) { c.MaxReceipts = 0 }, func(c *digitaltwin.ControlConfig) { c.MaxReceipts = 1000001 },
		func(c *digitaltwin.ControlConfig) { c.MaxPendingOutbox = 0 }, func(c *digitaltwin.ControlConfig) { c.MaxPendingOutbox = 1000001 },
	} {
		c := config
		mutate(&c)
		if _, err := digitaltwin.NewPostgresControlRepository(controlUnavailableDB{}, c); !errors.Is(err, digitaltwin.ErrInvalidInput) {
			t.Fatalf("bad config %+v: %v", c, err)
		}
	}
	repo, err := digitaltwin.NewPostgresControlRepository(controlUnavailableDB{}, config)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	actor := uuid.New()
	urn := "urn:ngsi-ld:Sensor:fixture:s1"
	request := controlRequest(t, "60", "0", "valid")
	for _, tc := range []struct {
		ctx     context.Context
		actor   uuid.UUID
		urn     string
		request digitaltwin.DesiredRequest
		want    error
	}{
		{nil, actor, urn, request, digitaltwin.ErrInvalidInput}, {ctx, uuid.Nil, urn, request, digitaltwin.ErrNilActor},
		{ctx, actor, "bad", request, digitaltwin.ErrInvalidInput}, {ctx, actor, urn, digitaltwin.DesiredRequest{}, digitaltwin.ErrInvalidInput},
		{ctx, actor, urn, request, digitaltwin.ErrDependencyUnavailable},
	} {
		if _, err = repo.Admit(tc.ctx, tc.actor, tc.urn, tc.request); !errors.Is(err, tc.want) {
			t.Fatalf("admission input: want %v got %v", tc.want, err)
		}
	}
	if _, err = repo.ListCommands(ctx, actor, urn, 101, ""); !errors.Is(err, digitaltwin.ErrInvalidInput) {
		t.Fatal(err)
	}
	for _, cursor := range []string{"!", strings.Repeat("a", 513), "W10", "WyJ3cm9uZyIsInRpbWUiLCJpZCJd"} {
		if _, err = repo.ListCommands(ctx, actor, urn, 20, cursor); !errors.Is(err, digitaltwin.ErrInvalidInput) {
			t.Fatalf("cursor %v", err)
		}
	}
	var nilContext context.Context // Deliberately exercise defensive API validation.
	if _, err = repo.ListCommands(nilContext, actor, urn, 20, ""); !errors.Is(err, digitaltwin.ErrInvalidInput) {
		t.Fatal(err)
	}
	if _, err = repo.ListCommands(ctx, uuid.Nil, urn, 20, ""); !errors.Is(err, digitaltwin.ErrNilActor) {
		t.Fatal(err)
	}
	if _, err = repo.ListCommands(ctx, actor, "bad", 20, ""); !errors.Is(err, digitaltwin.ErrInvalidInput) {
		t.Fatal(err)
	}
	if _, err = repo.ListCommands(ctx, actor, urn, 20, ""); !errors.Is(err, digitaltwin.ErrDependencyUnavailable) {
		t.Fatal(err)
	}
	if _, err = repo.GetCommand(ctx, actor, urn, "bad"); !errors.Is(err, digitaltwin.ErrInvalidInput) {
		t.Fatal(err)
	}
	if _, err = repo.GetCommand(ctx, actor, urn, uuid.NewString()); !errors.Is(err, digitaltwin.ErrDependencyUnavailable) {
		t.Fatal(err)
	}
}
