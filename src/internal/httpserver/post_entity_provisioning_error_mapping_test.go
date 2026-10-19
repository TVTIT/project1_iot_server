package httpserver

import (
	"context"
	"errors"
	"io"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/puddle/v2"

	"iot-platform/internal/digitaltwin"
	"iot-platform/internal/gateway"
)

// beginFailureDB fails every BeginTx with the configured error, exercising the real
// gateway provisioning repository plus the Digital Twin create service down to the
// HTTP status mapper, not the classifier in isolation.
type beginFailureDB struct{ err error }

func (d *beginFailureDB) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) {
	return nil, d.err
}

type stubSensorProvisioner struct{}

func (stubSensorProvisioner) ProvisionSensorForAdminOrOwner(context.Context, uuid.UUID, gateway.ProvisionSensorInput) (gateway.ProvisionSensorResult, error) {
	return gateway.ProvisionSensorResult{}, errors.New("unexpected sensor provisioning call")
}

func newGatewayCreateService(t *testing.T, beginErr error) digitaltwin.EntityCreateService {
	t.Helper()
	repo, err := gateway.NewPostgresProvisioningRepository(&beginFailureDB{err: beginErr})
	if err != nil {
		t.Fatalf("NewPostgresProvisioningRepository: %v", err)
	}
	svc, err := digitaltwin.NewCreateService(repo, stubSensorProvisioner{})
	if err != nil {
		t.Fatalf("NewCreateService: %v", err)
	}
	return svc
}

func TestPostEntityGatewayProvisioningMapsAvailabilityTo503(t *testing.T) {
	input := digitaltwin.CreateEntityInput{
		Type: digitaltwin.EntityTypeGateway,
		Gateway: &gateway.ProvisionGatewayInput{
			GatewayID:   "gw_http_map",
			Name:        "HTTP Map Gateway",
			OwnerUserID: uuid.New(),
		},
	}

	cases := []struct {
		name        string
		beginErr    error
		wantStatus  int
		wantCode    string
		unavailable bool
	}{
		{
			name:        "closed pool begin",
			beginErr:    puddle.ErrClosedPool,
			wantStatus:  http.StatusServiceUnavailable,
			wantCode:    "service_unavailable",
			unavailable: true,
		},
		{
			name:        "transport eof begin",
			beginErr:    io.EOF,
			wantStatus:  http.StatusServiceUnavailable,
			wantCode:    "service_unavailable",
			unavailable: true,
		},
		{
			name:        "undefined table begin stays internal",
			beginErr:    &pgconn.PgError{Code: "42P01"},
			wantStatus:  http.StatusInternalServerError,
			wantCode:    "internal_error",
			unavailable: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc := newGatewayCreateService(t, tc.beginErr)
			_, err := svc.CreateEntity(context.Background(), uuid.New(), input)
			if err == nil {
				t.Fatal("expected provisioning error, got nil")
			}
			if got := errors.Is(err, gateway.ErrGatewayProvisioningUnavailable); got != tc.unavailable {
				t.Fatalf("ErrGatewayProvisioningUnavailable presence=%v, want %v: %v", got, tc.unavailable, err)
			}
			if !errors.Is(err, tc.beginErr) {
				t.Fatalf("cause lost: %v", err)
			}
			status, code, message := ClassifyMutationError(err)
			if status != tc.wantStatus || code != tc.wantCode {
				t.Fatalf("got status=%d code=%q, want status=%d code=%q", status, code, tc.wantStatus, tc.wantCode)
			}
			if message == "" {
				t.Fatal("expected a safe client message")
			}
		})
	}
}
