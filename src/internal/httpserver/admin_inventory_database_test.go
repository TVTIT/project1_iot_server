package httpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/puddle/v2"

	"iot-platform/internal/auth"
	"iot-platform/internal/gateway"
	"iot-platform/internal/httpapi"
)

// This fixture crosses the real repository/service/HTTP boundary, without a DB.
type inventoryFailureDatabase struct {
	cause           error
	phase           string
	admin           bool
	checks, queries int
	rows            *inventoryFailureRows
}

func (d *inventoryFailureDatabase) QueryRow(context.Context, string, ...any) pgx.Row {
	d.checks++
	return inventoryAdminRow{admin: d.admin}
}

type inventoryAdminRow struct{ admin bool }

func (r inventoryAdminRow) Scan(dest ...any) error { *dest[0].(*bool) = r.admin; return nil }

func (d *inventoryFailureDatabase) Query(context.Context, string, ...any) (pgx.Rows, error) {
	d.queries++
	if d.phase == "query" {
		return nil, d.cause
	}
	d.rows = &inventoryFailureRows{cause: d.cause, phase: d.phase}
	return d.rows, nil
}

type inventoryFailureRows struct {
	pgx.Rows
	cause        error
	phase        string
	next, closed bool
}

func (r *inventoryFailureRows) Next() bool {
	if r.next {
		return false
	}
	r.next = true
	return true
}
func (r *inventoryFailureRows) Scan(dest ...any) error {
	if r.phase == "scan" {
		return r.cause
	}
	*dest[0].(*string) = "fixture-partial-item"
	*dest[1].(*string) = "Fixture"
	return nil
}
func (r *inventoryFailureRows) Err() error { return r.cause }
func (r *inventoryFailureRows) Close()     { r.closed = true }

func TestAdminInventoryDatabaseFailureHTTP(t *testing.T) {
	// Get an actual pgconn ConnectError through an injected failing dialer; no socket access.
	cfg, err := pgconn.ParseConfig("host=localhost user=fixture database=fixture sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	cfg.LookupFunc = func(context.Context, string) ([]string, error) { return []string{"127.0.0.1"}, nil }
	cfg.DialFunc = func(context.Context, string, string) (net.Conn, error) {
		return nil, &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
	}
	_, connectErr := pgconn.ConnectConfig(context.Background(), cfg)
	var ce *pgconn.ConnectError
	if !errors.As(connectErr, &ce) {
		t.Fatal("fixture did not produce ConnectError")
	}
	cfg.DialFunc = func(context.Context, string, string) (net.Conn, error) {
		return nil, &pgconn.PgError{Code: "28P01", Message: "fixture-driver-detail"}
	}
	_, rejectedConnectErr := pgconn.ConnectConfig(context.Background(), cfg)

	for _, tc := range []struct {
		name   string
		cause  error
		status int
	}{
		{"connect", connectErr, 503},
		{"connect server authentication", rejectedConnectErr, 500},
		{"closed connection", pgconn.ErrConnClosed, 503},
		{"closed pool", puddle.ErrClosedPool, 503},
		{"network reset", &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}, 503},
		{"eof", io.EOF, 503},
		{"unexpected eof", io.ErrUnexpectedEOF, 503},
		{"shutdown", &pgconn.PgError{Code: "57P01", Message: "fixture-driver-detail"}, 503},
		{"crash", &pgconn.PgError{Code: "57P02"}, 503},
		{"starting", &pgconn.PgError{Code: "57P03"}, 503},
		{"connection failure", &pgconn.PgError{Code: "08006"}, 503},
		{"connection limit", &pgconn.PgError{Code: "53300"}, 503},
		{"timeout", context.DeadlineExceeded, 503},
		{"cancellation", context.Canceled, 503},
		{"sql logic", &pgconn.PgError{Code: "42703", Message: "fixture-driver-detail"}, 500},
		{"constraint", &pgconn.PgError{Code: "23514"}, 500},
		{"sql permission", &pgconn.PgError{Code: "42501"}, 500},
		{"protocol bug", &pgconn.PgError{Code: "08P01"}, 500},
		{"arbitrary", errors.New("fixture-driver-detail conn closed"), 500},
	} {
		for _, phase := range []string{"query", "iteration"} {
			t.Run(tc.name+"/"+phase, func(t *testing.T) {
				assertInventoryDatabaseFailureHTTP(t, fmt.Errorf("fixture-driver-detail: %w", tc.cause), phase, true, tc.status)
			})
		}
	}
	t.Run("malformed scan", func(t *testing.T) {
		assertInventoryDatabaseFailureHTTP(t, errors.New("fixture-driver-detail malformed timestamp"), "scan", true, 500)
	})
	t.Run("nonadmin is not availability", func(t *testing.T) {
		assertInventoryDatabaseFailureHTTP(t, pgconn.ErrConnClosed, "query", false, 403)
	})
}

func assertInventoryDatabaseFailureHTTP(t *testing.T, cause error, phase string, admin bool, status int) {
	t.Helper()
	db := &inventoryFailureDatabase{cause: cause, phase: phase, admin: admin}
	repo, err := gateway.NewPostgresAdminGatewayRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	service, err := gateway.NewAdminGatewayService(repo, time.Second, 10, 20)
	if err != nil {
		t.Fatal(err)
	}
	actor := uuid.New()
	deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
	deps.TokenVerifier = tokenVerifierFunc(func(context.Context, string) (auth.Principal, error) {
		return auth.Principal{UserID: actor, Role: "authenticated"}, nil
	})
	deps.PlatformAdminChecker = platformAdminCheckerFunc(func(context.Context, uuid.UUID) (bool, error) { return true, nil })
	deps.AdminGatewayLister = adminGatewayListerFunc(func(ctx context.Context, actor uuid.UUID, query gateway.AdminGatewayInventoryQuery) (gateway.AdminGatewayInventoryPage, error) {
		page, err := service.ListAdminGatewayInventory(ctx, actor, query)
		if admin && !errors.Is(err, cause) {
			t.Error("repository/service lost original cause")
		}
		return page, err
	})
	deps.AdminInventoryDefaultLimit = 10
	deps.AdminInventoryMaxLimit = 20
	var logs bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(old) })
	router, err := NewRouter(deps)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/gateways", nil)
	req.Header.Set("Authorization", "Bearer fixture-token")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != status {
		t.Errorf("status=%d want=%d", rec.Code, status)
	}
	if rec.Header().Get("Cache-Control") != "no-store" || rec.Header().Get("Pragma") != "no-cache" {
		t.Error("missing no-store/no-cache")
	}
	var envelope httpapi.APIErrorEnvelope
	if err := json.Unmarshal(rec.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	code, message := "internal_error", "internal server error"
	if status == 503 {
		code, message = "service_unavailable", "service unavailable"
	}
	if status == 403 {
		code, message = "forbidden", "access denied"
	}
	if envelope.Error.Code != code || envelope.Error.Message != message {
		t.Errorf("unsafe/wrong envelope: %s", rec.Body.String())
	}
	for _, marker := range []string{"fixture-driver-detail", "fixture-token", "fixture-partial-item", "user=fixture", "ECONN"} {
		if strings.Contains(rec.Body.String()+logs.String(), marker) {
			t.Errorf("response/audit leaked %q", marker)
		}
	}
	if db.checks != 1 || (admin && db.queries != 1) || (!admin && db.queries != 0) {
		t.Error("admin recheck/query boundary mismatch")
	}
	if db.rows != nil && !db.rows.closed {
		t.Error("rows not closed")
	}
}
