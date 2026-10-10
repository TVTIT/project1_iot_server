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

type ownerFailureDatabase struct {
	tx       *ownerFailureTx
	beginErr error
}

func (d ownerFailureDatabase) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) {
	return d.tx, d.beginErr
}

type ownerFailureTx struct {
	pgx.Tx
	phase                             string
	cause                             error
	role                              gateway.Role
	reads, writes, commits, rollbacks int
}

type ownerFailureRow func(...any) error

func (r ownerFailureRow) Scan(dest ...any) error { return r(dest...) }

func (tx *ownerFailureTx) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	tx.reads++
	return ownerFailureRow(func(dest ...any) error {
		switch {
		case strings.Contains(sql, "SELECT name FROM public.gateways"):
			if tx.phase == "parent" {
				return tx.cause
			}
			*dest[0].(*string) = "Fixture parent"
		case strings.Contains(sql, "SELECT role FROM public.user_gateways"):
			if tx.phase == "membership" {
				return tx.cause
			}
			*dest[0].(*gateway.Role) = tx.role
		case strings.Contains(sql, "SELECT e.id"):
			if tx.phase == "graph" {
				return tx.cause
			}
			*dest[0].(*uuid.UUID) = uuid.New()
		case strings.Contains(sql, "INSERT INTO public.sensors"):
			tx.writes++
			stamp := time.Now()
			*dest[0].(**time.Time) = &stamp
		case strings.Contains(sql, "INSERT INTO public.twin_entities"):
			tx.writes++
			*dest[0].(*uuid.UUID) = uuid.New()
		default:
			return errors.New("unexpected fixture query")
		}
		return nil
	})
}
func (tx *ownerFailureTx) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	tx.writes++
	return pgconn.NewCommandTag("INSERT 0 1"), nil
}
func (tx *ownerFailureTx) Commit(context.Context) error {
	tx.commits++
	return tx.cause
}
func (tx *ownerFailureTx) Rollback(context.Context) error {
	tx.rollbacks++
	return pgx.ErrTxClosed // Cleanup attempt is not proof of a successful rollback.
}

func TestOwnerDatabaseFailureHTTP(t *testing.T) {
	cfg, err := pgconn.ParseConfig("host=localhost user=fixture database=fixture sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	cfg.LookupFunc = func(context.Context, string) ([]string, error) { return []string{"127.0.0.1"}, nil }
	cfg.DialFunc = func(context.Context, string, string) (net.Conn, error) {
		return nil, &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
	}
	_, connectErr := pgconn.ConnectConfig(context.Background(), cfg)
	var connect *pgconn.ConnectError
	if !errors.As(connectErr, &connect) {
		t.Fatal("expected injected ConnectError")
	}
	for _, tc := range []struct {
		name   string
		cause  error
		status int
	}{
		{"closed", pgconn.ErrConnClosed, 503},
		{"pool", puddle.ErrClosedPool, 503},
		{"connect", connectErr, 503},
		{"network", &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}, 503},
		{"eof", io.EOF, 503},
		{"unexpected eof", io.ErrUnexpectedEOF, 503},
		{"shutdown", &pgconn.PgError{Code: "57P01", Message: "fixture-driver-detail"}, 503},
		{"connection", &pgconn.PgError{Code: "08006"}, 503},
		{"deadline", context.DeadlineExceeded, 503},
		{"cancel", context.Canceled, 503},
		{"logical sql", &pgconn.PgError{Code: "42703"}, 500},
		{"constraint", &pgconn.PgError{Code: "23514"}, 500},
		{"protocol", &pgconn.PgError{Code: "08P01"}, 500},
		{"permission", &pgconn.PgError{Code: "42501"}, 500},
		{"scan malformed", &pgx.ScanArgError{ColumnIndex: 0, Err: errors.New("fixture-driver-detail malformed value")}, 500},
		{"scan decoder eof", &pgx.ScanArgError{ColumnIndex: 0, Err: io.ErrUnexpectedEOF}, 500},
		{"programming", errors.New("fixture-driver-detail conn closed"), 500},
	} {
		for _, phase := range []string{"begin", "parent", "membership", "graph", "commit"} {
			t.Run(tc.name+"/"+phase, func(t *testing.T) {
				assertOwnerDatabaseFailureHTTP(t, phase, fmt.Errorf("fixture-driver-detail: %w", tc.cause), gateway.RoleOwner, tc.status)
			})
		}
	}
	for _, phase := range []string{"parent", "membership"} {
		t.Run("absent/"+phase, func(t *testing.T) { assertOwnerDatabaseFailureHTTP(t, phase, pgx.ErrNoRows, gateway.RoleOwner, 404) })
	}
	for _, role := range []gateway.Role{gateway.RoleOperator, gateway.RoleViewer} {
		t.Run(string(role), func(t *testing.T) { assertOwnerDatabaseFailureHTTP(t, "denied", nil, role, 403) })
	}
	t.Run("inconsistent graph", func(t *testing.T) { assertOwnerDatabaseFailureHTTP(t, "graph", pgx.ErrNoRows, gateway.RoleOwner, 500) })
	t.Run("representation conflict", func(t *testing.T) {
		assertOwnerDatabaseFailureHTTP(t, "graph", gateway.ErrProvisioningConflict, gateway.RoleOwner, 409)
	})
}

func assertOwnerDatabaseFailureHTTP(t *testing.T, phase string, cause error, role gateway.Role, status int) {
	t.Helper()
	tx := &ownerFailureTx{phase: phase, cause: cause, role: role}
	db := ownerFailureDatabase{tx: tx}
	if phase == "begin" {
		db.beginErr = cause
	}
	repo, err := gateway.NewPostgresOwnerSensorProvisioningRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	service, err := gateway.NewOwnerSensorProvisioningService(repo, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
	actor := uuid.New()
	deps.TokenVerifier = tokenVerifierFunc(func(context.Context, string) (auth.Principal, error) {
		return auth.Principal{UserID: actor, Role: "authenticated"}, nil
	})
	deps.OwnerSensorProvisioner = ownerSensorProvisionerFunc(func(ctx context.Context, actor uuid.UUID, input gateway.ProvisionSensorInput) (gateway.ProvisionSensorResult, error) {
		result, err := service.ProvisionSensorForOwner(ctx, actor, input)
		if cause != nil && cause != pgx.ErrNoRows && !errors.Is(err, cause) {
			t.Error("lost original cause")
		}
		var pg *pgconn.PgError
		if errors.As(cause, &pg) {
			var preserved *pgconn.PgError
			if !errors.As(err, &preserved) || preserved != pg {
				t.Error("lost driver type")
			}
		}
		if result.GatewayID != "" || result.Created {
			t.Error("failure returned successful result")
		}
		return result, err
	})
	var logs bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(old) })
	router, err := NewRouter(deps)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPut, "/v1/gateways/fixture-gw/sensors/fixture-sensor", strings.NewReader(`{"name":"Fixture sensor"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer fixture-token")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != status {
		t.Errorf("status=%d want=%d", rec.Code, status)
	}
	if rec.Header().Get("Cache-Control") != "no-store" {
		t.Error("missing no-store")
	}
	var envelope httpapi.APIErrorEnvelope
	if err := json.Unmarshal(rec.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	codes := map[int]string{503: "service_unavailable", 500: "internal_error", 403: "forbidden", 404: "not_found", 409: "conflict"}
	messages := map[int]string{503: "service unavailable", 500: "internal server error", 403: "access denied", 404: "resource not found", 409: "resource conflict"}
	if envelope.Error.Code != codes[status] || envelope.Error.Message != messages[status] {
		t.Errorf("wrong/unsafe envelope: %s", rec.Body.String())
	}
	for _, marker := range []string{"fixture-driver-detail", "fixture-token", "user=fixture", "entity_id", "created_at"} {
		if strings.Contains(rec.Body.String()+logs.String(), marker) {
			t.Errorf("leaked %q", marker)
		}
	}
	if phase == "commit" {
		if tx.writes != 4 || tx.commits != 1 {
			t.Error("commit fixture did not execute authorized graph writes")
		}
		// No assertion of zero persisted writes: commit confirmation loss is ambiguous.
	} else if tx.writes != 0 || tx.commits != 0 {
		t.Error("writes/commit before authorization or failed admission")
	}
	if phase == "begin" {
		if tx.reads != 0 || tx.rollbacks != 0 {
			t.Error("used nonexistent transaction")
		}
	} else if tx.rollbacks != 1 {
		t.Error("missing bounded cleanup attempt")
	}
}
