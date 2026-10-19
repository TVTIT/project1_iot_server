package digitaltwin_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type temporalFixtureIDs struct {
	RunID             string
	GatewayIDs        []string
	EntityUUIDs       []uuid.UUID
	AllUserIDs        []uuid.UUID
	PlatformAdminUser uuid.UUID
}

type temporalCleanupReporter interface {
	Helper()
	Errorf(format string, args ...any)
	Logf(format string, args ...any)
}

func sanitizeCleanupError(err error) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	for _, scheme := range []string{"postgres://", "postgresql://"} {
		if idx := strings.Index(msg, scheme); idx != -1 {
			end := strings.IndexAny(msg[idx:], " \t\n\r\"'")
			if end == -1 {
				msg = msg[:idx] + scheme + "[REDACTED]"
			} else {
				msg = msg[:idx] + scheme + "[REDACTED]" + msg[idx+end:]
			}
		}
	}
	if idx := strings.Index(msg, "password="); idx != -1 {
		end := strings.IndexAny(msg[idx:], " \t\n\r\"'")
		if end == -1 {
			msg = msg[:idx] + "password=[REDACTED]"
		} else {
			msg = msg[:idx] + "password=[REDACTED]" + msg[idx+end:]
		}
	}
	return msg
}

func execTemporalCleanup(ctx context.Context, rep temporalCleanupReporter, db fixtureDB, hasError *bool, table string, query string, args ...any) {
	rep.Helper()
	if _, err := db.Exec(ctx, query, args...); err != nil {
		*hasError = true
		rep.Errorf("cleanup %s failed: %s", table, sanitizeCleanupError(err))
	}
}

func verifyTemporalCleanupCount(ctx context.Context, rep temporalCleanupReporter, db fixtureDB, hasError *bool, table string, query string, runID string, args ...any) {
	rep.Helper()
	var remaining int
	if err := db.QueryRow(ctx, query, args...).Scan(&remaining); err != nil {
		*hasError = true
		rep.Errorf("cleanup verification for %s failed: %s", table, sanitizeCleanupError(err))
		return
	}
	if remaining != 0 {
		*hasError = true
		rep.Errorf("cleanup incomplete: %d %s row(s) remain for run %s", remaining, table, runID)
	}
}

func performTemporalFixtureCleanup(ctx context.Context, rep temporalCleanupReporter, db fixtureDB, fixture temporalFixtureIDs) bool {
	rep.Helper()
	hasError := false

	// 1. Delete twin_temporal_values
	execTemporalCleanup(ctx, rep, db, &hasError, "twin_temporal_values",
		"DELETE FROM public.twin_temporal_values WHERE entity_id = ANY($1)",
		fixture.EntityUUIDs)

	// 2. Delete twin_states
	execTemporalCleanup(ctx, rep, db, &hasError, "twin_states",
		"DELETE FROM public.twin_states WHERE entity_id = ANY($1)",
		fixture.EntityUUIDs)

	// 3. Delete twin_relationships
	execTemporalCleanup(ctx, rep, db, &hasError, "twin_relationships",
		"DELETE FROM public.twin_relationships WHERE source_entity_id = ANY($1) OR target_entity_id = ANY($1)",
		fixture.EntityUUIDs)

	// 4. Delete twin_entities
	execTemporalCleanup(ctx, rep, db, &hasError, "twin_entities",
		"DELETE FROM public.twin_entities WHERE id = ANY($1) OR gateway_id = ANY($2)",
		fixture.EntityUUIDs, fixture.GatewayIDs)

	// 5. Delete telemetry
	execTemporalCleanup(ctx, rep, db, &hasError, "telemetry",
		"DELETE FROM public.telemetry WHERE gateway_id = ANY($1)",
		fixture.GatewayIDs)

	// 6. Delete processed_messages
	execTemporalCleanup(ctx, rep, db, &hasError, "processed_messages",
		"DELETE FROM public.processed_messages WHERE gateway_id = ANY($1)",
		fixture.GatewayIDs)

	// 7. Delete sensors
	execTemporalCleanup(ctx, rep, db, &hasError, "sensors",
		"DELETE FROM public.sensors WHERE gateway_id = ANY($1)",
		fixture.GatewayIDs)

	// 8. Delete user_gateways
	execTemporalCleanup(ctx, rep, db, &hasError, "user_gateways",
		"DELETE FROM public.user_gateways WHERE gateway_id = ANY($1) OR user_id = ANY($2)",
		fixture.GatewayIDs, fixture.AllUserIDs)

	// 9. Delete gateways
	execTemporalCleanup(ctx, rep, db, &hasError, "gateways",
		"DELETE FROM public.gateways WHERE gateway_id = ANY($1)",
		fixture.GatewayIDs)

	// 10. Delete platform_admins
	execTemporalCleanup(ctx, rep, db, &hasError, "platform_admins",
		"DELETE FROM public.platform_admins WHERE user_id = ANY($1)",
		fixture.AllUserIDs)

	// 11. Delete profiles
	execTemporalCleanup(ctx, rep, db, &hasError, "profiles",
		"DELETE FROM public.profiles WHERE id = ANY($1)",
		fixture.AllUserIDs)

	// 12. Delete auth.users
	var hasAuthUsers bool
	authProbeErr := db.QueryRow(ctx, "SELECT to_regclass('auth.users') IS NOT NULL").Scan(&hasAuthUsers)
	if authProbeErr != nil {
		hasError = true
		rep.Errorf("cleanup auth.users table probe failed: %s", sanitizeCleanupError(authProbeErr))
		// Attempt deletion anyway by exact IDs since fixture seeds Auth users
		execTemporalCleanup(ctx, rep, db, &hasError, "auth.users",
			"DELETE FROM auth.users WHERE id = ANY($1)", fixture.AllUserIDs)
	} else if hasAuthUsers {
		execTemporalCleanup(ctx, rep, db, &hasError, "auth.users",
			"DELETE FROM auth.users WHERE id = ANY($1)", fixture.AllUserIDs)
	}

	// 13. Verify cleanup completeness across all fixture resources
	verifyTemporalCleanupCount(ctx, rep, db, &hasError, "twin_temporal_values",
		"SELECT COUNT(*) FROM public.twin_temporal_values WHERE entity_id = ANY($1)",
		fixture.RunID, fixture.EntityUUIDs)

	verifyTemporalCleanupCount(ctx, rep, db, &hasError, "twin_states",
		"SELECT COUNT(*) FROM public.twin_states WHERE entity_id = ANY($1)",
		fixture.RunID, fixture.EntityUUIDs)

	verifyTemporalCleanupCount(ctx, rep, db, &hasError, "twin_relationships",
		"SELECT COUNT(*) FROM public.twin_relationships WHERE source_entity_id = ANY($1) OR target_entity_id = ANY($1)",
		fixture.RunID, fixture.EntityUUIDs)

	verifyTemporalCleanupCount(ctx, rep, db, &hasError, "twin_entities",
		"SELECT COUNT(*) FROM public.twin_entities WHERE id = ANY($1) OR gateway_id = ANY($2)",
		fixture.RunID, fixture.EntityUUIDs, fixture.GatewayIDs)

	verifyTemporalCleanupCount(ctx, rep, db, &hasError, "telemetry",
		"SELECT COUNT(*) FROM public.telemetry WHERE gateway_id = ANY($1)",
		fixture.RunID, fixture.GatewayIDs)

	verifyTemporalCleanupCount(ctx, rep, db, &hasError, "processed_messages",
		"SELECT COUNT(*) FROM public.processed_messages WHERE gateway_id = ANY($1)",
		fixture.RunID, fixture.GatewayIDs)

	verifyTemporalCleanupCount(ctx, rep, db, &hasError, "sensors",
		"SELECT COUNT(*) FROM public.sensors WHERE gateway_id = ANY($1)",
		fixture.RunID, fixture.GatewayIDs)

	verifyTemporalCleanupCount(ctx, rep, db, &hasError, "user_gateways",
		"SELECT COUNT(*) FROM public.user_gateways WHERE gateway_id = ANY($1) OR user_id = ANY($2)",
		fixture.RunID, fixture.GatewayIDs, fixture.AllUserIDs)

	verifyTemporalCleanupCount(ctx, rep, db, &hasError, "gateways",
		"SELECT COUNT(*) FROM public.gateways WHERE gateway_id = ANY($1)",
		fixture.RunID, fixture.GatewayIDs)

	verifyTemporalCleanupCount(ctx, rep, db, &hasError, "platform_admins",
		"SELECT COUNT(*) FROM public.platform_admins WHERE user_id = ANY($1)",
		fixture.RunID, fixture.AllUserIDs)

	verifyTemporalCleanupCount(ctx, rep, db, &hasError, "profiles",
		"SELECT COUNT(*) FROM public.profiles WHERE id = ANY($1)",
		fixture.RunID, fixture.AllUserIDs)

	var hasAuthUsersVerify bool
	if err := db.QueryRow(ctx, "SELECT to_regclass('auth.users') IS NOT NULL").Scan(&hasAuthUsersVerify); err != nil {
		hasError = true
		rep.Errorf("cleanup auth.users verification probe failed: %s", sanitizeCleanupError(err))
	} else if hasAuthUsersVerify {
		verifyTemporalCleanupCount(ctx, rep, db, &hasError, "auth.users",
			"SELECT COUNT(*) FROM auth.users WHERE id = ANY($1)",
			fixture.RunID, fixture.AllUserIDs)
	}

	if !hasError {
		rep.Logf("Fixture cleanup verified: 0 remaining rows for run %s", fixture.RunID)
		return true
	}
	return false
}

// ---------------------------------------------------------------------------
// Unit tests verifying cleanup reporting, error checks, and zero-row logic
// ---------------------------------------------------------------------------

type fakeTemporalReporter struct {
	errors []string
	logs   []string
}

func (f *fakeTemporalReporter) Helper() {}

func (f *fakeTemporalReporter) Errorf(format string, args ...any) {
	f.errors = append(f.errors, fmt.Sprintf(format, args...))
}

func (f *fakeTemporalReporter) Logf(format string, args ...any) {
	f.logs = append(f.logs, fmt.Sprintf(format, args...))
}

type fakeTemporalRow struct {
	intVal  int
	boolVal bool
	err     error
}

func (r fakeTemporalRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) == 1 {
		if p, ok := dest[0].(*int); ok {
			*p = r.intVal
			return nil
		}
		if p, ok := dest[0].(*bool); ok {
			*p = r.boolVal
			return nil
		}
	}
	return nil
}

type fakeTemporalDB struct {
	execErrFn     func(sql string) error
	queryRowErrFn func(sql string) pgx.Row
}

func (d *fakeTemporalDB) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	if d.execErrFn != nil {
		if err := d.execErrFn(sql); err != nil {
			return pgconn.CommandTag{}, err
		}
	}
	return pgconn.CommandTag{}, nil
}

func (d *fakeTemporalDB) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	if d.queryRowErrFn != nil {
		return d.queryRowErrFn(sql)
	}
	if strings.Contains(sql, "to_regclass") {
		return fakeTemporalRow{boolVal: true}
	}
	return fakeTemporalRow{intVal: 0}
}

func testFixtureIDs() temporalFixtureIDs {
	return temporalFixtureIDs{
		RunID:             "testrun",
		GatewayIDs:        []string{"gw_a_test", "gw_b_test"},
		EntityUUIDs:       []uuid.UUID{uuid.New(), uuid.New()},
		AllUserIDs:        []uuid.UUID{uuid.New(), uuid.New()},
		PlatformAdminUser: uuid.New(),
	}
}

func TestTemporalFixtureCleanup_AuthProbeError(t *testing.T) {
	rep := &fakeTemporalReporter{}
	db := &fakeTemporalDB{
		queryRowErrFn: func(sql string) pgx.Row {
			if strings.Contains(sql, "to_regclass") {
				return fakeTemporalRow{err: errors.New("simulated auth.users probe error (postgres://user:pass@host/db)")}
			}
			return fakeTemporalRow{intVal: 0}
		},
	}

	success := performTemporalFixtureCleanup(context.Background(), rep, db, testFixtureIDs())
	if success {
		t.Fatalf("expected cleanup verification to return false on auth probe failure, got true")
	}
	if len(rep.errors) == 0 {
		t.Fatalf("expected errors to be reported for auth probe failure, got none")
	}

	var foundProbeErr bool
	for _, errStr := range rep.errors {
		if strings.Contains(errStr, "auth.users table probe failed") || strings.Contains(errStr, "auth.users verification probe failed") {
			foundProbeErr = true
		}
		if strings.Contains(errStr, "pass@host") {
			t.Errorf("error revealed credentials: %s", errStr)
		}
	}
	if !foundProbeErr {
		t.Fatalf("expected error mentioning auth.users probe failure, got %v", rep.errors)
	}

	for _, logStr := range rep.logs {
		if strings.Contains(logStr, "Fixture cleanup verified") {
			t.Fatalf("must not log success on auth probe failure, got log: %s", logStr)
		}
	}
}

func TestTemporalFixtureCleanup_CountScanError(t *testing.T) {
	rep := &fakeTemporalReporter{}
	db := &fakeTemporalDB{
		queryRowErrFn: func(sql string) pgx.Row {
			if strings.Contains(sql, "to_regclass") {
				return fakeTemporalRow{boolVal: true}
			}
			if strings.Contains(sql, "public.gateways") {
				return fakeTemporalRow{err: errors.New("connection failed: password=secretpass")}
			}
			return fakeTemporalRow{intVal: 0}
		},
	}

	success := performTemporalFixtureCleanup(context.Background(), rep, db, testFixtureIDs())
	if success {
		t.Fatalf("expected cleanup verification to return false on count query scan failure, got true")
	}
	if len(rep.errors) == 0 {
		t.Fatalf("expected errors to be reported for count scan failure, got none")
	}

	var foundGatewaysErr bool
	for _, errStr := range rep.errors {
		if strings.Contains(errStr, "cleanup verification for gateways failed") {
			foundGatewaysErr = true
		}
		if strings.Contains(errStr, "secretpass") {
			t.Errorf("error revealed credentials: %s", errStr)
		}
	}
	if !foundGatewaysErr {
		t.Fatalf("expected error mentioning gateways verification failure, got %v", rep.errors)
	}

	for _, logStr := range rep.logs {
		if strings.Contains(logStr, "Fixture cleanup verified") {
			t.Fatalf("must not log success on count scan failure, got log: %s", logStr)
		}
	}
}

func TestTemporalFixtureCleanup_ResidualRows(t *testing.T) {
	rep := &fakeTemporalReporter{}
	db := &fakeTemporalDB{
		queryRowErrFn: func(sql string) pgx.Row {
			if strings.Contains(sql, "to_regclass") {
				return fakeTemporalRow{boolVal: true}
			}
			if strings.Contains(sql, "public.twin_states") {
				return fakeTemporalRow{intVal: 2}
			}
			return fakeTemporalRow{intVal: 0}
		},
	}

	success := performTemporalFixtureCleanup(context.Background(), rep, db, testFixtureIDs())
	if success {
		t.Fatalf("expected cleanup verification to return false on residual rows, got true")
	}
	if len(rep.errors) == 0 {
		t.Fatalf("expected errors to be reported for residual rows, got none")
	}

	var foundResidualErr bool
	for _, errStr := range rep.errors {
		if strings.Contains(errStr, "cleanup incomplete: 2 twin_states row(s) remain") {
			foundResidualErr = true
		}
	}
	if !foundResidualErr {
		t.Fatalf("expected error mentioning residual twin_states rows, got %v", rep.errors)
	}

	for _, logStr := range rep.logs {
		if strings.Contains(logStr, "Fixture cleanup verified") {
			t.Fatalf("must not log success when rows remain, got log: %s", logStr)
		}
	}
}

func TestTemporalFixtureCleanup_VerifiedZeroResources(t *testing.T) {
	rep := &fakeTemporalReporter{}
	db := &fakeTemporalDB{
		queryRowErrFn: func(sql string) pgx.Row {
			if strings.Contains(sql, "to_regclass") {
				return fakeTemporalRow{boolVal: true}
			}
			return fakeTemporalRow{intVal: 0}
		},
	}

	success := performTemporalFixtureCleanup(context.Background(), rep, db, testFixtureIDs())
	if !success {
		t.Fatalf("expected cleanup verification to succeed with zero residual rows, got false, errors: %v", rep.errors)
	}
	if len(rep.errors) != 0 {
		t.Fatalf("expected no errors on full verification, got: %v", rep.errors)
	}

	var foundSuccessLog bool
	for _, logStr := range rep.logs {
		if strings.Contains(logStr, "Fixture cleanup verified: 0 remaining rows for run testrun") {
			foundSuccessLog = true
		}
	}
	if !foundSuccessLog {
		t.Fatalf("expected success log 'Fixture cleanup verified: 0 remaining rows for run testrun', got logs: %v", rep.logs)
	}
}

func TestTemporalFixtureCleanup_VerifiedZeroResources_WithoutAuthUsers(t *testing.T) {
	rep := &fakeTemporalReporter{}
	db := &fakeTemporalDB{
		queryRowErrFn: func(sql string) pgx.Row {
			if strings.Contains(sql, "to_regclass") {
				return fakeTemporalRow{boolVal: false}
			}
			return fakeTemporalRow{intVal: 0}
		},
	}

	success := performTemporalFixtureCleanup(context.Background(), rep, db, testFixtureIDs())
	if !success {
		t.Fatalf("expected cleanup verification to succeed when auth.users does not exist, got false, errors: %v", rep.errors)
	}
	if len(rep.errors) != 0 {
		t.Fatalf("expected no errors, got: %v", rep.errors)
	}

	var foundSuccessLog bool
	for _, logStr := range rep.logs {
		if strings.Contains(logStr, "Fixture cleanup verified: 0 remaining rows for run testrun") {
			foundSuccessLog = true
		}
	}
	if !foundSuccessLog {
		t.Fatalf("expected success log, got logs: %v", rep.logs)
	}
}

func TestTemporalFixtureCleanup_SanitizeErrors(t *testing.T) {
	rawErr := errors.New("connection failed to postgres://admin:supersecret@127.0.0.1:5432/iot_db with password=anothersecret in pool")
	sanitized := sanitizeCleanupError(rawErr)
	if strings.Contains(sanitized, "supersecret") {
		t.Fatalf("sanitized error leaked connection password: %s", sanitized)
	}
	if strings.Contains(sanitized, "anothersecret") {
		t.Fatalf("sanitized error leaked password parameter: %s", sanitized)
	}
	if !strings.Contains(sanitized, "postgres://[REDACTED]") {
		t.Fatalf("expected redacted url in sanitized error: %s", sanitized)
	}
	if !strings.Contains(sanitized, "password=[REDACTED]") {
		t.Fatalf("expected redacted password in sanitized error: %s", sanitized)
	}
}
