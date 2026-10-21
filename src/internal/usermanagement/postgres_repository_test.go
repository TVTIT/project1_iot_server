package usermanagement_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"iot-platform/internal/gateway"
	"iot-platform/internal/usermanagement"
)

func resolveDatabaseURLs(t *testing.T) (string, string) {
	t.Helper()
	if os.Getenv("USER_MGMT_TEST_NATIVE") != "1" {
		t.Skip("set USER_MGMT_TEST_NATIVE=1 to run native DB/Auth tests")
	}

	setupURL := os.Getenv("USER_MGMT_TEST_SETUP_URL")
	backendURL := os.Getenv("USER_MGMT_TEST_BACKEND_URL")
	if setupURL != "" && backendURL != "" {
		return setupURL, backendURL
	}

	dir, err := os.Getwd()
	if err != nil {
		return "", ""
	}

	var envPath string
	for {
		candidate := filepath.Join(dir, ".env")
		if _, statErr := os.Stat(candidate); statErr == nil {
			envPath = candidate
			break
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}

	if envPath == "" {
		return "", ""
	}

	content, err := os.ReadFile(envPath)
	if err != nil {
		return "", ""
	}

	envMap := make(map[string]string)
	for _, line := range strings.Split(string(content), "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "#") || !strings.Contains(trimmed, "=") {
			continue
		}
		parts := strings.SplitN(trimmed, "=", 2)
		envMap[strings.TrimSpace(parts[0])] = strings.Trim(strings.TrimSpace(parts[1]), `"'`)
	}

	pgUser := envMap["POSTGRES_USER"]
	if pgUser == "" {
		pgUser = "postgres"
	}
	pgPass := envMap["POSTGRES_PASSWORD"]
	pgDB := envMap["POSTGRES_DB"]
	if pgDB == "" {
		pgDB = "iot_platform"
	}
	pgPort := envMap["POSTGRES_PORT"]
	if pgPort == "" {
		pgPort = "5432"
	}
	backendPass := envMap["BACKEND_DB_PASSWORD"]

	if pgPass != "" {
		setupURL = fmt.Sprintf("postgres://%s:%s@127.0.0.1:%s/%s?sslmode=disable", pgUser, pgPass, pgPort, pgDB)
	}
	if backendPass != "" {
		backendURL = fmt.Sprintf("postgres://iot_backend_app:%s@127.0.0.1:%s/%s?sslmode=disable", backendPass, pgPort, pgDB)
	}

	if setupURL == "" || backendURL == "" {
		t.Fatal("native test requested but database configuration is missing")
	}
	return setupURL, backendURL
}

func TestPostgresRemoveGatewayUserNative(t *testing.T) {
	setupURL, appURL := resolveDatabaseURLs(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	setup, err := pgxpool.New(ctx, setupURL)
	if err != nil {
		t.Fatal("setup pool unavailable")
	}
	defer setup.Close()
	appCfg, err := pgxpool.ParseConfig(appURL)
	if err != nil {
		t.Fatal("invalid application pool configuration")
	}
	trace := &membershipWriteTrace{}
	appCfg.ConnConfig.Tracer = trace
	app, err := pgxpool.NewWithConfig(ctx, appCfg)
	if err != nil {
		t.Fatal("app pool unavailable")
	}
	defer app.Close()
	var role string
	var super bool
	if err := app.QueryRow(ctx, "SELECT current_user, rolsuper FROM pg_roles WHERE rolname=current_user").Scan(&role, &super); err != nil || role != "iot_backend_app" || super {
		t.Fatal("test requires runtime non-superuser principal")
	}
	repo, err := usermanagement.NewPostgresRepository(app)
	if err != nil {
		t.Fatal(err)
	}
	ids := []uuid.UUID{uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	actor, owner, target, other := ids[0], ids[1], ids[2], ids[3]
	gw := "gw_delete_" + uuid.NewString()[:8]
	gws := []string{gw, gw + "_other"}
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := setup.Exec(ctx, sql, args...); err != nil {
			t.Fatal(err)
		}
	}
	defer func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer ccancel()
		for _, table := range []string{"twin_states", "twin_entities", "user_gateways", "gateways"} {
			q := "DELETE FROM public." + table + " WHERE gateway_id = ANY($1)"
			if table == "twin_states" {
				q = "DELETE FROM public.twin_states WHERE entity_id IN (SELECT id FROM public.twin_entities WHERE gateway_id=ANY($1))"
			}
			checkedCleanup(t, setup, cctx, q, gws)
		}
		checkedCleanup(t, setup, cctx, "DELETE FROM public.platform_admins WHERE user_id=ANY($1)", ids)
		checkedCleanup(t, setup, cctx, "DELETE FROM public.profiles WHERE id=ANY($1)", ids)
		checkedCleanup(t, setup, cctx, "DELETE FROM auth.users WHERE id=ANY($1)", ids)
		var remaining int
		if err := setup.QueryRow(cctx, `SELECT (SELECT count(*) FROM public.gateways WHERE gateway_id=ANY($1)) + (SELECT count(*) FROM public.user_gateways WHERE gateway_id=ANY($1) OR user_id=ANY($2)) + (SELECT count(*) FROM public.twin_entities WHERE gateway_id=ANY($1)) + (SELECT count(*) FROM public.profiles WHERE id=ANY($2)) + (SELECT count(*) FROM auth.users WHERE id=ANY($2)) + (SELECT count(*) FROM public.platform_admins WHERE user_id=ANY($2))`, gws, ids).Scan(&remaining); err != nil || remaining != 0 {
			t.Error("removal fixture cleanup incomplete")
		}
	}()
	for _, id := range ids {
		exec("INSERT INTO auth.users(id,email,raw_user_meta_data,created_at,aud,role) VALUES($1,$2,'{\"full_name\":\"Delete fixture\"}',now(),'authenticated','authenticated')", id, id.String()+"@example.com")
	}
	exec("INSERT INTO public.platform_admins(user_id) VALUES($1),($2)", actor, target)
	prov, err := gateway.NewPostgresProvisioningRepository(app)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range gws {
		if _, err := prov.ProvisionGateway(ctx, actor, gateway.ProvisionGatewayInput{GatewayID: id, Name: "Delete fixture", OwnerUserID: owner}); err != nil {
			t.Fatal(err)
		}
	}
	exec("INSERT INTO public.user_gateways(user_id,gateway_id,role) VALUES($1,$3,'viewer'),($2,$4,'operator')", other, target, gw, gws[1])
	// Full-row snapshots, including private account rows, are compared but never printed.
	snapshot := func(exclude bool) string {
		t.Helper()
		var value string
		err := setup.QueryRow(ctx, `SELECT jsonb_build_object(
		'accounts',(SELECT jsonb_agg(to_jsonb(x) ORDER BY id) FROM auth.users x WHERE id=ANY($1)),
		'profiles',(SELECT jsonb_agg(to_jsonb(x) ORDER BY id) FROM public.profiles x WHERE id=ANY($1)),
		'admins',(SELECT jsonb_agg(to_jsonb(x) ORDER BY user_id) FROM public.platform_admins x WHERE user_id=ANY($1)),
		'members',(SELECT jsonb_agg(to_jsonb(x) ORDER BY gateway_id,user_id) FROM public.user_gateways x WHERE gateway_id=ANY($2) AND NOT ($3 AND gateway_id=$4 AND user_id=$5)),
		'gateways',(SELECT jsonb_agg(to_jsonb(x) ORDER BY gateway_id) FROM public.gateways x WHERE gateway_id=ANY($2)),
		'entities',(SELECT jsonb_agg(to_jsonb(x) ORDER BY id) FROM public.twin_entities x WHERE gateway_id=ANY($2)),
		'states',(SELECT jsonb_agg(to_jsonb(x) ORDER BY entity_id) FROM public.twin_states x WHERE entity_id IN (SELECT id FROM public.twin_entities WHERE gateway_id=ANY($2))))::text`, ids, gws, exclude, gw, target).Scan(&value)
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	for _, membershipRole := range []string{"viewer", "operator"} {
		t.Run(membershipRole, func(t *testing.T) {
			exec("INSERT INTO public.user_gateways(user_id,gateway_id,role) VALUES($1,$2,$3)", target, gw, membershipRole)
			before := snapshot(true)
			if err := repo.RemoveGatewayUser(ctx, actor, gw, target); err != nil {
				t.Fatalf("remove %s: %v", membershipRole, err)
			}
			var count int
			if err := setup.QueryRow(ctx, "SELECT count(*) FROM public.user_gateways WHERE gateway_id=$1 AND user_id=$2", gw, target).Scan(&count); err != nil || count != 0 {
				t.Fatal("target membership not removed")
			}
			if snapshot(true) != before {
				t.Error("unrelated resource/account/membership changed")
			}
			if err := repo.RemoveGatewayUser(ctx, actor, gw, target); err != nil {
				t.Fatal(err)
			}
			if snapshot(true) != before {
				t.Error("repeat delete changed data")
			}
		})
		// A pre-grant run intentionally fails above; reset only this owned target row.
		exec("DELETE FROM public.user_gateways WHERE gateway_id=$1 AND user_id=$2", gw, target)
	}
	for _, tc := range []struct {
		name   string
		actor  uuid.UUID
		gw     string
		target uuid.UUID
		want   error
	}{
		{"absent membership", actor, gw, target, nil}, {"absent account", actor, gw, uuid.New(), nil},
		{"missing gateway", actor, gw + "_missing", target, usermanagement.ErrGatewayNotFound},
		{"owner", actor, gw, owner, usermanagement.ErrOwnerTransferRequired},
		{"non admin", other, gw, other, usermanagement.ErrActorNotAdmin},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := snapshot(false)
			writes := trace.writes.Load()
			if err := repo.RemoveGatewayUser(ctx, tc.actor, tc.gw, tc.target); !errors.Is(err, tc.want) {
				t.Errorf("got %v want %v", err, tc.want)
			}
			if snapshot(false) != before {
				t.Error("denial/no-op changed data")
			}
			if trace.writes.Load() != writes {
				t.Error("denial/no-op executed a mutation statement")
			}
		})
	}
	// SQL failure after row deletion must roll back. Function/trigger are scoped to this run.
	for _, deferred := range []bool{false, true} {
		t.Run(fmt.Sprintf("SQL rollback deferred=%t", deferred), func(t *testing.T) {
			exec("INSERT INTO public.user_gateways(user_id,gateway_id,role) VALUES($1,$2,'viewer') ON CONFLICT(user_id,gateway_id) DO NOTHING", target, gw)
			before := snapshot(false)
			fn := "delete_failure_" + strings.ReplaceAll(uuid.NewString(), "-", "")
			defer cleanupMembershipFailureObjects(t, setup, fn)
			exec(fmt.Sprintf(`CREATE FUNCTION public.%s() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'owned delete failure' USING ERRCODE='P0001'; END $$`, fn))
			triggerKind, timing := "", ""
			if deferred {
				triggerKind, timing = "CONSTRAINT ", "DEFERRABLE INITIALLY DEFERRED "
			}
			exec(fmt.Sprintf(`CREATE %sTRIGGER %s AFTER DELETE ON public.user_gateways %sFOR EACH ROW WHEN (OLD.gateway_id = '%s') EXECUTE FUNCTION public.%s()`, triggerKind, fn, timing, gw, fn))
			err := repo.RemoveGatewayUser(ctx, actor, gw, target)
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) || pgErr.Code != "P0001" {
				t.Fatalf("expected SQL failure, got %v", err)
			}
			if snapshot(false) != before {
				t.Error("failed DELETE was not rolled back")
			}
		})
	}
	t.Run("failure objects cleaned after operation cancellation", func(t *testing.T) {
		fn := "delete_failure_" + strings.ReplaceAll(uuid.NewString(), "-", "")
		// Register before setup, including partial setup failure.
		defer cleanupMembershipFailureObjects(t, setup, fn)
		opCtx, opCancel := context.WithCancel(ctx)
		defer opCancel()
		if _, err := setup.Exec(opCtx, fmt.Sprintf(`CREATE FUNCTION public.%s() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN OLD; END $$`, fn)); err != nil {
			t.Fatal(err)
		}
		if _, err := setup.Exec(opCtx, fmt.Sprintf(`CREATE TRIGGER %s AFTER DELETE ON public.user_gateways FOR EACH ROW WHEN (OLD.gateway_id = '%s') EXECUTE FUNCTION public.%s()`, fn, gw, fn)); err != nil {
			t.Fatal(err)
		}
		opCancel()
		if _, err := setup.Exec(opCtx, "SELECT 1"); !errors.Is(err, context.Canceled) {
			t.Fatalf("operation context was not cancelled: %v", err)
		}
		cleanupMembershipFailureObjects(t, setup, fn)
		var absent bool
		if err := setup.QueryRow(ctx, `SELECT to_regprocedure($1) IS NULL AND NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid='public.user_gateways'::regclass AND tgname=$2)`, "public."+fn+"()", fn).Scan(&absent); err != nil || !absent {
			t.Error("failure-injection objects remain after cancelled-operation cleanup")
		}
	})
	t.Run("owner transfer locks out removal", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		locked, release := make(chan struct{}), make(chan struct{})
		cfg, err := pgxpool.ParseConfig(appURL)
		if err != nil {
			t.Fatal("invalid app pool config")
		}
		cfg.ConnConfig.Tracer = membershipTransferBarrier{locked: locked, release: release}
		transferPool, err := pgxpool.NewWithConfig(ctx, cfg)
		if err != nil {
			t.Fatal("transfer pool unavailable")
		}
		// Closing must occur after release, including on assertion failure.
		defer transferPool.Close()
		defer close(release)
		transferRepo, err := usermanagement.NewPostgresRepository(transferPool)
		if err != nil {
			t.Fatal(err)
		}
		transferDone := make(chan error, 1)
		go func() { transferDone <- transferRepo.TransferGatewayOwner(ctx, actor, gw, target) }()
		select {
		case <-locked:
		case <-ctx.Done():
			t.Fatal("transfer never acquired parent lock")
		}
		deleteCfg, err := pgxpool.ParseConfig(appURL)
		if err != nil {
			t.Fatal("invalid delete pool config")
		}
		deleteCfg.ConnConfig.RuntimeParams["application_name"] = gw + "_removal"
		deletePool, err := pgxpool.NewWithConfig(ctx, deleteCfg)
		if err != nil {
			t.Fatal("delete pool unavailable")
		}
		defer deletePool.Close()
		// Cancel in-flight calls before pool Close on every exit path.
		defer cancel()
		deleteRepo, err := usermanagement.NewPostgresRepository(deletePool)
		if err != nil {
			t.Fatal(err)
		}
		deleteDone := make(chan error, 1)
		go func() { deleteDone <- deleteRepo.RemoveGatewayUser(ctx, actor, gw, target) }()
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			var blocked bool
			if err := setup.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND cardinality(pg_blocking_pids(pid))>0)`, gw+"_removal").Scan(&blocked); err != nil {
				t.Fatal(err)
			}
			if blocked {
				break
			}
			select {
			case err := <-deleteDone:
				t.Fatalf("removal did not wait on parent lock: %v", err)
			case <-ticker.C:
			case <-ctx.Done():
				t.Fatal("did not observe blocked removal")
			}
		}
		select {
		case release <- struct{}{}:
		case <-ctx.Done():
			t.Fatal("transfer release timed out")
		}
		if err := <-transferDone; err != nil {
			t.Fatal(err)
		}
		if err := <-deleteDone; !errors.Is(err, usermanagement.ErrOwnerTransferRequired) {
			t.Fatalf("must not delete newly transferred owner: %v", err)
		}
		var membership string
		if err := setup.QueryRow(ctx, "SELECT role FROM public.user_gateways WHERE gateway_id=$1 AND user_id=$2", gw, target).Scan(&membership); err != nil || membership != "owner" {
			t.Fatal("new owner not preserved")
		}
	})
}

// Cleanup must survive cancellation/expiry of the test operation and continue
// after individual failures. Names are generated by this fixture, never supplied externally.
func cleanupMembershipFailureObjects(t *testing.T, pool *pgxpool.Pool, name string) {
	t.Helper()
	for _, query := range []string{
		"DROP TRIGGER IF EXISTS " + name + " ON public.user_gateways",
		"DROP FUNCTION IF EXISTS public." + name + "()",
	} {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := pool.Exec(ctx, query)
		cancel()
		if err != nil {
			t.Errorf("failure-injection object cleanup failed: %v", err)
		}
	}
}

type membershipTransferQueryKey struct{}
type membershipWriteTrace struct{ writes atomic.Int64 }

func (r *membershipWriteTrace) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	q := strings.ToUpper(strings.TrimSpace(data.SQL))
	if strings.HasPrefix(q, "DELETE ") || strings.HasPrefix(q, "INSERT ") || strings.HasPrefix(q, "UPDATE ") {
		r.writes.Add(1)
	}
	return ctx
}
func (*membershipWriteTrace) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

type membershipTransferBarrier struct {
	locked  chan struct{}
	release chan struct{}
}

func (b membershipTransferBarrier) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, membershipTransferQueryKey{}, strings.HasPrefix(data.SQL, "SELECT name FROM public.gateways"))
}
func (b membershipTransferBarrier) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	if held, _ := ctx.Value(membershipTransferQueryKey{}).(bool); held && data.Err == nil {
		close(b.locked)
		select {
		case <-b.release:
		case <-ctx.Done():
		}
	}
}

func TestPostgresRepositoryNative(t *testing.T) {
	setupURL, backendURL := resolveDatabaseURLs(t)
	if setupURL == "" || backendURL == "" {
		t.Fatal("native database test requested but configuration missing")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	setupPool, err := pgxpool.New(ctx, setupURL)
	if err != nil {
		t.Fatalf("failed to connect setup pool: %v", err)
	}
	defer setupPool.Close()

	backendPool, err := pgxpool.New(ctx, backendURL)
	if err != nil {
		t.Fatalf("failed to connect backend pool: %v", err)
	}
	defer backendPool.Close()

	var appRole string
	if err := backendPool.QueryRow(ctx, "SELECT current_user").Scan(&appRole); err != nil || appRole != "iot_backend_app" {
		t.Fatalf("backendPool current_user = %q, want iot_backend_app", appRole)
	}

	repo, err := usermanagement.NewPostgresRepository(backendPool)
	if err != nil {
		t.Fatalf("NewPostgresRepository failed: %v", err)
	}

	// Fixture data IDs
	actorAdminID := uuid.New()
	targetUserID := uuid.New()
	nonAdminActorID := uuid.New()
	orphanAuthUserID := uuid.New()
	testGatewayID := "gw_u2_fixture_" + strings.ReplaceAll(uuid.New().String()[:8], "-", "")

	now := time.Now().UTC()
	execSetup := func(query string, args ...any) {
		t.Helper()
		if _, err := setupPool.Exec(ctx, query, args...); err != nil {
			t.Fatalf("setup SQL failed: %v\nquery: %s", err, query)
		}
	}

	// Cleanup hook
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		checkedCleanup(t, setupPool, cleanupCtx, "DELETE FROM public.user_gateways WHERE gateway_id = $1", testGatewayID)
		checkedCleanup(t, setupPool, cleanupCtx, "DELETE FROM public.gateways WHERE gateway_id = $1", testGatewayID)
		ids := []uuid.UUID{actorAdminID, targetUserID, nonAdminActorID, orphanAuthUserID}
		checkedCleanup(t, setupPool, cleanupCtx, "DELETE FROM public.platform_admins WHERE user_id = ANY($1)", ids)
		checkedCleanup(t, setupPool, cleanupCtx, "DELETE FROM public.profiles WHERE id = ANY($1)", ids)
		checkedCleanup(t, setupPool, cleanupCtx, "DELETE FROM auth.users WHERE id = ANY($1)", ids)
		var remaining int
		if err := setupPool.QueryRow(cleanupCtx, `SELECT
		 (SELECT count(*) FROM auth.users WHERE id = ANY($1)) +
		 (SELECT count(*) FROM public.profiles WHERE id = ANY($1)) +
		 (SELECT count(*) FROM public.platform_admins WHERE user_id = ANY($1)) +
		 (SELECT count(*) FROM public.user_gateways WHERE user_id = ANY($1) OR gateway_id = $2) +
		 (SELECT count(*) FROM public.gateways WHERE gateway_id = $2)`, ids, testGatewayID).Scan(&remaining); err != nil || remaining != 0 {
			t.Error("repository fixture cleanup verification failed")
		}
	}()

	// Seed Auth users
	execSetup(`INSERT INTO auth.users (id, email, raw_user_meta_data, created_at, aud, role) 
		VALUES ($1, 'actor_admin@test.local', '{"full_name":"Actor Admin"}', $5, 'authenticated', 'authenticated'),
		       ($2, 'target_user@test.local', '{"full_name":"Target User"}', $5, 'authenticated', 'authenticated'),
		       ($3, 'non_admin@test.local', '{"full_name":"Non Admin"}', $5, 'authenticated', 'authenticated'),
		       ($4, 'orphan_user@test.local', '{"full_name":"Orphan User"}', $5, 'authenticated', 'authenticated')`,
		actorAdminID, targetUserID, nonAdminActorID, orphanAuthUserID, now,
	)

	// on_auth_user_created trigger inserts profiles for 1, 2, 3, 4 automatically.
	// For orphanAuthUserID, delete its profile to simulate profile inconsistency
	execSetup(`DELETE FROM public.profiles WHERE id = $1`, orphanAuthUserID)

	// Make actorAdminID a platform admin
	execSetup(`INSERT INTO public.platform_admins (user_id) VALUES ($1)`, actorAdminID)

	t.Run("list_admin_users_projection security and projection bounds", func(t *testing.T) {
		// Non-admin actor must be rejected with ErrActorNotAdmin
		_, err := repo.ListUsersProjection(ctx, nonAdminActorID, 100)
		if !errors.Is(err, usermanagement.ErrActorNotAdmin) {
			t.Fatalf("expected ErrActorNotAdmin for non-admin actor, got %v", err)
		}

		// When orphan profile exists, ListUsersProjection detects inconsistency
		_, err = repo.ListUsersProjection(ctx, actorAdminID, 100)
		if !errors.Is(err, usermanagement.ErrInconsistentProfile) {
			t.Fatalf("expected ErrInconsistentProfile, got %v", err)
		}

		// Restore profile for orphan user so projection can succeed
		execSetup(`INSERT INTO public.profiles (id, full_name) VALUES ($1, 'Orphan Restored')`, orphanAuthUserID)

		// Test bounded projection before aggregate materialization (limit = 2)
		rowsBounded, err := repo.ListUsersProjection(ctx, actorAdminID, 2)
		if err != nil {
			t.Fatalf("ListUsersProjection bounded failed: %v", err)
		}
		if len(rowsBounded) != 2 {
			t.Fatalf("expected exactly 2 rows with limit=2, got %d", len(rowsBounded))
		}

		// Full projection
		rows, err := repo.ListUsersProjection(ctx, actorAdminID, 100)
		if err != nil {
			t.Fatalf("ListUsersProjection failed: %v", err)
		}
		if len(rows) < 4 {
			t.Fatalf("expected at least 4 user projection rows, got %d", len(rows))
		}

		// Verify allowlisted columns; no secret fields
		var foundTarget bool
		for _, r := range rows {
			if r.UserID == targetUserID {
				foundTarget = true
				if r.Email != "target_user@test.local" {
					t.Fatalf("Email = %q, want target_user@test.local", r.Email)
				}
				if r.FullName == nil || *r.FullName != "Target User" {
					t.Fatalf("FullName = %v, want 'Target User'", r.FullName)
				}
				if r.PlatformRole != "user" {
					t.Fatalf("PlatformRole = %q, want user", r.PlatformRole)
				}
				if r.CreatedAt.IsZero() {
					t.Fatalf("CreatedAt should not be zero")
				}
				if !r.ProfileExists {
					t.Fatalf("ProfileExists should be true")
				}
			}
		}
		if !foundTarget {
			t.Fatalf("target user not found in projection rows")
		}
	})

	t.Run("ListUserGatewayRoles bounding and deterministic ordering", func(t *testing.T) {
		// Empty user IDs returns empty slice without error
		roles, err := repo.ListUserGatewayRoles(ctx, []uuid.UUID{}, 100)
		if err != nil {
			t.Fatalf("ListUserGatewayRoles empty failed: %v", err)
		}
		if len(roles) != 0 {
			t.Fatalf("expected 0 roles for empty user IDs, got %d", len(roles))
		}

		// Seed a gateway and 2 memberships
		execSetup(`INSERT INTO public.gateways (gateway_id, name) VALUES ($1, 'Test GW U2')`, testGatewayID)
		execSetup(`INSERT INTO public.user_gateways (user_id, gateway_id, role) VALUES ($1, $3, 'owner'), ($2, $3, 'operator')`,
			actorAdminID, targetUserID, testGatewayID)

		// Test bounding (limit = 1)
		rolesBounded, err := repo.ListUserGatewayRoles(ctx, []uuid.UUID{actorAdminID, targetUserID}, 1)
		if err != nil {
			t.Fatalf("ListUserGatewayRoles bounded failed: %v", err)
		}
		if len(rolesBounded) != 1 {
			t.Fatalf("expected 1 role with limit=1, got %d", len(rolesBounded))
		}

		// Full membership query
		rolesAll, err := repo.ListUserGatewayRoles(ctx, []uuid.UUID{actorAdminID, targetUserID}, 100)
		if err != nil {
			t.Fatalf("ListUserGatewayRoles full failed: %v", err)
		}
		if len(rolesAll) != 2 {
			t.Fatalf("expected 2 roles, got %d", len(rolesAll))
		}
	})

	t.Run("UpdateUserProfileAndRole canonical name and role grant/revoke/no-op", func(t *testing.T) {
		canonicalName := "Nguyễn Văn A"
		roleAdmin := "platform_admin"
		roleUser := "user"

		// 1. Grant admin and update to canonical Vietnamese name
		updated, err := repo.UpdateUserProfileAndRole(ctx, actorAdminID, targetUserID, &canonicalName, &roleAdmin)
		if err != nil {
			t.Fatalf("UpdateUserProfileAndRole failed: %v", err)
		}
		if updated.FullName == nil || *updated.FullName != canonicalName || updated.PlatformRole != "platform_admin" {
			t.Fatalf("unexpected updated user: %+v", updated)
		}

		// Verify in DB that granted_by is actorAdminID and granted_at is recorded
		var grantedBy uuid.UUID
		var grantedAt time.Time
		err = setupPool.QueryRow(ctx, "SELECT granted_by, granted_at FROM public.platform_admins WHERE user_id = $1", targetUserID).Scan(&grantedBy, &grantedAt)
		if err != nil || grantedBy != actorAdminID {
			t.Fatalf("granted_by = %v (err=%v), want %v", grantedBy, err, actorAdminID)
		}

		// 2. Grant admin again (no-op, preserves granted_by and granted_at)
		updatedNoOp, err := repo.UpdateUserProfileAndRole(ctx, actorAdminID, targetUserID, nil, &roleAdmin)
		if err != nil {
			t.Fatalf("no-op grant failed: %v", err)
		}
		if updatedNoOp.PlatformRole != "platform_admin" {
			t.Fatalf("unexpected platform role on no-op: %s", updatedNoOp.PlatformRole)
		}

		var noOpGrantedBy uuid.UUID
		var noOpGrantedAt time.Time
		err = setupPool.QueryRow(ctx, "SELECT granted_by, granted_at FROM public.platform_admins WHERE user_id = $1", targetUserID).Scan(&noOpGrantedBy, &noOpGrantedAt)
		if err != nil {
			t.Fatalf("query after no-op failed: %v", err)
		}
		if noOpGrantedBy != grantedBy || !noOpGrantedAt.Equal(grantedAt) {
			t.Fatalf("no-op grant must preserve granted_by (%v -> %v) and granted_at (%v -> %v)", grantedBy, noOpGrantedBy, grantedAt, noOpGrantedAt)
		}

		// 3. Revoke admin
		updatedRevoke, err := repo.UpdateUserProfileAndRole(ctx, actorAdminID, targetUserID, nil, &roleUser)
		if err != nil {
			t.Fatalf("revoke failed: %v", err)
		}
		if updatedRevoke.PlatformRole != "user" {
			t.Fatalf("unexpected platform role after revoke: %s", updatedRevoke.PlatformRole)
		}

		var adminCount int
		err = setupPool.QueryRow(ctx, "SELECT count(*) FROM public.platform_admins WHERE user_id = $1", targetUserID).Scan(&adminCount)
		if err != nil || adminCount != 0 {
			t.Fatalf("platform_admins count = %d (err=%v), want 0 after revoke", adminCount, err)
		}

		// Verify target user's gateway memberships are preserved intact
		var targetMembershipCount int
		err = setupPool.QueryRow(ctx, "SELECT count(*) FROM public.user_gateways WHERE user_id = $1", targetUserID).Scan(&targetMembershipCount)
		if err != nil || targetMembershipCount != 1 {
			t.Fatalf("user_gateways count = %d (err=%v), want 1 preserved after role revoke", targetMembershipCount, err)
		}
	})

	t.Run("UpdateUserProfileAndRole atomic combined PATCH rollback", func(t *testing.T) {
		roleUser := "user"
		roleAdmin := "platform_admin"

		// 1. Actor attempts combined rename AND forbidden self-demotion
		// Database state before attempt:
		var origName *string
		err := setupPool.QueryRow(ctx, "SELECT full_name FROM public.profiles WHERE id = $1", actorAdminID).Scan(&origName)
		if err != nil {
			t.Fatalf("read original name failed: %v", err)
		}

		attemptedName := "Actor Name Hacked"
		_, err = repo.UpdateUserProfileAndRole(ctx, actorAdminID, actorAdminID, &attemptedName, &roleUser)
		if !errors.Is(err, usermanagement.ErrSelfDemotionForbidden) {
			t.Fatalf("expected ErrSelfDemotionForbidden, got %v", err)
		}

		// Verify transaction rolled back: actor's full_name was NOT changed
		var nameAfterRollback *string
		err = setupPool.QueryRow(ctx, "SELECT full_name FROM public.profiles WHERE id = $1", actorAdminID).Scan(&nameAfterRollback)
		if err != nil {
			t.Fatalf("read name after rollback failed: %v", err)
		}
		if (origName == nil && nameAfterRollback != nil) || (origName != nil && (nameAfterRollback == nil || *nameAfterRollback != *origName)) {
			t.Fatalf("name was modified despite forbidden self-demotion! got %v, want %v", nameAfterRollback, origName)
		}

		// Verify actor is still platform_admin
		var actorAdminCount int
		err = setupPool.QueryRow(ctx, "SELECT count(*) FROM public.platform_admins WHERE user_id = $1", actorAdminID).Scan(&actorAdminCount)
		if err != nil || actorAdminCount != 1 {
			t.Fatalf("actor platform_admin removed despite transaction failure: %v", err)
		}

		// 2. Missing target user fails with ErrUserNotFound and writes nothing
		missingID := uuid.New()
		_, err = repo.UpdateUserProfileAndRole(ctx, actorAdminID, missingID, &attemptedName, &roleAdmin)
		if !errors.Is(err, usermanagement.ErrUserNotFound) {
			t.Fatalf("expected ErrUserNotFound for missing target, got %v", err)
		}

		var missingAdminCount int
		err = setupPool.QueryRow(ctx, "SELECT count(*) FROM public.platform_admins WHERE user_id = $1", missingID).Scan(&missingAdminCount)
		if err != nil || missingAdminCount != 0 {
			t.Fatalf("missing target must not have platform_admins row: %v", err)
		}
	})

	t.Run("UpdateUserProfileAndRole actor recheck inside transaction", func(t *testing.T) {
		newName := "Target Name"
		roleAdmin := "platform_admin"

		// 1. Non-admin actor is rejected inside transaction
		_, err := repo.UpdateUserProfileAndRole(ctx, nonAdminActorID, targetUserID, &newName, &roleAdmin)
		if !errors.Is(err, usermanagement.ErrActorNotAdmin) {
			t.Fatalf("expected ErrActorNotAdmin for non-admin actor, got %v", err)
		}

		// 2. Admin actor whose role was revoked just before transaction
		execSetup("DELETE FROM public.platform_admins WHERE user_id = $1", actorAdminID)
		_, err = repo.UpdateUserProfileAndRole(ctx, actorAdminID, targetUserID, &newName, &roleAdmin)
		if !errors.Is(err, usermanagement.ErrActorNotAdmin) {
			t.Fatalf("expected ErrActorNotAdmin for revoked admin actor, got %v", err)
		}
		// Restore actor admin
		execSetup("INSERT INTO public.platform_admins (user_id) VALUES ($1)", actorAdminID)
	})

	t.Run("VerifyActorIsAdmin permissions and validation", func(t *testing.T) {
		// 1. Admin actor succeeds
		if err := repo.VerifyActorIsAdmin(ctx, actorAdminID); err != nil {
			t.Fatalf("expected nil for admin actor, got %v", err)
		}

		// 2. Non-admin actor rejected
		if err := repo.VerifyActorIsAdmin(ctx, nonAdminActorID); !errors.Is(err, usermanagement.ErrActorNotAdmin) {
			t.Fatalf("expected ErrActorNotAdmin for non-admin, got %v", err)
		}

		// 3. Random unknown actor rejected
		if err := repo.VerifyActorIsAdmin(ctx, uuid.New()); !errors.Is(err, usermanagement.ErrActorNotAdmin) {
			t.Fatalf("expected ErrActorNotAdmin for unknown actor, got %v", err)
		}

		// 4. Nil UUID rejected
		if err := repo.VerifyActorIsAdmin(ctx, uuid.Nil); !errors.Is(err, usermanagement.ErrActorNotAdmin) {
			t.Fatalf("expected ErrActorNotAdmin for nil UUID, got %v", err)
		}
	})

	t.Run("VerifyCreatedUserProfile validations and invariants", func(t *testing.T) {
		// Seed a clean new user profile with exact name and no admin/membership
		newUserID := uuid.New()
		defer func() {
			_, _ = setupPool.Exec(ctx, "DELETE FROM public.profiles WHERE id = $1", newUserID)
			_, _ = setupPool.Exec(ctx, "DELETE FROM auth.users WHERE id = $1", newUserID)
		}()
		execSetup(`INSERT INTO auth.users (id, email, raw_user_meta_data, created_at, aud, role) 
			VALUES ($1, 'new_verify_user@test.local', '{"full_name":"New Verified User"}', $2, 'authenticated', 'authenticated')`,
			newUserID, now,
		)

		// 1. Exact match, no admin, no memberships -> success
		if err := repo.VerifyCreatedUserProfile(ctx, newUserID, "New Verified User"); err != nil {
			t.Fatalf("expected nil for valid profile, got %v", err)
		}

		// 2. Name mismatch -> ErrInconsistentProfile
		if err := repo.VerifyCreatedUserProfile(ctx, newUserID, "Wrong Name"); !errors.Is(err, usermanagement.ErrInconsistentProfile) {
			t.Fatalf("expected ErrInconsistentProfile on name mismatch, got %v", err)
		}

		// 3. Missing profile -> ErrInconsistentProfile
		missingUserID := uuid.New()
		if err := repo.VerifyCreatedUserProfile(ctx, missingUserID, "Anyone"); !errors.Is(err, usermanagement.ErrInconsistentProfile) {
			t.Fatalf("expected ErrInconsistentProfile on missing profile, got %v", err)
		}

		// 4. Target unexpectedly has platform_admin -> ErrInconsistentProfile
		execSetup("INSERT INTO public.platform_admins (user_id) VALUES ($1)", newUserID)
		if err := repo.VerifyCreatedUserProfile(ctx, newUserID, "New Verified User"); !errors.Is(err, usermanagement.ErrInconsistentProfile) {
			t.Fatalf("expected ErrInconsistentProfile when user has platform_admin, got %v", err)
		}
		execSetup("DELETE FROM public.platform_admins WHERE user_id = $1", newUserID)

		// 5. Target unexpectedly has gateway membership -> ErrInconsistentProfile
		execSetup("INSERT INTO public.user_gateways (user_id, gateway_id, role) VALUES ($1, $2, 'viewer')", newUserID, testGatewayID)
		if err := repo.VerifyCreatedUserProfile(ctx, newUserID, "New Verified User"); !errors.Is(err, usermanagement.ErrInconsistentProfile) {
			t.Fatalf("expected ErrInconsistentProfile when user has gateway membership, got %v", err)
		}
		execSetup("DELETE FROM public.user_gateways WHERE user_id = $1 AND gateway_id = $2", newUserID, testGatewayID)

		// 6. Nil UUID -> ErrInvalidInput
		if err := repo.VerifyCreatedUserProfile(ctx, uuid.Nil, "Name"); !errors.Is(err, usermanagement.ErrInvalidInput) {
			t.Fatalf("expected ErrInvalidInput on nil targetID, got %v", err)
		}
	})

	t.Run("runtime versus human grants least-privilege verification", func(t *testing.T) {
		// 1. iot_backend_app can UPDATE profiles.full_name
		_, err := backendPool.Exec(ctx, "UPDATE public.profiles SET full_name = 'Valid Update' WHERE id = $1", targetUserID)
		if err != nil {
			t.Fatalf("iot_backend_app must be able to update full_name: %v", err)
		}

		// 2. iot_backend_app CANNOT update profiles.id (read-only id)
		_, err = backendPool.Exec(ctx, "UPDATE public.profiles SET id = $1 WHERE id = $2", uuid.New(), targetUserID)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
			t.Fatalf("expected insufficient_privilege 42501 updating profiles.id, got %v", err)
		}

		// 3. iot_backend_app CANNOT write to auth.users directly
		_, err = backendPool.Exec(ctx, "UPDATE auth.users SET email = 'hacked@test.local' WHERE id = $1", targetUserID)
		if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
			t.Fatalf("expected insufficient_privilege 42501 updating auth.users, got %v", err)
		}

		// 4. Human roles (authenticated, anon) cannot call list_admin_users_projection
		for _, humanRole := range []string{"anon", "authenticated"} {
			_, err = setupPool.Exec(ctx, fmt.Sprintf("SET ROLE %s; SELECT * FROM public.list_admin_users_projection('%s', 10); RESET ROLE;", humanRole, actorAdminID))
			if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
				t.Fatalf("expected insufficient_privilege 42501 for role %s calling list_admin_users_projection, got %v", humanRole, err)
			}
		}

		// 5. Human roles (authenticated, anon) cannot mutate platform_admins directly
		for _, humanRole := range []string{"anon", "authenticated"} {
			_, err = setupPool.Exec(ctx, fmt.Sprintf("SET ROLE %s; INSERT INTO public.platform_admins (user_id) VALUES ('%s'); RESET ROLE;", humanRole, targetUserID))
			if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
				t.Fatalf("expected insufficient_privilege 42501 for role %s inserting into platform_admins, got %v", humanRole, err)
			}

			_, err = setupPool.Exec(ctx, fmt.Sprintf("SET ROLE %s; DELETE FROM public.platform_admins WHERE user_id = '%s'; RESET ROLE;", humanRole, targetUserID))
			if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
				t.Fatalf("expected insufficient_privilege 42501 for role %s deleting from platform_admins, got %v", humanRole, err)
			}
		}
	})

	t.Run("AssignGatewayUser allowed results, invariants and rollback", func(t *testing.T) {
		u5TestGatewayID := "gw_u5_repo_" + strings.ReplaceAll(uuid.New().String()[:8], "-", "")
		u5OwnerID := uuid.New()
		u5TargetID := uuid.New()
		now := time.Now().UTC()

		defer func() {
			_, _ = setupPool.Exec(ctx, "DELETE FROM public.user_gateways WHERE gateway_id = $1", u5TestGatewayID)
			_, _ = setupPool.Exec(ctx, "DELETE FROM public.gateways WHERE gateway_id = $1", u5TestGatewayID)
			_, _ = setupPool.Exec(ctx, "DELETE FROM public.profiles WHERE id IN ($1, $2)", u5OwnerID, u5TargetID)
			_, _ = setupPool.Exec(ctx, "DELETE FROM auth.users WHERE id IN ($1, $2)", u5OwnerID, u5TargetID)
		}()

		// Seed auth users + gateway + owner membership
		execSetup(`INSERT INTO auth.users (id, email, raw_user_meta_data, created_at, aud, role) 
			VALUES ($1, 'owner_u5_repo@test.local', '{"full_name":"Owner U5 Repo"}', $3, 'authenticated', 'authenticated'),
			       ($2, 'target_u5_repo@test.local', '{"full_name":"Target U5 Repo"}', $3, 'authenticated', 'authenticated')`,
			u5OwnerID, u5TargetID, now)
		execSetup(`INSERT INTO public.gateways (gateway_id, name, created_at) VALUES ($1, 'U5 Repo GW', $2)`, u5TestGatewayID, now)
		execSetup(`INSERT INTO public.user_gateways (user_id, gateway_id, role, created_at) VALUES ($1, $2, 'owner', $3)`, u5OwnerID, u5TestGatewayID, now)

		// 1. Non-admin actor rejected
		err := repo.AssignGatewayUser(ctx, nonAdminActorID, u5TestGatewayID, u5TargetID, "operator")
		if !errors.Is(err, usermanagement.ErrActorNotAdmin) {
			t.Fatalf("expected ErrActorNotAdmin for non-admin actor, got %v", err)
		}

		// 2. Gateway not found rejected
		err = repo.AssignGatewayUser(ctx, actorAdminID, "gw_does_not_exist_xyz", u5TargetID, "operator")
		if !errors.Is(err, usermanagement.ErrGatewayNotFound) {
			t.Fatalf("expected ErrGatewayNotFound for missing gateway, got %v", err)
		}

		// 3. User not found rejected
		err = repo.AssignGatewayUser(ctx, actorAdminID, u5TestGatewayID, uuid.New(), "operator")
		if !errors.Is(err, usermanagement.ErrUserNotFound) {
			t.Fatalf("expected ErrUserNotFound for missing target user, got %v", err)
		}

		// 4. Target is currently owner -> ErrOwnerTransferRequired
		err = repo.AssignGatewayUser(ctx, actorAdminID, u5TestGatewayID, u5OwnerID, "operator")
		if !errors.Is(err, usermanagement.ErrOwnerTransferRequired) {
			t.Fatalf("expected ErrOwnerTransferRequired for owner target, got %v", err)
		}

		// 5. Insert operator
		err = repo.AssignGatewayUser(ctx, actorAdminID, u5TestGatewayID, u5TargetID, "operator")
		if err != nil {
			t.Fatalf("AssignGatewayUser insert operator failed: %v", err)
		}

		var role1 string
		var createdAt1 time.Time
		err = setupPool.QueryRow(ctx, "SELECT role, created_at FROM public.user_gateways WHERE gateway_id = $1 AND user_id = $2", u5TestGatewayID, u5TargetID).Scan(&role1, &createdAt1)
		if err != nil || role1 != "operator" {
			t.Fatalf("role in DB = %q (err=%v), want operator", role1, err)
		}

		// 6. Exact no-op operator preserves created_at
		err = repo.AssignGatewayUser(ctx, actorAdminID, u5TestGatewayID, u5TargetID, "operator")
		if err != nil {
			t.Fatalf("AssignGatewayUser operator no-op failed: %v", err)
		}
		var role2 string
		var createdAt2 time.Time
		err = setupPool.QueryRow(ctx, "SELECT role, created_at FROM public.user_gateways WHERE gateway_id = $1 AND user_id = $2", u5TestGatewayID, u5TargetID).Scan(&role2, &createdAt2)
		if err != nil || role2 != "operator" {
			t.Fatalf("role in DB = %q (err=%v), want operator", role2, err)
		}
		if !createdAt2.Equal(createdAt1) {
			t.Fatalf("created_at modified on exact no-op: initial=%v, after=%v", createdAt1, createdAt2)
		}

		// 7. Update to viewer preserves created_at
		err = repo.AssignGatewayUser(ctx, actorAdminID, u5TestGatewayID, u5TargetID, "viewer")
		if err != nil {
			t.Fatalf("AssignGatewayUser update viewer failed: %v", err)
		}
		var role3 string
		var createdAt3 time.Time
		err = setupPool.QueryRow(ctx, "SELECT role, created_at FROM public.user_gateways WHERE gateway_id = $1 AND user_id = $2", u5TestGatewayID, u5TargetID).Scan(&role3, &createdAt3)
		if err != nil || role3 != "viewer" {
			t.Fatalf("role in DB = %q (err=%v), want viewer", role3, err)
		}
		if !createdAt3.Equal(createdAt1) {
			t.Fatalf("created_at modified on update to viewer: initial=%v, after=%v", createdAt1, createdAt3)
		}

		// 8. Exact no-op viewer preserves created_at
		err = repo.AssignGatewayUser(ctx, actorAdminID, u5TestGatewayID, u5TargetID, "viewer")
		if err != nil {
			t.Fatalf("AssignGatewayUser viewer no-op failed: %v", err)
		}
		var role4 string
		var createdAt4 time.Time
		err = setupPool.QueryRow(ctx, "SELECT role, created_at FROM public.user_gateways WHERE gateway_id = $1 AND user_id = $2", u5TestGatewayID, u5TargetID).Scan(&role4, &createdAt4)
		if err != nil || role4 != "viewer" {
			t.Fatalf("role in DB = %q (err=%v), want viewer", role4, err)
		}
		if !createdAt4.Equal(createdAt1) {
			t.Fatalf("created_at modified on viewer no-op: initial=%v, after=%v", createdAt1, createdAt4)
		}
	})

	t.Run("TransferGatewayOwner allowed results, invariants and rollback", func(t *testing.T) {
		u6TestGatewayID := "gw_u6_repo_" + strings.ReplaceAll(uuid.New().String()[:8], "-", "")
		u6OwnerID := uuid.New()
		u6TargetNoMemberID := uuid.New()
		u6TargetOperatorID := uuid.New()
		u6TargetViewerID := uuid.New()
		u6SecondOwnerID := uuid.New()
		now := time.Now().UTC()

		defer func() {
			_, _ = setupPool.Exec(ctx, "DELETE FROM public.user_gateways WHERE gateway_id = $1", u6TestGatewayID)
			_, _ = setupPool.Exec(ctx, "DELETE FROM public.gateways WHERE gateway_id = $1", u6TestGatewayID)
			_, _ = setupPool.Exec(ctx, "DELETE FROM public.profiles WHERE id IN ($1, $2, $3, $4, $5)",
				u6OwnerID, u6TargetNoMemberID, u6TargetOperatorID, u6TargetViewerID, u6SecondOwnerID)
			_, _ = setupPool.Exec(ctx, "DELETE FROM auth.users WHERE id IN ($1, $2, $3, $4, $5)",
				u6OwnerID, u6TargetNoMemberID, u6TargetOperatorID, u6TargetViewerID, u6SecondOwnerID)
		}()

		// Seed auth users + gateway + initial memberships
		execSetup(`INSERT INTO auth.users (id, email, raw_user_meta_data, created_at, aud, role) 
			VALUES ($1, 'owner_u6_repo@test.local', '{"full_name":"Owner U6 Repo"}', $6, 'authenticated', 'authenticated'),
			       ($2, 'target_nomember_u6@test.local', '{"full_name":"Target NoMember"}', $6, 'authenticated', 'authenticated'),
			       ($3, 'target_operator_u6@test.local', '{"full_name":"Target Operator"}', $6, 'authenticated', 'authenticated'),
			       ($4, 'target_viewer_u6@test.local', '{"full_name":"Target Viewer"}', $6, 'authenticated', 'authenticated'),
			       ($5, 'second_owner_u6@test.local', '{"full_name":"Second Owner"}', $6, 'authenticated', 'authenticated')`,
			u6OwnerID, u6TargetNoMemberID, u6TargetOperatorID, u6TargetViewerID, u6SecondOwnerID, now)
		execSetup(`INSERT INTO public.gateways (gateway_id, name, created_at) VALUES ($1, 'U6 Repo GW', $2)`, u6TestGatewayID, now)
		execSetup(`INSERT INTO public.user_gateways (user_id, gateway_id, role, created_at) VALUES ($1, $2, 'owner', $3)`, u6OwnerID, u6TestGatewayID, now)
		execSetup(`INSERT INTO public.user_gateways (user_id, gateway_id, role, created_at) VALUES ($1, $2, 'operator', $3)`, u6TargetOperatorID, u6TestGatewayID, now)
		execSetup(`INSERT INTO public.user_gateways (user_id, gateway_id, role, created_at) VALUES ($1, $2, 'viewer', $3)`, u6TargetViewerID, u6TestGatewayID, now)

		var initialOwnerCreatedAt time.Time
		err := setupPool.QueryRow(ctx, "SELECT created_at FROM public.user_gateways WHERE gateway_id = $1 AND user_id = $2", u6TestGatewayID, u6OwnerID).Scan(&initialOwnerCreatedAt)
		if err != nil {
			t.Fatalf("query initial owner created_at failed: %v", err)
		}

		var initialOperatorCreatedAt time.Time
		err = setupPool.QueryRow(ctx, "SELECT created_at FROM public.user_gateways WHERE gateway_id = $1 AND user_id = $2", u6TestGatewayID, u6TargetOperatorID).Scan(&initialOperatorCreatedAt)
		if err != nil {
			t.Fatalf("query initial operator created_at failed: %v", err)
		}

		var initialViewerCreatedAt time.Time
		err = setupPool.QueryRow(ctx, "SELECT created_at FROM public.user_gateways WHERE gateway_id = $1 AND user_id = $2", u6TestGatewayID, u6TargetViewerID).Scan(&initialViewerCreatedAt)
		if err != nil {
			t.Fatalf("query initial viewer created_at failed: %v", err)
		}

		// 1. Non-admin actor rejected
		err = repo.TransferGatewayOwner(ctx, nonAdminActorID, u6TestGatewayID, u6TargetNoMemberID)
		if !errors.Is(err, usermanagement.ErrActorNotAdmin) {
			t.Fatalf("expected ErrActorNotAdmin for non-admin actor, got %v", err)
		}

		// 2. Missing gateway rejected
		err = repo.TransferGatewayOwner(ctx, actorAdminID, "gw_does_not_exist_xyz", u6TargetNoMemberID)
		if !errors.Is(err, usermanagement.ErrGatewayNotFound) {
			t.Fatalf("expected ErrGatewayNotFound for missing gateway, got %v", err)
		}

		// 3. Missing target user rejected
		err = repo.TransferGatewayOwner(ctx, actorAdminID, u6TestGatewayID, uuid.New())
		if !errors.Is(err, usermanagement.ErrUserNotFound) {
			t.Fatalf("expected ErrUserNotFound for missing target user, got %v", err)
		}

		// 4. Target is already owner: exact no-op
		err = repo.TransferGatewayOwner(ctx, actorAdminID, u6TestGatewayID, u6OwnerID)
		if err != nil {
			t.Fatalf("expected nil error on already-owner no-op, got %v", err)
		}
		var checkOwnerRole string
		var checkOwnerCreatedAt time.Time
		err = setupPool.QueryRow(ctx, "SELECT role, created_at FROM public.user_gateways WHERE gateway_id = $1 AND user_id = $2", u6TestGatewayID, u6OwnerID).Scan(&checkOwnerRole, &checkOwnerCreatedAt)
		if err != nil || checkOwnerRole != "owner" {
			t.Fatalf("role in DB = %q (err=%v), want owner", checkOwnerRole, err)
		}
		if !checkOwnerCreatedAt.Equal(initialOwnerCreatedAt) {
			t.Fatalf("owner created_at modified on no-op: initial=%v, after=%v", initialOwnerCreatedAt, checkOwnerCreatedAt)
		}

		// 5. Zero owners: remove owner row temporarily -> ErrGatewayOwnerInconsistent
		execSetup(`DELETE FROM public.user_gateways WHERE gateway_id = $1 AND user_id = $2`, u6TestGatewayID, u6OwnerID)
		err = repo.TransferGatewayOwner(ctx, actorAdminID, u6TestGatewayID, u6TargetNoMemberID)
		if !errors.Is(err, usermanagement.ErrGatewayOwnerInconsistent) {
			t.Fatalf("expected ErrGatewayOwnerInconsistent on zero owners, got %v", err)
		}

		// 6. Multiple owners: insert two owners -> ErrGatewayOwnerInconsistent
		execSetup(`INSERT INTO public.user_gateways (user_id, gateway_id, role, created_at) VALUES ($1, $2, 'owner', $3), ($4, $2, 'owner', $3)`,
			u6OwnerID, u6TestGatewayID, initialOwnerCreatedAt, u6SecondOwnerID)
		err = repo.TransferGatewayOwner(ctx, actorAdminID, u6TestGatewayID, u6TargetNoMemberID)
		if !errors.Is(err, usermanagement.ErrGatewayOwnerInconsistent) {
			t.Fatalf("expected ErrGatewayOwnerInconsistent on multiple owners, got %v", err)
		}

		// Restore clean state: exactly one owner (u6OwnerID)
		execSetup(`DELETE FROM public.user_gateways WHERE gateway_id = $1 AND user_id = $2`, u6TestGatewayID, u6SecondOwnerID)

		// 7. Transfer to target with no prior membership:
		// Old owner becomes viewer; target becomes owner; old owner created_at preserved; target gets new created_at.
		err = repo.TransferGatewayOwner(ctx, actorAdminID, u6TestGatewayID, u6TargetNoMemberID)
		if err != nil {
			t.Fatalf("TransferGatewayOwner to target with no prior membership failed: %v", err)
		}

		var oldOwnerRole string
		var oldOwnerAfterCreatedAt time.Time
		err = setupPool.QueryRow(ctx, "SELECT role, created_at FROM public.user_gateways WHERE gateway_id = $1 AND user_id = $2", u6TestGatewayID, u6OwnerID).Scan(&oldOwnerRole, &oldOwnerAfterCreatedAt)
		if err != nil || oldOwnerRole != "viewer" {
			t.Fatalf("old owner role in DB = %q (err=%v), want viewer", oldOwnerRole, err)
		}
		if !oldOwnerAfterCreatedAt.Equal(initialOwnerCreatedAt) {
			t.Fatalf("old owner created_at modified after demotion: initial=%v, after=%v", initialOwnerCreatedAt, oldOwnerAfterCreatedAt)
		}

		var newOwnerRole string
		var newOwnerCreatedAt time.Time
		err = setupPool.QueryRow(ctx, "SELECT role, created_at FROM public.user_gateways WHERE gateway_id = $1 AND user_id = $2", u6TestGatewayID, u6TargetNoMemberID).Scan(&newOwnerRole, &newOwnerCreatedAt)
		if err != nil || newOwnerRole != "owner" {
			t.Fatalf("new owner role in DB = %q (err=%v), want owner", newOwnerRole, err)
		}
		if newOwnerCreatedAt.IsZero() {
			t.Fatal("expected non-zero created_at for new owner")
		}

		var totalOwners int
		err = setupPool.QueryRow(ctx, "SELECT count(*) FROM public.user_gateways WHERE gateway_id = $1 AND role = 'owner'", u6TestGatewayID).Scan(&totalOwners)
		if err != nil || totalOwners != 1 {
			t.Fatalf("total owners = %d (err=%v), want exactly 1", totalOwners, err)
		}

		// 8. Transfer to target with existing operator role:
		// Target was operator; target becomes owner; old owner (u6TargetNoMemberID) becomes viewer; target's created_at preserved!
		err = repo.TransferGatewayOwner(ctx, actorAdminID, u6TestGatewayID, u6TargetOperatorID)
		if err != nil {
			t.Fatalf("TransferGatewayOwner to operator target failed: %v", err)
		}

		var opRoleAfter string
		var opCreatedAtAfter time.Time
		err = setupPool.QueryRow(ctx, "SELECT role, created_at FROM public.user_gateways WHERE gateway_id = $1 AND user_id = $2", u6TestGatewayID, u6TargetOperatorID).Scan(&opRoleAfter, &opCreatedAtAfter)
		if err != nil || opRoleAfter != "owner" {
			t.Fatalf("operator target role = %q, want owner", opRoleAfter)
		}
		if !opCreatedAtAfter.Equal(initialOperatorCreatedAt) {
			t.Fatalf("operator created_at modified: initial=%v, after=%v", initialOperatorCreatedAt, opCreatedAtAfter)
		}

		var prevOwnerRole string
		err = setupPool.QueryRow(ctx, "SELECT role FROM public.user_gateways WHERE gateway_id = $1 AND user_id = $2", u6TestGatewayID, u6TargetNoMemberID).Scan(&prevOwnerRole)
		if err != nil || prevOwnerRole != "viewer" {
			t.Fatalf("previous owner role = %q, want viewer", prevOwnerRole)
		}

		// 9. Transfer to target with existing viewer role:
		// Target was viewer; target becomes owner; target's created_at preserved!
		err = repo.TransferGatewayOwner(ctx, actorAdminID, u6TestGatewayID, u6TargetViewerID)
		if err != nil {
			t.Fatalf("TransferGatewayOwner to viewer target failed: %v", err)
		}

		var viewerRoleAfter string
		var viewerCreatedAtAfter time.Time
		err = setupPool.QueryRow(ctx, "SELECT role, created_at FROM public.user_gateways WHERE gateway_id = $1 AND user_id = $2", u6TestGatewayID, u6TargetViewerID).Scan(&viewerRoleAfter, &viewerCreatedAtAfter)
		if err != nil || viewerRoleAfter != "owner" {
			t.Fatalf("viewer target role = %q, want owner", viewerRoleAfter)
		}
		if !viewerCreatedAtAfter.Equal(initialViewerCreatedAt) {
			t.Fatalf("viewer created_at modified: initial=%v, after=%v", initialViewerCreatedAt, viewerCreatedAtAfter)
		}

		err = setupPool.QueryRow(ctx, "SELECT count(*) FROM public.user_gateways WHERE gateway_id = $1 AND role = 'owner'", u6TestGatewayID).Scan(&totalOwners)
		if err != nil || totalOwners != 1 {
			t.Fatalf("final total owners = %d, want exactly 1", totalOwners)
		}
	})
}
