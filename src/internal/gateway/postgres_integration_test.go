package gateway

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresAuthorizationIntegration(t *testing.T) {
	if os.Getenv("AUTHORIZATION_TEST_ISOLATED") != "1" {
		t.Skip("run sh scripts/test-stage2-authorization.sh")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	setup, err := pgxpool.New(ctx, os.Getenv("AUTHORIZATION_TEST_SETUP_URL"))
	if err != nil {
		t.Fatal("setup pool initialization failed")
	}
	defer setup.Close()
	app, err := pgxpool.New(ctx, os.Getenv("AUTHORIZATION_TEST_BACKEND_URL"))
	if err != nil {
		t.Fatal("backend pool initialization failed")
	}
	defer app.Close()
	var currentUser string
	if err := app.QueryRow(ctx, "SELECT current_user").Scan(&currentUser); err != nil || currentUser != "iot_backend_app" {
		t.Fatal("must use iot_backend_app")
	}
	repo, err := NewPostgresRepository(app)
	if err != nil {
		t.Fatal(err)
	}
	a, b, newcomer, admin := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	prefix := "fixture_" + uuid.New().String()
	ga, gb, empty, missing := prefix+"_A", prefix+"_B", prefix+"_empty", prefix+"_missing"
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := setup.Exec(ctx, sql, args...); err != nil {
			t.Fatal("fixture SQL failed")
		}
	}
	exec("INSERT INTO profiles(id) VALUES ($1),($2),($3),($4)", a, b, newcomer, admin)
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := setup.Exec(cleanup, "DELETE FROM gateways WHERE gateway_id IN ($1,$2,$3)", ga, gb, empty); err != nil {
			t.Error("gateway cleanup failed")
		}
		if _, err := setup.Exec(cleanup, "DELETE FROM profiles WHERE id IN ($1,$2,$3,$4)", a, b, newcomer, admin); err != nil {
			t.Error("profile cleanup failed")
		}
	}()
	exec("INSERT INTO platform_admins(user_id) VALUES ($1)", admin)
	exec("INSERT INTO gateways(gateway_id,name,description,created_at) VALUES ($1,'A',NULL,NULL),($2,'B','metadata',now()),($3,'Empty',NULL,now())", ga, gb, empty)
	exec("INSERT INTO user_gateways(user_id,gateway_id,role) VALUES ($1,$2,'owner'),($1,$3,'viewer'),($4,$5,'operator')", a, ga, empty, b, gb)
	exec("INSERT INTO sensors(gateway_id,sensor_id,name,unit,created_at) VALUES ($1,'z','last','unit',now()),($1,'a','first',NULL,NULL),($2,'a','other','different',now())", ga, gb)
	for _, role := range []Role{RoleOwner, RoleOperator, RoleViewer} {
		exec("UPDATE user_gateways SET role=$1 WHERE user_id=$2 AND gateway_id=$3", role, a, ga)
		gotRole, err := repo.GetGatewayRole(ctx, a, ga)
		if err != nil || gotRole != role {
			t.Fatal("role lookup mismatch")
		}
		items, err := repo.ListGatewaysForUser(ctx, a)
		if err != nil || len(items) != 2 || items[0].GatewayID != ga || items[1].GatewayID != empty || items[0].Role != role || items[0].Description != nil || items[0].CreatedAt != nil {
			t.Fatal("gateway isolation/order/nullability mismatch")
		}
		itemsB, err := repo.ListGatewaysForUser(ctx, b)
		if err != nil || len(itemsB) != 1 || itemsB[0].GatewayID != gb || itemsB[0].Description == nil {
			t.Fatal("user B gateway isolation mismatch")
		}
		sensors, err := repo.ListSensorsForUserAndGateway(ctx, a, ga)
		if err != nil || len(sensors) != 2 || sensors[0].SensorID != "a" || sensors[1].SensorID != "z" || sensors[0].Name != "first" || sensors[0].Unit != nil || sensors[0].CreatedAt != nil || sensors[1].Unit == nil {
			t.Fatal("sensor isolation/order/nullability mismatch")
		}
	}
	for _, id := range []uuid.UUID{newcomer, admin} {
		items, err := repo.ListGatewaysForUser(ctx, id)
		if err != nil || items == nil || len(items) != 0 {
			t.Fatal("unmapped user/admin saw gateways")
		}
		if _, err := repo.ListSensorsForUserAndGateway(ctx, id, ga); !errors.Is(err, ErrNotFound) {
			t.Fatal("unmapped user/admin saw sensors")
		}
	}
	for _, pair := range []struct {
		id      uuid.UUID
		gateway string
	}{{a, gb}, {b, ga}, {a, missing}, {a, "' OR true --"}} {
		if _, err := repo.ListSensorsForUserAndGateway(ctx, pair.id, pair.gateway); !errors.Is(err, ErrNotFound) {
			t.Fatal("sensor denial mismatch")
		}
		if _, err := repo.GetGatewayRole(ctx, pair.id, pair.gateway); !errors.Is(err, ErrNotFound) {
			t.Fatal("role denial mismatch")
		}
	}
	sensors, err := repo.ListSensorsForUserAndGateway(ctx, a, empty)
	if err != nil || sensors == nil || len(sensors) != 0 {
		t.Fatal("empty authorized gateway must return nonnil empty list")
	}
	sensorsB, err := repo.ListSensorsForUserAndGateway(ctx, b, gb)
	if err != nil || len(sensorsB) != 1 || sensorsB[0].Name != "other" {
		t.Fatal("same sensor ID crossed gateways")
	}
	exec("DELETE FROM user_gateways WHERE user_id=$1 AND gateway_id=$2", a, ga)
	if _, err := repo.ListSensorsForUserAndGateway(ctx, a, ga); !errors.Is(err, ErrNotFound) {
		t.Fatal("membership revocation not reflected")
	}
	remaining, err := repo.ListGatewaysForUser(ctx, a)
	if err != nil || len(remaining) != 1 || remaining[0].GatewayID != empty {
		t.Fatal("revocation changed wrong gateway")
	}
	cancelled, cancelNow := context.WithCancel(ctx)
	cancelNow()
	if _, err := repo.ListGatewaysForUser(cancelled, a); !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation not propagated")
	}

	t.Run("admin gateway inventory repository", func(t *testing.T) {
		testPostgresAdminGatewayInventory(ctx, t, setup, app)
	})

	tx, err := setup.Begin(ctx)
	if err != nil {
		t.Fatal("lock transaction failed")
	}
	defer func() {
		rollback, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(rollback)
	}()
	if _, err := tx.Exec(ctx, "LOCK TABLE user_gateways IN ACCESS EXCLUSIVE MODE"); err != nil {
		t.Fatal("lock failed")
	}
	for _, read := range []func(context.Context) error{
		func(c context.Context) error { _, err := repo.ListGatewaysForUser(c, a); return err },
		func(c context.Context) error { _, err := repo.ListSensorsForUserAndGateway(c, a, empty); return err },
		func(c context.Context) error { _, err := repo.GetGatewayRole(c, a, empty); return err },
	} {
		deadline, stop := context.WithTimeout(ctx, 100*time.Millisecond)
		err := read(deadline)
		stop()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("read did not respect deadline")
		}
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal("unlock failed")
	}
	app.Close()
	if _, err := repo.ListGatewaysForUser(ctx, a); err == nil {
		t.Fatal("closed pool accepted query")
	}
}

func testPostgresAdminGatewayInventory(ctx context.Context, t *testing.T, setup *pgxpool.Pool, app *pgxpool.Pool) {
	adminRepo, err := NewPostgresAdminGatewayRepository(app)
	if err != nil {
		t.Fatal(err)
	}

	adminID := uuid.New()
	normalUserID := uuid.New()
	prefix := "z_fixinv_" + uuid.New().String()[:8]
	g0 := prefix + "_0"
	g1 := prefix + "_1"
	gA := prefix + "_A"
	gZ := prefix + "_Z"
	ga := prefix + "_a"

	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := setup.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("fixture SQL failed: %v", err)
		}
	}

	exec("INSERT INTO profiles(id) VALUES ($1),($2)", adminID, normalUserID)
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = setup.Exec(cleanup, "DELETE FROM gateways WHERE gateway_id IN ($1,$2,$3,$4,$5)", g0, g1, gA, gZ, ga)
		_, _ = setup.Exec(cleanup, "DELETE FROM platform_admins WHERE user_id=$1", adminID)
		_, _ = setup.Exec(cleanup, "DELETE FROM profiles WHERE id IN ($1,$2)", adminID, normalUserID)
	}()

	exec("INSERT INTO platform_admins(user_id) VALUES ($1)", adminID)
	exec("INSERT INTO gateways(gateway_id, name, description, created_at) VALUES ($1, 'Gate 0', NULL, NULL)", g0)
	exec("INSERT INTO gateways(gateway_id, name, description, created_at) VALUES ($1, 'Gate 1', 'desc-1', now())", g1)
	exec("INSERT INTO gateways(gateway_id, name, description, created_at) VALUES ($1, 'Gate A', 'desc-A', now())", gA)
	exec("INSERT INTO gateways(gateway_id, name, description, created_at) VALUES ($1, 'Gate Z', 'desc-Z', now())", gZ)
	exec("INSERT INTO gateways(gateway_id, name, description, created_at) VALUES ($1, 'Gate a', 'desc-a', now())", ga)

	exec("INSERT INTO user_gateways(user_id, gateway_id, role) VALUES ($1, $2, 'owner')", normalUserID, g0)

	// 1. Non-admin forbidden
	_, err = adminRepo.ListAdminGateways(ctx, normalUserID, AdminGatewayInventoryQuery{Limit: 10})
	if !errors.Is(err, ErrAdminForbidden) {
		t.Fatalf("expected ErrAdminForbidden for normal user, got %v", err)
	}

	// 2. Keyset pagination and C-ordering for admin without membership
	p1Prefix, err := adminRepo.ListAdminGateways(ctx, adminID, AdminGatewayInventoryQuery{Limit: 2, After: prefix})
	if err != nil {
		t.Fatalf("p1Prefix failed: %v", err)
	}
	if len(p1Prefix.Items) != 2 || p1Prefix.Items[0].GatewayID != g0 || p1Prefix.Items[1].GatewayID != g1 {
		t.Fatalf("p1Prefix ordering mismatch: got %+v", p1Prefix.Items)
	}
	if p1Prefix.NextCursor == nil || *p1Prefix.NextCursor != g1 {
		t.Fatalf("p1Prefix next_cursor mismatch: got %v", p1Prefix.NextCursor)
	}
	if p1Prefix.Items[0].Description != nil || p1Prefix.Items[0].CreatedAt != nil {
		t.Fatalf("expected null description and created_at on g0, got desc=%v created=%v", p1Prefix.Items[0].Description, p1Prefix.Items[0].CreatedAt)
	}
	if p1Prefix.Items[1].Description == nil || p1Prefix.Items[1].CreatedAt == nil {
		t.Fatalf("expected non-null on g1")
	}

	p2, err := adminRepo.ListAdminGateways(ctx, adminID, AdminGatewayInventoryQuery{Limit: 2, After: g1})
	if err != nil {
		t.Fatalf("page 2 query failed: %v", err)
	}
	if len(p2.Items) != 2 || p2.Items[0].GatewayID != gA || p2.Items[1].GatewayID != gZ {
		t.Fatalf("page 2 ordering mismatch: got %+v", p2.Items)
	}
	if p2.NextCursor == nil || *p2.NextCursor != gZ {
		t.Fatalf("page 2 next_cursor mismatch: got %v", p2.NextCursor)
	}

	p3, err := adminRepo.ListAdminGateways(ctx, adminID, AdminGatewayInventoryQuery{Limit: 2, After: gZ})
	if err != nil {
		t.Fatalf("page 3 query failed: %v", err)
	}
	if len(p3.Items) != 1 || p3.Items[0].GatewayID != ga {
		t.Fatalf("page 3 ordering mismatch: got %+v", p3.Items)
	}
	if p3.NextCursor != nil {
		t.Fatalf("page 3 expected nil next_cursor, got %v", *p3.NextCursor)
	}

	p4, err := adminRepo.ListAdminGateways(ctx, adminID, AdminGatewayInventoryQuery{Limit: 2, After: ga})
	if err != nil {
		t.Fatalf("page 4 query failed: %v", err)
	}
	if len(p4.Items) != 0 || p4.NextCursor != nil {
		t.Fatalf("page 4 expected empty items, got %+v", p4)
	}

	// 3. Admin revocation
	exec("DELETE FROM platform_admins WHERE user_id=$1", adminID)
	_, err = adminRepo.ListAdminGateways(ctx, adminID, AdminGatewayInventoryQuery{Limit: 10})
	if !errors.Is(err, ErrAdminForbidden) {
		t.Fatalf("expected ErrAdminForbidden after admin revocation, got %v", err)
	}

	// 4. Outage / timeout
	exec("INSERT INTO platform_admins(user_id) VALUES ($1)", adminID)
	cancelled, cancelNow := context.WithCancel(ctx)
	cancelNow()
	_, err = adminRepo.ListAdminGateways(cancelled, adminID, AdminGatewayInventoryQuery{Limit: 10})
	if err == nil {
		t.Fatal("expected error on cancelled context")
	}
}

func TestPostgresAdminGatewayInventoryIntegration(t *testing.T) {
	if os.Getenv("AUTHORIZATION_TEST_ISOLATED") != "1" {
		t.Skip("run sh scripts/test-stage2-authorization.sh")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	setup, err := pgxpool.New(ctx, os.Getenv("AUTHORIZATION_TEST_SETUP_URL"))
	if err != nil {
		t.Fatal("setup pool initialization failed")
	}
	defer setup.Close()
	app, err := pgxpool.New(ctx, os.Getenv("AUTHORIZATION_TEST_BACKEND_URL"))
	if err != nil {
		t.Fatal("backend pool initialization failed")
	}
	defer app.Close()

	testPostgresAdminGatewayInventory(ctx, t, setup, app)
}
