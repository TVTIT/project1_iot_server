package mqttcredential

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresAuthorityGuards(t *testing.T) {
	if os.Getenv("TASK263A_APP_DSN") == "" {
		t.Skip("isolated PostgreSQL fixture required")
	}
	ctx := context.Background()
	app, err := pgxpool.New(ctx, os.Getenv("TASK263A_APP_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	admin, err := pgxpool.New(ctx, os.Getenv("TASK263A_ADMIN_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	r, _ := NewPostgresRepository(app, 3*time.Second)
	actor := uuid.New()
	g := "test_" + uuid.New().String()
	for _, statement := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO profiles(id) VALUES($1)`, []any{actor}},
		{`INSERT INTO platform_admins(user_id) VALUES($1)`, []any{actor}},
		{`INSERT INTO gateways(gateway_id,name) VALUES($1,'Authority fixture')`, []any{g}},
	} {
		if _, err = admin.Exec(ctx, statement.sql, statement.args...); err != nil {
			t.Fatal(err)
		}
	}
	b, err := r.BeginOperation(ctx, BeginRequest{actor, uuid.New(), MutationInput{g, uuid.New(), ActionProvision}})
	if err != nil {
		t.Fatal(err)
	}
	for _, mutation := range []string{
		`status='succeeded',phase='finalize',ram_applied=true,snapshot_observed=true,fresh_positive_verified=true,completed_at=now(),updated_at=now()`,
		`status='recovery_needed',phase='recovery',updated_at=now()`,
	} {
		t.Run("event-only "+mutation, func(t *testing.T) {
			tx, e := app.Begin(ctx)
			if e != nil {
				t.Fatal(e)
			}
			defer rollbackTestTransaction(t, tx)
			if _, e = tx.Exec(ctx, `UPDATE gateway_mqtt_credential_events SET `+mutation+` WHERE operation_id=$1`, b.Operation.OperationID); e != nil {
				t.Fatalf("syntactically valid transition rejected before commit: %v", e)
			}
			if e = tx.Commit(ctx); e == nil {
				t.Fatal("event-only authority mismatch committed")
			}
			var eventStatus, metadataStatus string
			if e = app.QueryRow(ctx, `SELECT e.status,c.status FROM gateway_mqtt_credential_events e JOIN gateway_mqtt_credentials c ON c.last_operation_id=e.operation_id WHERE e.operation_id=$1`, b.Operation.OperationID).Scan(&eventStatus, &metadataStatus); e != nil {
				t.Fatal(e)
			}
			if eventStatus != "pending" || metadataStatus != "provisioning" {
				t.Fatalf("failed commit changed authority: %s/%s", eventStatus, metadataStatus)
			}
		})
	}
	for _, mutation := range []string{
		`status='active',activated_at=now()`,
		`status='revoked',revoked_at=now()`,
		`credential_version=credential_version+1`,
		`last_operation_id=NULL`,
		`changed_by=NULL`,
		`updated_at=updated_at+interval '1 second'`,
		`last_error_code='internal_error'`,
		`changed_by=NULL,credential_version=credential_version+1`,
	} {
		t.Run(mutation, func(t *testing.T) {
			if _, e := app.Exec(ctx, `UPDATE gateway_mqtt_credentials SET `+mutation+` WHERE gateway_id=$1`, g); e == nil {
				t.Fatal("fabricated authority committed")
			}
		})
	}
	ep, _ := freshIdentity()
	q := RecoveryRequest{uuid.New(), MaintenanceGuard{operationGuard(b.Operation, b.Metadata), MaintenanceInProgress, nil}, ep}
	if _, err = r.BeginRecovery(ctx, q); err != nil {
		t.Fatal(err)
	}
	var before, after string
	// Fixture-only nested FK context must not bless a combined authority write.
	// Admin owns this fault trigger; the application gets no additional privilege.
	functionName := "aaa_authority_fault_" + uuid.New().String()[:8]
	if _, err = admin.Exec(ctx, `CREATE FUNCTION `+functionName+`() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF OLD.changed_by IS NOT NULL AND NEW.changed_by IS NULL THEN NEW.credential_version := OLD.credential_version+1; END IF; RETURN NEW; END $$;
		CREATE TRIGGER `+functionName+` BEFORE UPDATE ON gateway_mqtt_credentials FOR EACH ROW EXECUTE FUNCTION `+functionName+`()`); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if _, err := admin.Exec(cleanup, `DROP TRIGGER IF EXISTS `+functionName+` ON gateway_mqtt_credentials; DROP FUNCTION IF EXISTS `+functionName+`() `); err != nil {
			t.Error("fault trigger cleanup failed")
		}
	}()
	if _, err = admin.Exec(ctx, `DELETE FROM profiles WHERE id=$1`, actor); err == nil {
		t.Fatal("nested actor nulling blessed combined authority mutation")
	}
	if _, err = admin.Exec(ctx, `DROP TRIGGER `+functionName+` ON gateway_mqtt_credentials; DROP FUNCTION `+functionName+`()`); err != nil {
		t.Fatal(err)
	}
	if err = admin.QueryRow(ctx, `SELECT (to_jsonb(c)-'changed_by')::text FROM gateway_mqtt_credentials c WHERE gateway_id=$1`, g).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if _, err = admin.Exec(ctx, `DELETE FROM profiles WHERE id=$1`, actor); err != nil {
		t.Fatal(err)
	}
	if err = admin.QueryRow(ctx, `SELECT (to_jsonb(c)-'changed_by')::text FROM gateway_mqtt_credentials c WHERE gateway_id=$1 AND changed_by IS NULL`, g).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Fatal("profile deletion changed authority")
	}
	if _, err = r.CompleteRecovery(ctx, q, RecoveryDisableEvidence{BrokerEpoch: ep, RAMDisabled: true, SnapshotObserved: true}); err != nil {
		t.Fatal(err)
	}
}
