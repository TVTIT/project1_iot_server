package mqttcredential

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Native app connections (not SET ROLE): explicit public DML under hostile
// caller paths. Temp privileges are intentionally retained.
func TestPostgresReplacementSearchPathSecurity(t *testing.T) {
	if os.Getenv("TASK284_REPLACEMENT_APP_DSN") == "" {
		t.Skip("owned security fixture required")
	}
	ctx := context.Background()
	open := func(key string) *pgxpool.Pool {
		p, err := pgxpool.New(ctx, os.Getenv(key))
		if err != nil {
			t.Fatal("fixture connection")
		}
		t.Cleanup(p.Close)
		return p
	}
	app, admin := open("TASK284_REPLACEMENT_APP_DSN"), open("TASK284_REPLACEMENT_ADMIN_DSN")
	r, _ := NewPostgresRepository(app, 5*time.Second)
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := admin.Exec(ctx, sql, args...); err != nil {
			t.Fatal(err)
		}
	}
	actor, nonadmin := uuid.New(), uuid.New()
	exec(`INSERT INTO public.profiles(id) VALUES($1),($2)`, actor, nonadmin)
	exec(`INSERT INTO public.platform_admins(user_id) VALUES($1)`, actor)
	seed := func() BeginResult {
		t.Helper()
		g := "security_" + uuid.New().String()
		exec(`INSERT INTO public.gateways(gateway_id,name) VALUES($1,'Security fixture')`, g)
		b, err := r.BeginOperation(ctx, BeginRequest{actor, uuid.New(), MutationInput{g, uuid.New(), ActionProvision}})
		if err != nil {
			t.Fatal(err)
		}
		c, err := CompleteOperation(b.Operation, ExecutionVerifiedSuccess, VerificationEvidence{true, true, true}, time.Now().UTC())
		if err != nil {
			t.Fatal(err)
		}
		b.Metadata, err = r.ConditionalFinalize(ctx, OperationUpdate{OperationGuard{g, b.Operation.OperationID, b.Operation.Status, b.Metadata.Status, b.Metadata.CredentialVersion}, c})
		if err != nil {
			t.Fatal(err)
		}
		b.Operation = c.Operation
		mg := MaintenanceGuard{OperationGuard: OperationGuard{g, b.Operation.OperationID, b.Operation.Status, b.Metadata.Status, b.Metadata.CredentialVersion}, ExpectedStatus: MaintenanceInProgress}
		checkpoint, err := r.BindMaintenanceEpoch(ctx, mg, strings.Repeat("a", 64))
		if err != nil {
			t.Fatal(err)
		}
		mg.ExpectedEpoch = checkpoint.BrokerEpoch
		if _, err = r.CompleteMaintenance(ctx, mg); err != nil {
			t.Fatal(err)
		}
		b, err = r.BeginOperation(ctx, BeginRequest{actor, uuid.New(), MutationInput{g, uuid.New(), ActionRotate}})
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	shadow := func(t *testing.T, tx pgx.Tx, tables []string, path string) {
		t.Helper()
		for _, table := range tables {
			if _, err := tx.Exec(ctx, `CREATE TEMP TABLE `+table+` ON COMMIT DROP AS TABLE public.`+table); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := tx.Exec(ctx, `SET LOCAL search_path=`+path); err != nil {
			t.Fatal(err)
		}
	}
	all := []string{"gateways", "gateway_mqtt_credential_events", "gateway_mqtt_credentials", "mqtt_credential_maintenance", "mqtt_credential_recovery", "mqtt_rotation_replacements", "profiles", "platform_admins"}
	snapshot := func(g string) string {
		t.Helper()
		var s string
		err := admin.QueryRow(ctx, `SELECT jsonb_build_array(
		 (SELECT jsonb_agg(to_jsonb(e) ORDER BY operation_id) FROM public.gateway_mqtt_credential_events e WHERE gateway_id=$1),
		 (SELECT to_jsonb(c) FROM public.gateway_mqtt_credentials c WHERE gateway_id=$1),
		 (SELECT jsonb_agg(to_jsonb(p) ORDER BY operation_id) FROM public.mqtt_credential_maintenance p WHERE gateway_id=$1),
		 (SELECT jsonb_agg(to_jsonb(d) ORDER BY predecessor_id) FROM public.mqtt_rotation_replacements d WHERE gateway_id=$1),
		 (SELECT jsonb_agg(e.operation_id ORDER BY e.operation_id) FROM public.gateway_mqtt_credential_events e WHERE gateway_id=$1 AND public.mqtt_operation_effective(e.operation_id)))::text`, g).Scan(&s)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	// New successor intent plus automatic checkpoint and current projection.
	intent := func(tx pgx.Tx, old BeginResult, successor, who uuid.UUID, gateway string, link any) error {
		_, err := tx.Exec(ctx, `INSERT INTO public.gateway_mqtt_credential_events
		 (operation_id,gateway_id,credential_version,action,status,actor_user_id,idempotency_key,
		 previous_status,previous_credential_version,previous_activated_at,previous_revoked_at,replacement_predecessor_id)
		 SELECT $1,$2,credential_version+1,'rotate','pending',$3,$4,status,credential_version,activated_at,revoked_at,$5
		 FROM public.gateway_mqtt_credentials WHERE gateway_id=$6`, successor, gateway, who, uuid.New(), link, old.Metadata.GatewayID)
		return err
	}
	projection := func(tx pgx.Tx, old BeginResult, successor uuid.UUID) error {
		_, err := tx.Exec(ctx, `UPDATE public.gateway_mqtt_credentials c SET
		 credential_version=e.credential_version,status='rotating',last_operation_id=e.operation_id,
		 changed_by=e.actor_user_id,updated_at=e.created_at,last_error_code=NULL
		 FROM public.gateway_mqtt_credential_events e WHERE c.gateway_id=$1 AND e.operation_id=$2`, old.Metadata.GatewayID, successor)
		return err
	}
	t.Run("closure and least privileges", func(t *testing.T) {
		var count int
		if err := admin.QueryRow(ctx, `SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public' AND p.proconfig @> ARRAY['search_path=pg_catalog, public, pg_temp']`).Scan(&count); err != nil || count != 19 {
			t.Fatal("expected exact 19-function closure", count, err)
		}
		var safe bool
		err := app.QueryRow(ctx, `SELECT current_user='iot_backend_app'
		 AND has_database_privilege(current_user,current_database(),'TEMP')
		 AND NOT has_schema_privilege(current_user,'public','CREATE')
		 AND NOT has_table_privilege(current_user,'public.platform_admins','UPDATE')
		 AND NOT has_table_privilege(current_user,'public.mqtt_rotation_replacements','UPDATE')
		 AND NOT has_table_privilege(current_user,'public.mqtt_rotation_replacements','DELETE')
		 AND has_column_privilege(current_user,'public.mqtt_rotation_replacements','predecessor_id','INSERT')
		 AND NOT has_column_privilege(current_user,'public.mqtt_rotation_replacements','disposition','INSERT')
		 AND NOT has_column_privilege(current_user,'public.gateway_mqtt_credential_events','legacy_projection','INSERT')
		 AND NOT has_column_privilege(current_user,'public.gateway_mqtt_credential_events','replacement_predecessor_id','UPDATE')
		 AND has_function_privilege(current_user,'public.lock_mqtt_replacement_admin(uuid)','EXECUTE')`).Scan(&safe)
		if err != nil || !safe {
			t.Fatal("privilege boundary", err)
		}
		// Actual trigger roots must all be pinned, not only a handpicked helper.
		err = admin.QueryRow(ctx, `SELECT bool_and(p.proconfig @> ARRAY['search_path=pg_catalog, public, pg_temp']) FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid WHERE NOT t.tgisinternal AND t.tgrelid IN ('public.gateway_mqtt_credential_events'::regclass,'public.gateway_mqtt_credentials'::regclass,'public.mqtt_credential_maintenance'::regclass,'public.mqtt_credential_recovery'::regclass,'public.mqtt_rotation_replacements'::regclass)`).Scan(&safe)
		if err != nil || !safe {
			t.Fatal("unprotected trigger root", err)
		}
	})
	for _, tc := range []struct {
		name, path string
		tables     []string
	}{
		{"event", "pg_temp,public", []string{"gateway_mqtt_credential_events"}},
		{"projection", "pg_temp,public", []string{"gateway_mqtt_credentials"}},
		{"checkpoint", "pg_temp,public", []string{"mqtt_credential_maintenance"}},
		{"disposition", "pg_temp,public", []string{"mqtt_rotation_replacements"}},
		{"all explicit temp first", "pg_temp,public", all},
		{"all implicit temp precedence", "pg_catalog,public", all},
	} {
		t.Run(tc.name+" valid atomic replacement and inherited truth", func(t *testing.T) {
			old := seed()
			tx, err := app.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer rollbackTestTransaction(t, tx)
			shadow(t, tx, tc.tables, tc.path)
			// Corrupt every shadow; real validators must ignore it.
			for _, table := range tc.tables {
				if _, err = tx.Exec(ctx, `DELETE FROM pg_temp.`+table); err != nil {
					t.Fatal(err)
				}
			}
			if _, err = tx.Exec(ctx, `SELECT public.validate_mqtt_projection_authority($1),public.validate_mqtt_replacement_inventory($1)`, old.Metadata.GatewayID); err != nil {
				t.Fatal("validator did not resolve public truth", err)
			}
			successor := uuid.New()
			if _, err = tx.Exec(ctx, `INSERT INTO public.mqtt_rotation_replacements(predecessor_id,successor_id,gateway_id) VALUES($1,$2,$3)`, old.Operation.OperationID, successor, old.Metadata.GatewayID); err != nil {
				t.Fatal(err)
			}
			if err = intent(tx, old, successor, actor, old.Metadata.GatewayID, old.Operation.OperationID); err != nil {
				t.Fatal(err)
			}
			if err = projection(tx, old, successor); err != nil {
				t.Fatal(err)
			}
			if err = tx.Commit(ctx); err != nil {
				t.Fatal("valid replacement failed hostile path", err)
			}
			var ok bool
			if err = admin.QueryRow(ctx, `SELECT NOT public.mqtt_operation_effective($1) AND public.mqtt_operation_effective($2) AND EXISTS(SELECT 1 FROM public.gateway_mqtt_credentials WHERE gateway_id=$3 AND last_operation_id=$2) AND EXISTS(SELECT 1 FROM public.mqtt_credential_maintenance WHERE operation_id=$2)`, old.Operation.OperationID, successor, old.Metadata.GatewayID).Scan(&ok); err != nil || !ok {
				t.Fatal("public atomic result", err)
			}
		})
	}
	for _, mode := range []string{"crossGateway", "missinglink", "current spoof", "nonadmin directinsert", "wrongaction"} {
		t.Run(mode+" rejected without public change", func(t *testing.T) {
			old := seed()
			if mode == "wrongaction" {
				// A real provision is not eligible even if temp events say rotate.
				g := "security_" + uuid.New().String()
				exec(`INSERT INTO public.gateways(gateway_id,name) VALUES($1,'Security fixture')`, g)
				var err error
				old, err = r.BeginOperation(ctx, BeginRequest{actor, uuid.New(), MutationInput{g, uuid.New(), ActionProvision}})
				if err != nil {
					t.Fatal(err)
				}
			}
			before := snapshot(old.Metadata.GatewayID)
			tx, err := app.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer rollbackTestTransaction(t, tx)
			shadow(t, tx, all, "pg_temp,public")
			if _, err = tx.Exec(ctx, `UPDATE pg_temp.gateway_mqtt_credential_events SET action='rotate'`); err != nil {
				t.Fatal(err)
			}
			if _, err = tx.Exec(ctx, `INSERT INTO pg_temp.platform_admins(user_id) VALUES($1)`, nonadmin); err != nil {
				t.Fatal(err)
			}
			successor := uuid.New()
			_, err = tx.Exec(ctx, `INSERT INTO public.mqtt_rotation_replacements(predecessor_id,successor_id,gateway_id) VALUES($1,$2,$3)`, old.Operation.OperationID, successor, old.Metadata.GatewayID)
			if err == nil {
				who, gateway := actor, old.Metadata.GatewayID
				var link any = old.Operation.OperationID
				switch mode {
				case "crossGateway":
					gateway = seed().Metadata.GatewayID
				case "missinglink":
					link = nil
				case "nonadmin directinsert":
					who = nonadmin
				}
				err = intent(tx, old, successor, who, gateway, link)
				if err == nil && mode != "current spoof" {
					err = projection(tx, old, successor)
				}
				if err == nil && mode == "current spoof" {
					_, err = tx.Exec(ctx, `UPDATE pg_temp.gateway_mqtt_credentials SET last_operation_id=$1 WHERE gateway_id=$2`, successor, old.Metadata.GatewayID)
				}
			}
			if err == nil {
				err = tx.Commit(ctx)
			} else {
				_ = tx.Rollback(ctx)
			}
			if err == nil {
				t.Fatal("invalid structural transaction committed")
			}
			var pgerr *pgconn.PgError
			if !errors.As(err, &pgerr) || pgerr.Code != "P0001" {
				t.Fatal("expected structural guard exception, not incidental SQL error", err)
			}
			if after := snapshot(old.Metadata.GatewayID); after != before {
				t.Fatal("audit/checkpoint/sidecar/projection/effective scans changed")
			}
		})
	}
}
