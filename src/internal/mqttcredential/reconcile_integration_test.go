package mqttcredential

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"net"
	"os"
	"testing"
	"time"

	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestStartupServicePinnedBrokerPostgres(t *testing.T) {
	if os.Getenv("TASK266_ISOLATED") != "1" {
		t.Skip("owned native broker and migration-014 PostgreSQL required")
	}
	var input struct{ Password, AppDSN, AdminDSN string }
	if json.NewDecoder(os.Stdin).Decode(&input) != nil {
		t.Fatal("fixture input")
	}
	ctx := context.Background()
	app, e := pgxpool.New(ctx, input.AppDSN)
	if e != nil {
		t.Fatal(e)
	}
	defer app.Close()
	admin, e := pgxpool.New(ctx, input.AdminDSN)
	if e != nil {
		t.Fatal(e)
	}
	defer admin.Close()
	r, e := NewPostgresRepository(app, 3*time.Second, 2)
	if e != nil {
		t.Fatal(e)
	}
	var version int
	if e = admin.QueryRow(ctx, `SELECT count(*) FROM schema_migrations WHERE version=14`).Scan(&version); e != nil || version != 1 {
		t.Fatal("migration 014 required", e)
	}
	ca, e := os.ReadFile("/ca.crt")
	if e != nil {
		t.Fatal(e)
	}
	ctrl := ControllerClient{ControlDir: "/control", Timeout: 5 * time.Second, MaxFrameBytes: 2048}
	cfg := DynSecAdapterConfig{Controller: ctrl, ProtectedUsernames: []string{"admin", BackendUsername}, Timeout: 20 * time.Second, RecoveryTimeout: 5 * time.Second}
	cfg.NewClient = func(ctx context.Context) (*DynSecClient, error) {
		for i := 0; i < 30; i++ {
			c, e := NewDynSecClient(ctx, DynSecConfig{BrokerURL: "ssl://localhost:18884", CAPEM: ca, ManagerUsername: "admin", ManagerPassword: input.Password, ProtectedUsernames: []string{BackendUsername}, Timeout: 3 * time.Second, MaxPayloadBytes: 16384, MaxInflight: 4, QueueSize: 8, ValidateTarget: func(u string) bool { return len(u) > 7 && u[:7] == "fixture" }, ValidateRole: func(u, role string) bool { return role == "gateway_"+u }, ManagementDial: ctrl.ManagementDial})
			if e == nil {
				return c, nil
			}
			select {
			case <-ctx.Done():
				return nil, ErrVerificationFailed
			case <-time.After(50 * time.Millisecond):
			}
		}
		return nil, ErrVerificationFailed
	}
	reader, e := NewDynSecSnapshotReader(DynSecSnapshotConfig{Path: "/security/dynsec.json", WriterUID: 1883, MaxBytes: 1 << 20, ValidateTarget: func(u string) bool { return len(u) > 7 && u[:7] == "fixture" }})
	if e != nil {
		t.Fatal(e)
	}
	cfg.Observe = reader.Observe
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		t.Fatal("CA")
	}
	cfg.Login = func(ctx context.Context, u, p string) error {
		conn, e := ctrl.DialManagementTLS(ctx, &tls.Config{RootCAs: roots, ServerName: "localhost", MinVersion: tls.VersionTLS12})
		if e != nil {
			return ErrVerificationFailed
		}
		defer conn.Close()
		conn.SetDeadline(time.Now().Add(3 * time.Second))
		packet := packets.NewControlPacket(packets.Connect).(*packets.ConnectPacket)
		packet.ProtocolName, packet.ProtocolVersion, packet.CleanSession = "MQTT", 4, true
		packet.ClientIdentifier, packet.UsernameFlag, packet.PasswordFlag, packet.Username, packet.Password = uuid.NewString(), true, true, u, []byte(p)
		if packet.Write(conn) != nil {
			return ErrVerificationFailed
		}
		reply, e := packets.ReadPacket(conn)
		ack, ok := reply.(*packets.ConnackPacket)
		if e != nil || !ok || ack.ReturnCode != 0 {
			return ErrVerificationFailed
		}
		return nil
	}
	actor := uuid.New()
	if _, e = admin.Exec(ctx, `INSERT INTO profiles(id) VALUES($1)`, actor); e != nil {
		t.Fatal(e)
	}
	if _, e = admin.Exec(ctx, `INSERT INTO platform_admins(user_id) VALUES($1)`, actor); e != nil {
		t.Fatal(e)
	}
	newGateway := func() string {
		g := "fixture_" + uuid.NewString()
		if _, e = admin.Exec(ctx, `INSERT INTO gateways(gateway_id,name) VALUES($1,'Startup fixture')`, g); e != nil {
			t.Fatal(e)
		}
		return g
	}
	newService := func(failOpen bool) *ProvisionService {
		a, e := NewDynSecAdapter(cfg)
		if e != nil {
			t.Fatal(e)
		}
		rt := &provisionFaultRuntime{DynSecAdapter: a, failOpen: failOpen}
		s, e := NewProvisionService(r, r, rt, ProvisionServiceOptions{RecoveryTimeout: 5 * time.Second, PageSize: 2, MaxPages: 32})
		if e != nil {
			t.Fatal(e)
		}
		return s
	}
	active := newGateway()
	activeResponse, e := newService(false).Provision(ctx, actor, MutationInput{active, uuid.New(), ActionProvision})
	if e != nil || activeResponse.Secret == nil {
		t.Fatal("active fixture", e)
	}
	rotating := newGateway()
	if _, e = newService(false).Provision(ctx, actor, MutationInput{rotating, uuid.New(), ActionProvision}); e != nil {
		t.Fatal(e)
	}
	terminal := newGateway()
	if _, e = newService(true).Provision(ctx, actor, MutationInput{terminal, uuid.New(), ActionProvision}); e == nil {
		t.Fatal("unfinished success fixture did not fail OPEN")
	}
	var terminalBefore string
	if e = admin.QueryRow(ctx, `SELECT (to_jsonb(e)-'actor_user_id')::text FROM gateway_mqtt_credential_events e WHERE gateway_id=$1`, terminal).Scan(&terminalBefore); e != nil {
		t.Fatal(e)
	}
	if _, e = r.BeginOperation(ctx, BeginRequest{actor, uuid.New(), MutationInput{rotating, uuid.New(), ActionRotate}}); e != nil {
		t.Fatal("rotate crash fixture", e)
	}
	var original Operation
	for i := 0; i < 3; i++ {
		g := "fixture_" + uuid.NewString()
		if _, e = admin.Exec(ctx, `INSERT INTO gateways(gateway_id,name) VALUES($1,'Startup fixture')`, g); e != nil {
			t.Fatal(e)
		}
		b, e := r.BeginOperation(ctx, BeginRequest{actor, uuid.New(), MutationInput{g, uuid.New(), ActionProvision}})
		if e != nil {
			t.Fatal(e)
		}
		original = b.Operation
	}
	// Durable intent survives a prior lifetime; startup must CAS rebind it before
	// observing disable in the new epoch rather than reusing old evidence.
	cp, e := r.ResolveMaintenance(ctx, original.OperationID)
	if e != nil {
		t.Fatal(e)
	}
	m0, e := r.GetMetadata(ctx, original.GatewayID)
	if e != nil {
		t.Fatal(e)
	}
	recoveryID := uuid.New()
	oldEpoch, e := freshIdentity()
	if e != nil {
		t.Fatal(e)
	}
	request := RecoveryRequest{RecoveryID: recoveryID, Guard: maintenanceGuard(original, m0, cp), BrokerEpoch: oldEpoch}
	if _, e = r.BeginRecovery(ctx, request); e != nil {
		t.Fatal("pending recovery fixture", e)
	}
	if _, e = r.BeginRecovery(ctx, request); e != nil {
		t.Fatal("same UUID replay", e)
	}
	serviceFaultHook(t, "startup-kill-pending")
	description, e := ctrl.Describe(ctx)
	if e != nil || description.Open || !description.Alive || description.Epoch == oldEpoch {
		t.Fatal("fresh PID1 must remain CLOSED", e)
	}
	// Startup attribution must survive the original human profile's deletion.
	if _, e = admin.Exec(ctx, `DELETE FROM profiles WHERE id=$1`, actor); e != nil {
		t.Fatal("deleted actor fixture", e)
	}
	a, e := NewDynSecAdapter(cfg)
	if e != nil {
		t.Fatal(e)
	}
	backendChecks := 0
	faultRuntime := &startupNativeFaultRuntime{DynSecAdapter: a, t: t, target: rotating, mode: "unreadable"}
	ambiguous := &startupRecoveryCommitSeam{RecoveryRepository: r}
	s, e := NewStartupReconciler(r, r, ambiguous, faultRuntime, StartupOptions{Timeout: time.Minute, PageSize: 2, MaxPages: 16, VerifyBackend: func(ctx context.Context, receipt VerificationReceipt) error {
		// Real correlated manager traffic, not public TCP liveness. Fixture manager
		// backend role bootstrap is established by the shared fixture.
		backendChecks++
		c, e := cfg.NewClient(ctx)
		if e != nil {
			return e
		}
		defer c.Close()
		_, e = c.GetClient(ctx, "fixture_absent_backend_check")
		if e != errDynSecClientAbsent {
			return ErrVerificationFailed
		}
		return nil
	}})
	if e != nil {
		t.Fatal(e)
	}
	for _, mode := range []string{"unreadable", "save-fault"} {
		faultRuntime.mode = mode
		if e = s.Run(ctx); e == nil || s.Ready() {
			t.Fatal("native fault became recovery proof", mode)
		}
		if !faultRuntime.injected {
			t.Fatal("native fault hook not reached", mode)
		}
		serviceFaultHook(t, map[string]string{"unreadable": "startup-readable", "save-fault": "startup-save-restore"}[mode])
		faultRuntime.injected = false
		pending, lookupErr := r.ListPendingRecovery(ctx, uuid.Nil, 2)
		if lookupErr != nil || len(pending) == 0 {
			t.Fatal("fault lost durable pending fence", lookupErr)
		}
		if d, err := ctrl.Describe(ctx); err != nil || d.Open {
			t.Fatal("fault left ingress OPEN", err)
		}
	}
	faultRuntime.mode = ""
	if e = s.Run(ctx); e != nil {
		t.Fatal("startup absent-principal recovery", e)
	}
	if ambiguous.resolved == 0 {
		t.Fatal("actual recovery UUID resolution not exercised")
	}
	resolved, e := r.ResolveRecovery(ctx, recoveryID)
	if e != nil || resolved.Status != RecoveryDisabled || resolved.BrokerEpoch == request.BrokerEpoch {
		t.Fatal("recovery did not rebind current epoch", e)
	}
	if !s.Ready() || backendChecks != 1 {
		t.Fatal("startup barrier")
	}
	o, e := r.ResolveCommitAmbiguity(ctx, original.OperationID)
	if e != nil || o.Status != original.Status || o.RecoveryID == nil {
		t.Fatal("original event rewritten", e)
	}
	m, e := r.GetMetadata(ctx, original.GatewayID)
	if e != nil || m.Status != CredentialRevoked || m.CredentialVersion != original.CredentialVersion {
		t.Fatal("recovery projection", e)
	}
	// Same controlled lifetime clean retry: current revokes replay without making
	// another recovery row or pretending the old password was delivered.
	manager, e := cfg.NewClient(ctx)
	if e != nil {
		t.Fatal(e)
	}
	if e = manager.EnableClient(ctx, rotating); e != nil {
		t.Fatal("stale enabled fixture", e)
	}
	manager.Close()
	if snap, err := reader.Observe(ctx, rotating); err != nil || snap.Disabled {
		t.Fatal("actual stale enabled snapshot not established", err)
	}
	if e = s.Run(ctx); e != nil {
		t.Fatal("clean recovered retry", e)
	}
	if snap, err := reader.Observe(ctx, rotating); err != nil || !snap.Disabled {
		t.Fatal("DB revoke not reapplied before OPEN", err)
	}
	var count int
	if e = admin.QueryRow(ctx, `SELECT count(*) FROM mqtt_credential_recovery`).Scan(&count); e != nil || count != 5 {
		t.Fatal("logical recovery count", e, count)
	}
	var terminalAfter string
	if e = admin.QueryRow(ctx, `SELECT (to_jsonb(e)-'actor_user_id')::text FROM gateway_mqtt_credential_events e WHERE gateway_id=$1`, terminal).Scan(&terminalAfter); e != nil || terminalAfter != terminalBefore {
		t.Fatal("terminal event altered by cleanup", e)
	}
	// Valid completed active B was not disabled by startup, on the real private
	// TLS channel after OPEN. Retention of power-loss password proof is not claimed.
	if e = cfg.Login(ctx, active, activeResponse.Secret.password); e != nil {
		t.Fatal("valid active B unusable", e)
	}
	// Admin may now allocate a new generation; it is not replay of the lost key.
	actor = uuid.New()
	if _, e = admin.Exec(ctx, `INSERT INTO profiles(id) VALUES($1)`, actor); e != nil {
		t.Fatal(e)
	}
	if _, e = admin.Exec(ctx, `INSERT INTO platform_admins(user_id) VALUES($1)`, actor); e != nil {
		t.Fatal(e)
	}
	response, e := newService(false).Provision(ctx, actor, MutationInput{original.GatewayID, uuid.New(), ActionProvision})
	if e != nil || response.Secret == nil {
		current, lookupErr := r.GetMetadata(ctx, original.GatewayID)
		if lookupErr == nil {
			event, eventErr := r.ResolveCommitAmbiguity(ctx, current.LastOperationID)
			if eventErr == nil {
				t.Logf("new generation diagnostic: generation=%d event_status=%s phase=%s positive=%t", current.CredentialVersion, event.Status, event.Phase, event.Evidence.FreshPositiveVerified)
			}
		}
		t.Fatal("new generation denied", e)
	}
	next, e := r.GetMetadata(ctx, original.GatewayID)
	if e != nil || next.Status != CredentialActive || next.CredentialVersion != original.CredentialVersion+1 {
		t.Fatal("new generation not active", e)
	}
	if e = cfg.Login(ctx, original.GatewayID, response.Secret.password); e != nil {
		t.Fatal("new generation fresh login", e)
	}
	// Bounded scan exhaustion is a real CLOSED startup pass, not truncation
	// accepted as success. Sample each fixture gate route and record duration;
	// TCP outcomes are lifecycle observations, NOT authentication-denial proof.
	s.cfg.MaxPages = 1
	if e = s.Run(ctx); e == nil || s.Ready() {
		t.Fatal("pagination budget silently truncated inventory")
	}
	for _, route := range []string{"broker:8883", "broker:8884"} {
		start := time.Now()
		conn, dialErr := net.DialTimeout("tcp", route, time.Second)
		outcome := "dial-rejected"
		if dialErr == nil {
			outcome = "tcp-accepted"
			conn.Close()
		}
		t.Logf("CLOSED gate sample route=%s outcome=%s elapsed_ms=%d (not authentication evidence)", route, outcome, time.Since(start).Milliseconds())
	}
	if d, err := ctrl.Describe(ctx); err != nil || d.Open {
		t.Fatal("budget exhaustion did not stay CLOSED", err)
	}
	s.cfg.MaxPages = 16
	if e = s.Run(ctx); e != nil || !s.Ready() {
		t.Fatal("operator budget retry", e)
	}
	if e = cfg.Login(ctx, active, activeResponse.Secret.password); e != nil {
		t.Fatal("active B lost after fault retries", e)
	}
	t.Log("PASS real v14 startup: paginated lost intent/rotate/terminal success cleanup, immutable terminal event, active B login, disposition, clean retry, new generation")
}

// Only native fixture controls are injected; proof still comes from the real
// correlated plugin and protected JSON reader, never manufactured booleans.
type startupNativeFaultRuntime struct {
	*DynSecAdapter
	t            *testing.T
	target, mode string
	injected     bool
}

func serviceFaultHook(t *testing.T, name string) {
	t.Helper()
	if err := waitProvisionFixture(name); err != nil {
		t.Fatal("owned startup fault handshake", name, err)
	}
}

// Application-level reply-loss seam, explicitly NOT PostgreSQL wire loss. The
// underlying commit and bounded UUID lookup both run on the real repository.
type startupRecoveryCommitSeam struct {
	RecoveryRepository
	resolved int
}

func (s *startupRecoveryCommitSeam) CompleteRecovery(ctx context.Context, q RecoveryRequest, p RecoveryDisableEvidence) (Metadata, error) {
	m, err := s.RecoveryRepository.CompleteRecovery(ctx, q, p)
	if err == nil {
		return Metadata{}, &CommitOutcomeUnknown{OperationID: q.RecoveryID}
	}
	return m, err
}
func (s *startupRecoveryCommitSeam) ResolveRecovery(ctx context.Context, id uuid.UUID) (RecoveryRecord, error) {
	s.resolved++
	return s.RecoveryRepository.ResolveRecovery(ctx, id)
}

func (f *startupNativeFaultRuntime) DisableStartup(ctx context.Context, r VerificationReceipt, u string) (RecoveryDisableEvidence, error) {
	if f.mode != "" && !f.injected && (f.mode == "unreadable" || u == f.target) {
		serviceFaultHook(f.t, "startup-"+f.mode)
		f.injected = true
	}
	return f.DynSecAdapter.DisableStartup(ctx, r, u)
}

// Real v13 application-role counterexample: both unresolved events and terminal
// success with unfinished maintenance need a recovery contract, not blind OPEN.
// No broker observation is fabricated here; proof flags exercise the existing
// repository contract only. Actual native startup scenarios remain NOT RUN.
func TestPostgresStartupRecoveryContractBlocker(t *testing.T) {
	if os.Getenv("TASK263A_APP_DSN") == "" {
		t.Skip("isolated PostgreSQL fixture required")
	}
	ctx := context.Background()
	app, e := pgxpool.New(ctx, os.Getenv("TASK263A_APP_DSN"))
	if e != nil {
		t.Fatal(e)
	}
	defer app.Close()
	admin, e := pgxpool.New(ctx, os.Getenv("TASK263A_ADMIN_DSN"))
	if e != nil {
		t.Fatal(e)
	}
	defer admin.Close()
	r, e := NewPostgresRepository(app, 3*time.Second, 1000)
	if e != nil {
		t.Fatal(e)
	}
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, e := admin.Exec(ctx, sql, args...); e != nil {
			t.Fatal(e)
		}
	}
	actor := uuid.New()
	exec(`INSERT INTO profiles(id) VALUES($1)`, actor)
	exec(`INSERT INTO platform_admins(user_id) VALUES($1)`, actor)
	for _, terminal := range []bool{false, true} {
		name := "recovery_needed"
		if terminal {
			name = "succeeded_unfinished_checkpoint"
		}
		t.Run(name, func(t *testing.T) {
			g := "fixture_" + uuid.NewString()
			exec(`INSERT INTO gateways(gateway_id,name) VALUES($1,'Startup contract fixture')`, g)
			b, e := r.BeginOperation(ctx, BeginRequest{actor, uuid.New(), MutationInput{g, uuid.New(), ActionProvision}})
			if e != nil {
				t.Fatal(e)
			}
			cp, e := r.ResolveMaintenance(ctx, b.Operation.OperationID)
			if e != nil {
				t.Fatal(e)
			}
			outcome, proof := ExecutionRecoveryRequired, VerificationEvidence{true, true, false}
			if terminal {
				outcome, proof = ExecutionVerifiedSuccess, VerificationEvidence{true, true, true}
			}
			c, e := CompleteOperation(b.Operation, outcome, proof, time.Now().UTC())
			if e != nil {
				t.Fatal(e)
			}
			u := OperationUpdate{operationGuard(b.Operation, b.Metadata), c}
			var m Metadata
			if terminal {
				m, e = r.ConditionalFinalize(ctx, u)
			} else {
				m, e = r.RequireRecovery(ctx, u)
			}
			if e != nil {
				t.Fatal(e)
			}
			unresolved, e := r.ListUnresolved(ctx, uuid.Nil, 1000)
			if e != nil {
				t.Fatal(e)
			}
			found := false
			for _, o := range unresolved {
				found = found || o.OperationID == b.Operation.OperationID
			}
			if found == terminal {
				t.Fatal("event scan membership inconsistent")
			}
			pending, e := r.ListPendingMaintenance(ctx, uuid.Nil, 1000)
			if e != nil {
				t.Fatal(e)
			}
			found = false
			for _, p := range pending {
				found = found || p.OperationID == b.Operation.OperationID
			}
			if !found {
				t.Fatal("unfinished maintenance missing from separate scan")
			}
			epoch, e := freshIdentity()
			if e != nil {
				t.Fatal(e)
			}
			guard := maintenanceGuard(c.Operation, m, cp)
			cp, e = r.BindMaintenanceEpoch(ctx, guard, epoch)
			if e != nil {
				t.Fatal(e)
			}
			guard = maintenanceGuard(c.Operation, m, cp)
			if !terminal {
				_, e = r.CompleteMaintenance(ctx, guard)
				requireCode(t, e, CodeCredentialConflict)
			}
			// Existing DB trigger forbids the proposed escape through failed and
			// preserves terminal success. Privileged SQL is not a recovery API.
			if _, e = admin.Exec(ctx, `UPDATE gateway_mqtt_credential_events SET status='failed',phase='finalize',completed_at=now() WHERE operation_id=$1`, b.Operation.OperationID); e == nil {
				t.Fatal("unsafe rewrite accepted by database")
			}
			for _, action := range []Action{ActionProvision, ActionRotate, ActionRevoke} {
				_, e = r.BeginOperation(ctx, BeginRequest{actor, uuid.New(), MutationInput{g, uuid.New(), action}})
				code := CodeRecoveryRequired
				if terminal {
					code = CodeOperationInProgress
				}
				requireCode(t, e, code)
			}
			current, e := r.GetMetadata(ctx, g)
			if e != nil || current.Status != m.Status || current.LastOperationID != m.LastOperationID {
				t.Fatal("denied recovery mutated authority", e)
			}
		})
	}
}
