package mqttcredential

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Deliberate service fault seams, NOT actual SQL wire loss. The underlying
// transaction is real; returning unknown models loss of its acknowledgment.
type provisionFaultRepository struct {
	*PostgresRepository
	fault string
}

func (r *provisionFaultRepository) ConditionalFinalize(ctx context.Context, u OperationUpdate) (Metadata, error) {
	if r.fault == "finalize-outage" {
		return Metadata{}, &DomainError{Code: CodeServiceUnavailable}
	}
	m, e := r.PostgresRepository.ConditionalFinalize(ctx, u)
	if e == nil && r.fault == "finalize-unknown" {
		return Metadata{}, &CommitOutcomeUnknown{OperationID: u.Guard.ExpectedOperationID}
	}
	return m, e
}
func (r *provisionFaultRepository) CompleteMaintenance(ctx context.Context, g MaintenanceGuard) (MaintenanceCheckpoint, error) {
	if r.fault == "completion-outage" {
		return MaintenanceCheckpoint{}, &DomainError{Code: CodeServiceUnavailable}
	}
	cp, e := r.PostgresRepository.CompleteMaintenance(ctx, g)
	if e == nil && r.fault == "completion-unknown" {
		return MaintenanceCheckpoint{}, &CommitOutcomeUnknown{OperationID: g.ExpectedOperationID}
	}
	return cp, e
}

type provisionFaultRuntime struct {
	*DynSecAdapter
	failOpen   bool
	disconnect bool
}

func (a *provisionFaultRuntime) OpenAfterFinalization(ctx context.Context, r DynSecAdapterResult, id uuid.UUID) error {
	if a.disconnect && waitProvisionFixture("controller-disconnect") != nil {
		return ErrLifecycleUnavailable
	}
	if a.failOpen {
		return ErrLifecycleUnavailable
	}
	return a.DynSecAdapter.OpenAfterFinalization(ctx, r, id)
}

func TestProvisionServicePinnedBrokerPostgres(t *testing.T) {
	servicePinnedBrokerPostgres(t, false)
}

func TestRotateServicePinnedBrokerPostgres(t *testing.T) {
	servicePinnedBrokerPostgres(t, true)
}

func TestRevokeServicePinnedBrokerPostgres(t *testing.T) {
	servicePinnedBrokerPostgres(t, false, true)
}

func servicePinnedBrokerPostgres(t *testing.T, rotate bool, revoke ...bool) {
	if os.Getenv("TASK266_ISOLATED") != "1" {
		t.Skip("owned isolated PostgreSQL v13 + native broker fixture required")
	}
	var input struct{ Password, AppDSN, AdminDSN string }
	if json.NewDecoder(os.Stdin).Decode(&input) != nil {
		t.Fatal("fixture input")
	}
	ctx := context.Background()
	app, e := pgxpool.New(ctx, input.AppDSN)
	if e != nil {
		t.Fatal("app pool")
	}
	defer app.Close()
	var migrationCount int
	admin, e := pgxpool.New(ctx, input.AdminDSN)
	if e != nil {
		t.Fatal("admin pool")
	}
	defer admin.Close()
	if e = admin.QueryRow(ctx, `SELECT count(*) FROM schema_migrations WHERE version=14 AND name='mqtt_credential_recovery'`).Scan(&migrationCount); e != nil || migrationCount != 1 {
		t.Fatal("isolated v13 fixture required", e)
	}
	repo, e := NewPostgresRepository(app, 3*time.Second, 2)
	if e != nil {
		t.Fatal(e)
	}
	actor := uuid.New()
	sql := func(q string, args ...any) {
		t.Helper()
		if _, e := admin.Exec(ctx, q, args...); e != nil {
			t.Fatal("fixture SQL", e)
		}
	}
	sql("INSERT INTO profiles(id) VALUES($1)", actor)
	sql("INSERT INTO platform_admins(user_id) VALUES($1)", actor)
	ca, e := os.ReadFile("/ca.crt")
	if e != nil {
		t.Fatal("CA fixture")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		t.Fatal("CA fixture")
	}
	ctrl := ControllerClient{ControlDir: "/control", Timeout: 5 * time.Second, MaxFrameBytes: 2048}
	cfg := DynSecAdapterConfig{Controller: ctrl, ProtectedUsernames: []string{"admin", BackendUsername}, Timeout: 20 * time.Second, RecoveryTimeout: 5 * time.Second}
	cfg.NewClient = func(ctx context.Context) (*DynSecClient, error) {
		for i := 0; i < 30; i++ {
			c, e := NewDynSecClient(ctx, DynSecConfig{BrokerURL: "ssl://localhost:18884", CAPEM: ca, ManagerUsername: "admin", ManagerPassword: input.Password, ProtectedUsernames: []string{BackendUsername}, Timeout: 2 * time.Second, MaxPayloadBytes: 16384, MaxInflight: 4, QueueSize: 8, ValidateTarget: func(u string) bool { return len(u) > 7 && u[:7] == "fixture" }, ValidateRole: func(u, r string) bool { return r == "gateway_"+u }, ManagementDial: ctrl.ManagementDial})
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
	cfg.Login = func(ctx context.Context, u, p string) error {
		conn, e := ctrl.DialManagementTLS(ctx, &tls.Config{RootCAs: roots, ServerName: "localhost", MinVersion: tls.VersionTLS12})
		if e != nil {
			return ErrVerificationFailed
		}
		defer func() { _ = conn.Close() }() // Preserve login result on teardown.
		if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
			return ErrVerificationFailed
		}
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
	var partialGateway string
	var savePassword string
	var generatedSecrets []string
	create := func(fault string) (*ProvisionService, *provisionFaultRuntime) {
		a, e := NewDynSecAdapter(cfg)
		if e != nil {
			t.Fatal(e)
		}
		runtime := &provisionFaultRuntime{DynSecAdapter: a, failOpen: fault == "open-failure", disconnect: fault == "controller-disconnect"}
		r := &provisionFaultRepository{PostgresRepository: repo, fault: fault}
		options := ProvisionServiceOptions{RecoveryTimeout: 10 * time.Second, PageSize: 2, MaxPages: 32}
		options.GeneratePassword = func() (string, error) {
			p, e := generatePassword()
			if e == nil {
				generatedSecrets = append(generatedSecrets, p)
			}
			return p, e
		}
		if fault == "rng" {
			options.GeneratePassword = func() (string, error) { return "", errors.New("private RNG failure") }
		}
		if fault == "partial-role" {
			options.GeneratePassword = func() (string, error) {
				// Native preflight already saw absence. Race a native createClient
				// here: adapter creates the role, then its createClient fails.
				c, e := cfg.NewClient(ctx)
				if e != nil {
					return "", e
				}
				defer c.Close()
				if c.CreateClient(ctx, partialGateway) != nil {
					return "", ErrVerificationFailed
				}
				p, e := generatePassword()
				if e == nil {
					generatedSecrets = append(generatedSecrets, p)
				}
				return p, e
			}
		}
		if fault == "save-fault" {
			options.GeneratePassword = func() (string, error) {
				if waitProvisionFixture("save-fault") != nil {
					return "", ErrVerificationFailed
				}
				p, e := generatePassword()
				savePassword = p
				if e == nil {
					generatedSecrets = append(generatedSecrets, p)
				}
				return p, e
			}
		}
		s, e := NewProvisionService(r, r, runtime, options)
		if e != nil {
			t.Fatal(e)
		}
		return s, runtime
	}
	if len(revoke) == 1 && revoke[0] {
		revokePinnedCases(ctx, t, repo, actor, sql, cfg, ctrl, create)
		return
	}
	// Every case uses an independent process-owned service/adapter and a new
	// Gateway. Recovery rows deliberately block future unrelated OPEN attempts.
	var revokedGateway, revokedPassword string
	var unaffectedGateway, unaffectedPassword string
	// Fresh DB authority and unknown Gateway are checked without runtime mutation.
	deniedService, _ := create("")
	deniedActor := uuid.New()
	sql("INSERT INTO profiles(id) VALUES($1)", deniedActor)
	if _, e := deniedService.Provision(ctx, deniedActor, MutationInput{"fixture_denied", uuid.New(), ActionProvision}); e == nil {
		t.Fatal("nonadmin admitted")
	}
	if _, e := deniedService.Provision(ctx, actor, MutationInput{"fixture_unknown", uuid.New(), ActionProvision}); e == nil {
		t.Fatal("unknown Gateway admitted")
	}
	var count int
	if e := admin.QueryRow(ctx, "SELECT count(*) FROM gateway_mqtt_credential_events").Scan(&count); e != nil || count != 0 {
		t.Fatal("denial persisted intent")
	}
	t.Log("PASS real platform-admin/unknown Gateway denial no intent/runtime")
	if rotate {
		unaffectedGateway = "fixture_" + uuid.NewString()
		sql("INSERT INTO gateways(gateway_id,name) VALUES($1,'Unaffected Gateway')", unaffectedGateway)
		v, x := deniedService.Provision(ctx, actor, MutationInput{unaffectedGateway, uuid.New(), ActionProvision})
		if x != nil || v.Secret == nil {
			t.Fatal("unaffected Gateway fixture", x)
		}
		unaffectedPassword = v.Secret.password
		v.Secret.ClearSecret()
		g := "fixture_" + uuid.NewString()
		sql("INSERT INTO gateways(gateway_id,name) VALUES($1,'Unprovisioned rotate fixture')", g)
		for _, target := range []string{g, BackendUsername, "admin"} {
			if _, x := deniedService.Rotate(ctx, actor, MutationInput{target, uuid.New(), ActionRotate}); x == nil {
				t.Fatal("unprovisioned/protected rotate admitted")
			}
		}
		if _, x := deniedService.Rotate(ctx, deniedActor, MutationInput{g, uuid.New(), ActionRotate}); x == nil {
			t.Fatal("nonadmin rotate admitted")
		}
		sql("DELETE FROM platform_admins WHERE user_id=$1", actor)
		if _, x := deniedService.Rotate(ctx, actor, MutationInput{g, uuid.New(), ActionRotate}); x == nil {
			t.Fatal("withdrawn admin rotate admitted")
		}
		sql("INSERT INTO platform_admins(user_id) VALUES($1)", actor)
		if x := admin.QueryRow(ctx, "SELECT count(*) FROM gateway_mqtt_credential_events WHERE gateway_id<>$1", unaffectedGateway).Scan(&count); x != nil || count != 0 {
			t.Fatal("rotate denial persisted intent")
		}
	}
	faults := []string{"", "finalize-unknown", "completion-unknown", "completion-outage", "open-failure", "finalize-outage", "rng", "partial-role", "save-fault"}
	if rotate {
		faults = []string{"", "finalize-unknown", "completion-unknown", "completion-outage", "open-failure", "finalize-outage", "rng", "legacy-role", "save-fault"}
	}
	for _, fault := range faults {
		t.Run(fault, func(t *testing.T) {
			// Previous error cases are deliberately left durable; run explicit test-only
			// DB cleanup in dependency order, never relabel immutable success evidence.
			if fault == "open-failure" || fault == "finalize-outage" || fault == "rng" || fault == "partial-role" || fault == "legacy-role" || fault == "save-fault" {
				sql("DELETE FROM mqtt_credential_maintenance WHERE status<>'completed'")
				sql("DELETE FROM gateway_mqtt_credentials WHERE status='recovery_needed'")
				sql("DELETE FROM gateway_mqtt_credential_events WHERE status='recovery_needed'")
			}
			g := "fixture_" + uuid.NewString()
			partialGateway = g
			sql("INSERT INTO gateways(gateway_id,name) VALUES($1,'Service fixture')", g)
			var oldPassword string
			if rotate {
				baseline, _ := create("")
				prior, x := baseline.Provision(ctx, actor, MutationInput{g, uuid.New(), ActionProvision})
				if x != nil || prior.Secret == nil || prior.CredentialVersion != 1 {
					t.Fatal("rotate prerequisite provision", x)
				}
				oldPassword = prior.Secret.password
				prior.Secret.ClearSecret()
				if fault == "legacy-role" {
					client, x := cfg.NewClient(ctx)
					if x != nil {
						t.Fatal("legacy native fixture", x)
					}
					_, x = client.adapterRequest(ctx, "removeClientRole", map[string]any{"username": g, "rolename": "gateway_" + g})
					client.Close()
					if x != nil {
						t.Fatal("legacy native role removal", x)
					}
				}
			}
			s, a := create(fault)
			in := MutationInput{g, uuid.New(), ActionProvision}
			mutate := s.Provision
			if rotate {
				in.Action = ActionRotate
				mutate = s.Rotate
			}
			v, e := mutate(ctx, actor, in)
			good := fault == "" || fault == "finalize-unknown" || fault == "completion-unknown"
			if good != (e == nil) {
				t.Fatalf("service outcome %T", e)
			}
			m, x := repo.GetMetadata(ctx, g)
			if x != nil {
				t.Fatal(x)
			}
			cp, x := repo.ResolveMaintenance(ctx, m.LastOperationID)
			if x != nil {
				t.Fatal(x)
			}
			op, x := repo.ResolveCommitAmbiguity(ctx, m.LastOperationID)
			if x != nil {
				t.Fatal(x)
			}
			d, x := ctrl.Describe(ctx)
			if x != nil {
				t.Fatal(x)
			}
			if good {
				if v.Secret == nil || cp.Status != MaintenanceCompleted || m.Status != CredentialActive || op.Status != OperationSucceeded || !op.Evidence.FreshPositiveVerified || !d.Open {
					t.Fatal("not committed verified completion")
				}
				if cfg.Login(ctx, g, v.Secret.password) != nil {
					t.Fatal("fresh native NEW login")
				}
				if rotate && (m.CredentialVersion != 2 || cfg.Login(ctx, g, oldPassword) == nil) {
					t.Fatal("rotate version or cold OLD rejection")
				}
				if rotate && cfg.Login(ctx, unaffectedGateway, unaffectedPassword) != nil {
					t.Fatal("rotate damaged unrelated Gateway credential")
				}
				if revokedGateway != "" {
					c, x := cfg.NewClient(ctx)
					if x != nil {
						t.Fatal(x)
					}
					native, x := c.GetClient(ctx, revokedGateway)
					c.Close()
					if x != nil || !native.Disabled || cfg.Login(ctx, revokedGateway, revokedPassword) == nil {
						t.Fatal("current other-Gateway revoke not reconciled")
					}
					t.Log("PASS current authoritative other-Gateway revocation replayed under same pending epoch before OPEN")
				}
				beforeGeneration := len(generatedSecrets)
				replay, x := mutate(ctx, actor, in)
				if x != nil || replay.Secret != nil || replay.SecretReturned {
					t.Fatal("lost response replay returned secret")
				}
				if len(generatedSecrets) != beforeGeneration {
					t.Fatal("replay generated another credential")
				}
				if rotate {
					if _, x := s.Provision(ctx, actor, MutationInput{g, in.IdempotencyKey, ActionProvision}); x == nil {
						t.Fatal("cross-action key admitted")
					}
					if revokedGateway != "" {
						if _, x := s.Rotate(ctx, actor, MutationInput{revokedGateway, in.IdempotencyKey, ActionRotate}); x == nil {
							t.Fatal("cross-Gateway key admitted")
						}
					}
				}
				if fault == "" {
					revokedGateway, revokedPassword = g, v.Secret.password
					// Real repository + native revoke proof, followed by intentional
					// native drift to exercise current authority on next provision.
					b, x := repo.BeginOperation(ctx, BeginRequest{actor, uuid.New(), MutationInput{g, uuid.New(), ActionRevoke}})
					if x != nil {
						t.Fatal(x)
					}
					r, x := a.Execute(ctx, b.Operation, nil)
					if x != nil {
						t.Fatal(x)
					}
					cp, x := repo.ResolveMaintenance(ctx, b.Operation.OperationID)
					if x != nil {
						t.Fatal(x)
					}
					cp, x = repo.BindMaintenanceEpoch(ctx, maintenanceGuard(b.Operation, b.Metadata, cp), r.receipt.Epoch)
					if x != nil {
						t.Fatal(x)
					}
					c, x := CompleteOperation(b.Operation, r.Outcome, r.Evidence, time.Now().UTC())
					if x != nil {
						t.Fatal(x)
					}
					m, x := repo.ConditionalFinalize(ctx, OperationUpdate{operationGuard(b.Operation, b.Metadata), c})
					if x != nil {
						t.Fatal(x)
					}
					if a.OpenAfterFinalization(ctx, r, b.Operation.OperationID) != nil {
						t.Fatal("revocation DB finalization OPEN")
					}
					if _, x = repo.CompleteMaintenance(ctx, maintenanceGuard(c.Operation, m, cp)); x != nil {
						t.Fatal(x)
					}
					client, x := cfg.NewClient(ctx)
					if x != nil {
						t.Fatal(x)
					}
					if client.EnableClient(ctx, g) != nil {
						t.Fatal("native drift fixture")
					}
					client.Close()
					if rotate {
						if _, x := s.Rotate(ctx, actor, MutationInput{g, uuid.New(), ActionRotate}); x == nil {
							t.Fatal("revoked rotate admitted")
						}
					}
				}
				v.Secret.ClearSecret()
				t.Log("PASS real v13 app-role intent/finalization/completion; native cold NEW login; replay metadata-only", fault)
			} else {
				if v.Secret != nil || d.Open || cp.Status != MaintenanceRecoveryNeeded {
					t.Fatal("failure leaked secret/OPEN or lost checkpoint")
				}
				if fault == "completion-outage" || fault == "open-failure" {
					if op.Status != OperationSucceeded || m.Status != CredentialActive {
						t.Fatal("rewrote terminal SUCCESS")
					}
				}
				if fault == "rng" && !rotate {
					c, x := cfg.NewClient(ctx)
					if x != nil {
						t.Fatal(x)
					}
					_, x = c.GetClient(ctx, g)
					c.Close()
					if !errors.Is(x, errDynSecClientAbsent) {
						t.Fatal("RNG failure native mutation")
					}
				}
				if fault == "partial-role" {
					c, x := cfg.NewClient(ctx)
					if x != nil {
						t.Fatal(x)
					}
					native, x := c.GetClient(ctx, g)
					_, roleErr := c.adapterRequest(ctx, "getRole", map[string]any{"rolename": "gateway_" + g})
					c.Close()
					if x != nil || !native.Disabled || roleErr != nil {
						t.Fatal("partial native role/client oracle")
					}
					t.Log("PASS native partial role success then createClient conflict; durable recovery CLOSED")
				}
				if fault == "save-fault" {
					if op.Evidence.FreshPositiveVerified {
						t.Fatal("save failure became verified success")
					}
					if savePassword == "" || cfg.Login(ctx, g, savePassword) == nil {
						t.Fatal("cold broker accepted unsaved NEW")
					}
					savePassword = ""
					t.Log("PASS native snapshot save permission fault; cold broker rejects unsaved NEW; no active metadata or secret")
				}
				if rotate {
					if fault == "legacy-role" && (op.Evidence != (VerificationEvidence{}) || op.Status != OperationRecoveryNeeded) {
						t.Fatal("legacy active metadata silently accepted native role mismatch")
					}
					_, x := s.Rotate(ctx, actor, MutationInput{g, uuid.New(), ActionRotate})
					if x == nil {
						t.Fatal("new rotate key bypassed unresolved recovery checkpoint")
					}
				}
				_ = a
				t.Log("PASS real service durable recovery CLOSED no secret; intentional fault seam", fault)
			}
			// Explicit safe columns are scanned, not password_hash/native snapshot.
			var leaked bool
			if x := admin.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM gateway_mqtt_credential_events WHERE gateway_id=$1 AND (phase LIKE '%password%' OR COALESCE(error_code,'') LIKE '%private%'))`, g).Scan(&leaked); x != nil || leaked {
				t.Fatal("safe audit secret scan")
			}
			for _, secret := range generatedSecrets {
				// Explicit business/audit columns only, no native password_hash.
				if x := admin.QueryRow(ctx, `SELECT EXISTS (
				 SELECT 1 FROM gateway_mqtt_credentials WHERE strpos(jsonb_build_object('gateway_id',gateway_id,'version',credential_version,'status',status,'last_operation_id',last_operation_id,'last_error_code',last_error_code)::text,$1)>0
				 UNION ALL SELECT 1 FROM gateway_mqtt_credential_events WHERE strpos(jsonb_build_object('operation_id',operation_id,'gateway_id',gateway_id,'action',action,'status',status,'phase',phase,'error_code',error_code,'ram_applied',ram_applied,'snapshot_observed',snapshot_observed,'fresh_positive_verified',fresh_positive_verified)::text,$1)>0
				 UNION ALL SELECT 1 FROM mqtt_credential_maintenance WHERE strpos(jsonb_build_object('operation_id',operation_id,'gateway_id',gateway_id,'status',status,'epoch',broker_epoch,'error_code',error_code)::text,$1)>0
				)`, secret).Scan(&leaked); x != nil || leaked {
					t.Fatal("secret persisted in explicit audit columns")
				}
			}
		})
	}
	if waitProvisionFixture("save-restore") != nil {
		t.Fatal("owned save permissions restore")
	}
	sql("DELETE FROM mqtt_credential_maintenance WHERE status<>'completed'")
	sql("DELETE FROM gateway_mqtt_credentials WHERE status='recovery_needed'")
	sql("DELETE FROM gateway_mqtt_credential_events WHERE status='recovery_needed'")
	if rotate {
		t.Log("PASS ROTATE real v13 app-role/native broker cold OLD rejection, NEW acceptance, replay and recovery fences")
	}
	g := "fixture_" + uuid.NewString()
	sql("INSERT INTO gateways(gateway_id,name) VALUES($1,'Disconnect fixture')", g)
	if rotate {
		baseline, _ := create("")
		v, x := baseline.Provision(ctx, actor, MutationInput{g, uuid.New(), ActionProvision})
		if x != nil || v.Secret == nil {
			t.Fatal("disconnect rotate prerequisite", x)
		}
		v.Secret.ClearSecret()
	}
	s, _ := create("controller-disconnect")
	mutate, action := s.Provision, ActionProvision
	if rotate {
		mutate, action = s.Rotate, ActionRotate
	}
	v, e := mutate(ctx, actor, MutationInput{g, uuid.New(), action})
	m, x := repo.GetMetadata(ctx, g)
	if x != nil {
		t.Fatal(x)
	}
	cp, x := repo.ResolveMaintenance(ctx, m.LastOperationID)
	if x != nil {
		t.Fatal(x)
	}
	op, x := repo.ResolveCommitAmbiguity(ctx, m.LastOperationID)
	if x != nil {
		t.Fatal(x)
	}
	if e == nil || v.Secret != nil || op.Status != OperationSucceeded || cp.Status != MaintenanceRecoveryNeeded || !s.poisoned {
		t.Fatal("actual controller disconnect rewrote success or disclosed secret")
	}
	t.Log("PASS actual controller disconnect before OPEN ACK; terminal SUCCESS preserved durable maintenance recovery no secret")
}

func waitProvisionFixture(name string) error {
	if os.WriteFile("/artifacts/"+name+"-ready", []byte("ready"), 0600) != nil {
		return ErrVerificationFailed
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, e := os.Stat("/artifacts/" + name + "-armed"); e == nil {
			return nil
		}
		time.Sleep(25 * time.Millisecond)
	}
	return ErrVerificationFailed
}
