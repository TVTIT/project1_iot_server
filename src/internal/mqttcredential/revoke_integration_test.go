package mqttcredential

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"os"
	"testing"
	"time"

	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/google/uuid"
)

func integrationRejected(ctx context.Context, ctrl ControllerClient, roots *x509.CertPool, user, password string) error {
	conn, e := ctrl.DialManagementTLS(ctx, &tls.Config{RootCAs: roots, ServerName: "localhost", MinVersion: tls.VersionTLS12})
	if e != nil {
		return ErrVerificationFailed
	}
	defer func() { _ = conn.Close() }()
	if conn.SetDeadline(time.Now().Add(3*time.Second)) != nil {
		return ErrVerificationFailed
	}
	p := packets.NewControlPacket(packets.Connect).(*packets.ConnectPacket)
	p.ProtocolName, p.ProtocolVersion, p.CleanSession = "MQTT", 4, true
	p.ClientIdentifier, p.UsernameFlag, p.PasswordFlag, p.Username, p.Password = uuid.NewString(), true, true, user, []byte(password)
	defer clear(p.Password)
	if p.Write(conn) != nil {
		return ErrVerificationFailed
	}
	r, e := packets.ReadPacket(conn)
	ack, ok := r.(*packets.ConnackPacket)
	if e != nil || !ok || (ack.ReturnCode != packets.ErrRefusedBadUsernameOrPassword && ack.ReturnCode != packets.ErrRefusedNotAuthorised) {
		return ErrVerificationFailed
	}
	return nil
}

// Revoke-only application seams. Native Execute and PostgreSQL writes remain
// real. The marker is reached AFTER correlated native disable/cold-load proof,
// not after an invented MQTT PUBACK. Python kills the owned controller PID1.
type revokeKillRuntime struct {
	ProvisionRuntime
	marker string
	result DynSecAdapterResult
	calls  int
	before func()
}

func (r *revokeKillRuntime) Execute(ctx context.Context, o Operation, generate func(context.Context) (string, error)) (DynSecAdapterResult, error) {
	r.calls++
	if r.before != nil {
		r.before()
	}
	v, e := r.ProvisionRuntime.Execute(ctx, o, generate)
	r.result = v
	if e == nil && r.marker == "revoke-kill-execute" {
		e = waitProvisionFixture(r.marker)
		if e == nil {
			// The controller lifetime that supplied this proof has been killed.
			// Model the caller observing that loss, rather than forwarding stale
			// successful proof to business finalization after restart.
			e = ErrLifecycleUnavailable
		}
	}
	return v, e
}

type revokeKillMaintenance struct {
	MaintenanceRepository
}

type revokeLookupRepository struct {
	Repository
	MaintenanceRepository
	operationReads, checkpointReads int
}

func (r *revokeLookupRepository) ResolveCommitAmbiguity(ctx context.Context, id uuid.UUID) (Operation, error) {
	r.operationReads++
	return r.Repository.ResolveCommitAmbiguity(ctx, id)
}

func (r *revokeLookupRepository) ResolveMaintenance(ctx context.Context, id uuid.UUID) (MaintenanceCheckpoint, error) {
	r.checkpointReads++
	return r.MaintenanceRepository.ResolveMaintenance(ctx, id)
}

func (r revokeKillMaintenance) CompleteMaintenance(_ context.Context, _ MaintenanceGuard) (MaintenanceCheckpoint, error) {
	if waitProvisionFixture("revoke-kill-open") != nil {
		return MaintenanceCheckpoint{}, ErrLifecycleUnavailable
	}
	return MaintenanceCheckpoint{}, &DomainError{Code: CodeServiceUnavailable}
}

func revokePinnedCases(ctx context.Context, t *testing.T, repo *PostgresRepository, actor uuid.UUID, sql func(string, ...any), cfg DynSecAdapterConfig, ctrl ControllerClient, broker string, create func(string) (*ProvisionService, *provisionFaultRuntime)) {
	t.Helper()
	provision := func() (string, string) {
		g := "fixture_" + uuid.NewString()
		sql("INSERT INTO gateways(gateway_id,name) VALUES($1,'Revoke fixture')", g)
		s, _ := create("")
		v, e := s.Provision(ctx, actor, MutationInput{g, uuid.New(), ActionProvision})
		if e != nil || v.Secret == nil {
			t.Fatal("fixture provision", e)
		}
		return g, v.Secret.password
	}
	other, otherPassword := provision()
	g, password := provision()
	// Pre-provision all fault targets before recovery fences intentionally block
	// later OPEN. No test clears recovery rows or pretends to reconcile startup.
	faultTargets := map[string][2]string{}
	for _, fault := range []string{"finalize-unknown", "completion-unknown", "kill-open", "finalize-outage", "kill-execute", "save-fault"} {
		fg, fp := provision()
		faultTargets[fault] = [2]string{fg, fp}
	}
	s, a := create("")
	rngCalls := 0
	noRNG := func() (string, error) { rngCalls++; return "", errors.New("revoke must never generate password") }
	s.cfg.GeneratePassword = noRNG
	t.Cleanup(func() {
		if rngCalls != 0 {
			t.Errorf("revoke RNG callbacks = %d, want exactly zero", rngCalls)
		}
	})
	for _, target := range []string{"fixture_unknown", BackendUsername, "admin"} {
		if _, e := s.Revoke(ctx, actor, MutationInput{target, uuid.New(), ActionRevoke}); e == nil {
			t.Fatal("unknown/protected admitted")
		}
	}
	for _, deniedActor := range []uuid.UUID{uuid.Nil, uuid.New()} {
		if _, e := s.Revoke(ctx, deniedActor, MutationInput{g, uuid.New(), ActionRevoke}); e == nil {
			t.Fatal("invalid/nonadmin revoke admitted")
		}
	}
	unprovisioned := "fixture_" + uuid.NewString()
	sql("INSERT INTO gateways(gateway_id,name) VALUES($1,'Unprovisioned')", unprovisioned)
	if _, e := s.Revoke(ctx, actor, MutationInput{unprovisioned, uuid.New(), ActionRevoke}); e == nil {
		t.Fatal("unprovisioned revoke admitted")
	} else if d, ok := e.(*DomainError); !ok || d.Code != CodeNotFound {
		t.Fatal("unprovisioned revoke must be not_found")
	}
	ca, _ := os.ReadFile("/ca.crt")
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(ca)
	// Both original sessions must traverse the public gate, never management IPC.
	description, e := ctrl.Describe(ctx)
	if e != nil || !description.Open || len(description.Addresses) == 0 {
		t.Fatal("public gate unavailable")
	}
	_, port, e := net.SplitHostPort(description.Addresses[0])
	if e != nil {
		t.Fatal("public gate address")
	}
	dial := func(user, pass string) net.Conn {
		t.Helper()
		conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 3 * time.Second}, "tcp", net.JoinHostPort(broker, port), &tls.Config{RootCAs: roots, ServerName: "localhost", MinVersion: tls.VersionTLS12})
		if err != nil {
			t.Fatal("public gated session dial")
		}
		if conn.SetDeadline(time.Now().Add(5*time.Second)) != nil {
			t.Fatal("session deadline")
		}
		p := packets.NewControlPacket(packets.Connect).(*packets.ConnectPacket)
		p.ProtocolName, p.ProtocolVersion, p.CleanSession = "MQTT", 4, true
		p.ClientIdentifier, p.UsernameFlag, p.PasswordFlag, p.Username, p.Password = uuid.NewString(), true, true, user, []byte(pass)
		if p.Write(conn) != nil {
			t.Fatal("session CONNECT")
		}
		r, err := packets.ReadPacket(conn)
		ack, ok := r.(*packets.ConnackPacket)
		if err != nil || !ok || ack.ReturnCode != 0 {
			t.Fatal("session authentication")
		}
		return conn
	}
	otherConn := dial(other, otherPassword)
	defer func() { _ = otherConn.Close() }()
	conn := dial(g, password)
	defer func() { _ = conn.Close() }() // Revoke intentionally disconnects this peer.
	in := MutationInput{g, uuid.New(), ActionRevoke}
	v, e := s.Revoke(ctx, actor, in)
	if e != nil || v.Status != CredentialRevoked || v.CredentialVersion != 1 || v.SecretReturned {
		t.Fatal("revoke result", e)
	}
	if otherConn.SetDeadline(time.Now().Add(3*time.Second)) != nil || packets.NewControlPacket(packets.Pingreq).Write(otherConn) != nil {
		t.Fatal("unrelated original gated session write")
	}
	ping, pingErr := packets.ReadPacket(otherConn)
	if _, ok := ping.(*packets.PingrespPacket); pingErr != nil || !ok {
		t.Fatal("unrelated ORIGINAL gated socket lost")
	}
	if err := conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal("disconnect probe deadline failed")
	}
	b := make([]byte, 1)
	if _, e = conn.Read(b); e == nil {
		t.Fatal("session not disconnected")
	}
	if timeout, ok := e.(net.Error); ok && timeout.Timeout() {
		t.Fatal("target disconnect was only a timeout")
	}
	if cfg.Login(ctx, g, password) == nil {
		t.Fatal("old fixture credential accepted")
	}
	if cfg.Login(ctx, other, otherPassword) != nil {
		t.Fatal("unrelated Gateway blocked")
	}
	op, e := repo.ResolveCommitAmbiguity(ctx, v.OperationID)
	cp, x := repo.ResolveMaintenance(ctx, v.OperationID)
	if e != nil || x != nil || op.Status != OperationSucceeded || !op.Evidence.RAMApplied || !op.Evidence.SnapshotObserved || op.Evidence.FreshPositiveVerified || cp.Status != MaintenanceCompleted {
		t.Fatal("revoke persisted proof")
	}
	if _, e = s.Revoke(ctx, actor, in); e != nil {
		t.Fatal("same-key replay", e)
	}
	if _, e = s.Revoke(ctx, actor, MutationInput{g, uuid.New(), ActionRevoke}); e != nil {
		t.Fatal("current verified no-op", e)
	}
	// A real cold load must still reject the OLD known fixture password. Initial
	// reconciliation/default CLOSED remains Task 9; do not manufacture OPEN here.
	if ctrl.CloseDrain(ctx) != nil {
		t.Fatal("close")
	}
	if _, e = ctrl.RestartClosed(ctx); e != nil {
		t.Fatal("restart")
	}
	// The controller ACK precedes broker readiness. Wait on a real correlated
	// manager connection, not an arbitrary sleep or negative network probe.
	ready, e := cfg.NewClient(ctx)
	if e != nil {
		t.Fatal("cold load management readiness")
	}
	ready.Close()
	if cfg.Login(ctx, g, password) == nil {
		t.Fatal("restart accepted revoked password")
	}
	if cfg.Login(ctx, other, otherPassword) != nil {
		t.Fatal("cold load unrelated Gateway")
	}
	// Fixture-only explicit receipt opening; full startup reconciliation is not
	// implemented here. Native inconsistency below is tested while actually OPEN.
	desc, e := ctrl.Describe(ctx)
	if e != nil || ctrl.OpenVerified(ctx, VerificationReceipt{Epoch: desc.Epoch, Nonce: desc.Nonce}) != nil {
		t.Fatal("fixture explicit open")
	}
	t.Log("PASS real v13 app-role revoke RAM/snapshot/session/replay/no-op/cold-load, no password generation")
	// Response-loss seams resolve through real pool reads, never Execute again.
	for _, fault := range []string{"finalize-unknown", "completion-unknown", "kill-open", "finalize-outage", "kill-execute"} {
		target := faultTargets[fault]
		fs, runtime := create(fault)
		fs.cfg.GeneratePassword = noRNG
		lookups := &revokeLookupRepository{Repository: fs.repo, MaintenanceRepository: fs.maintenance}
		fs.repo, fs.maintenance = lookups, lookups
		wrapped := &revokeKillRuntime{ProvisionRuntime: runtime}
		fs.runtime = wrapped
		if fault == "kill-open" {
			fs.maintenance = revokeKillMaintenance{fs.maintenance}
		}
		if fault == "kill-execute" {
			wrapped.marker = "revoke-kill-execute"
		}
		in := MutationInput{target[0], uuid.New(), ActionRevoke}
		start := time.Now()
		var v MutationResult
		var err error
		if fault == "finalize-unknown" {
			entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
			wrapped.before = func() { close(entered); <-release }
			go func() { defer close(done); v, err = fs.Revoke(ctx, actor, in) }()
			<-entered // real PG intent + checkpoint already committed
			_, raceErr := fs.Rotate(ctx, actor, MutationInput{other, uuid.New(), ActionRotate})
			var d *DomainError
			if !errors.As(raceErr, &d) || d.Code != CodeRuntimeBusy {
				close(release)
				<-done
				t.Fatal("single service owner bypass")
			}
			competing, _ := create("")
			competing.cfg.GeneratePassword = noRNG
			_, raceErr = competing.Rotate(ctx, actor, MutationInput{target[0], uuid.New(), ActionRotate})
			if !errors.As(raceErr, &d) || d.Code != CodeOperationInProgress {
				close(release)
				<-done
				t.Fatal("real PG per-Gateway pending checkpoint bypass", raceErr)
			}
			close(release)
			<-done
			t.Log("PASS actual PG rotate/revoke admission race: process owner and per-Gateway checkpoint; no distributed-owner claim")
		} else {
			v, err = fs.Revoke(ctx, actor, in)
		}
		good := fault == "finalize-unknown" || fault == "completion-unknown"
		if good != (err == nil) || v.SecretReturned || wrapped.calls != 1 {
			t.Fatalf("%s classification/secret/Execute count: error=%v calls=%d", fault, err, wrapped.calls)
		}
		if fault == "finalize-unknown" && lookups.operationReads != 1 {
			t.Fatal("finalization response loss did not use exactly one fresh pooled operation lookup")
		}
		if fault == "completion-unknown" && lookups.checkpointReads != 2 {
			t.Fatal("checkpoint response loss did not resolve via pooled read")
		}
		if !good {
			var d *DomainError
			if !errors.As(err, &d) || (d.Code != CodeFinalizationPending && d.Code != CodeRecoveryRequired) {
				t.Fatal("unsafe fault response", err)
			}
		}
		m, err := repo.GetMetadata(ctx, target[0])
		if err != nil || m.CredentialVersion != 1 {
			t.Fatal("fault version", err)
		}
		op, err := repo.ResolveCommitAmbiguity(ctx, m.LastOperationID)
		cp, ce := repo.ResolveMaintenance(ctx, m.LastOperationID)
		if err != nil || ce != nil || !op.Evidence.RAMApplied || !op.Evidence.SnapshotObserved || op.Evidence.FreshPositiveVerified {
			t.Fatal("fault durable evidence")
		}
		if good || fault == "kill-open" {
			if op.Status != OperationSucceeded || op.Phase != PhaseFinalize || m.Status != CredentialRevoked {
				t.Fatal("lost ACK/finalized revoke rewritten")
			}
		} else if m.Status != CredentialRecoveryNeeded || op.Status != OperationRecoveryNeeded || op.Phase != PhaseRecovery {
			t.Fatalf("unfinalized revoke state fault=%s metadata=%s operation=%s phase=%s", fault, m.Status, op.Status, op.Phase)
		}
		wantCP := MaintenanceRecoveryNeeded
		if good {
			wantCP = MaintenanceCompleted
		}
		if cp.Status != wantCP {
			t.Fatalf("%s checkpoint=%s", fault, cp.Status)
		}
		d, err := ctrl.Describe(ctx)
		if err != nil || d.Open != good {
			t.Fatal("fault gate state")
		}
		if !good && ctrl.OpenVerified(ctx, wrapped.result.receipt) == nil {
			t.Fatal("old execution receipt reopened fault lifetime")
		}
		if cfg.Login(ctx, target[0], target[1]) == nil {
			t.Fatal("disabled native credential accepted")
		}
		// External oracle traverses the gate, never the management bypass. CLOSED
		// may reject TCP or return EOF; a successful authenticated session is banned.
		if !good {
			for _, addr := range []string{"broker:8883", "broker:8884"} {
				conn, e := net.DialTimeout("tcp", addr, time.Second)
				if e == nil {
					if err := conn.SetDeadline(time.Now().Add(time.Second)); err != nil {
						t.Fatal("gate probe deadline failed")
					}
					b := []byte{0}
					_, e = conn.Read(b)
					_ = conn.Close() // Preserve the CLOSED gate read outcome.
					if timeout, ok := e.(net.Error); ok && timeout.Timeout() {
						t.Fatal("CLOSED gate hung instead of EOF")
					}
					if e == nil {
						t.Fatal("closed external gate returned data")
					}
				}
			}
		}
		if fault == "kill-open" || fault == "kill-execute" {
			if d.Epoch == wrapped.result.receipt.Epoch {
				t.Fatal("SIGKILL restart reused broker lifetime")
			}
			// Terminal replay is history only and must not reopen a fresh controller.
			_, _ = fs.Revoke(ctx, actor, in)
			d, err = ctrl.Describe(ctx)
			if err != nil || d.Open || wrapped.calls != 1 {
				t.Fatal("restart replay executed/opened")
			}
		}
		t.Logf("PASS revoke %s real native/PG, application seam, single Execute, pooled lookups operation=%d checkpoint=%d duration=%s", fault, lookups.operationReads, lookups.checkpointReads, time.Since(start))
	}
	// DB revoked/native enabled MUST NOT receive current-state success.
	// Open only the fixture test window; recovery rows are NOT cleared.
	desc, e = ctrl.Describe(ctx)
	if e != nil || ctrl.OpenVerified(ctx, VerificationReceipt{Epoch: desc.Epoch, Nonce: desc.Nonce}) != nil {
		t.Fatal("native inconsistency fixture window")
	}
	c, e := cfg.NewClient(ctx)
	if e != nil {
		t.Fatal("native fault client")
	}
	if c.EnableClient(ctx, g) != nil {
		t.Fatal("native fault enable")
	}
	c.Close()
	beforeNoop, e := repo.GetMetadata(ctx, g)
	if e != nil {
		t.Fatal("no-op metadata read")
	}
	if _, e = s.Revoke(ctx, actor, MutationInput{g, uuid.New(), ActionRevoke}); e == nil {
		t.Fatal("inconsistent no-op succeeded")
	}
	d, e := ctrl.Describe(ctx)
	if e != nil || d.Open {
		t.Fatal("inconsistency failed open")
	}
	afterNoop, e := repo.GetMetadata(ctx, g)
	if e != nil || afterNoop.LastOperationID != beforeNoop.LastOperationID || afterNoop.CredentialVersion != beforeNoop.CredentialVersion || afterNoop.Status != CredentialRevoked {
		t.Fatal("inconsistent no-op created business intent")
	}
	oldCP, e := repo.ResolveMaintenance(ctx, afterNoop.LastOperationID)
	if e != nil || oldCP.Status != MaintenanceCompleted {
		t.Fatal("no-op rewrote historical checkpoint")
	}
	if _, e = s.Revoke(ctx, actor, MutationInput{other, uuid.New(), ActionRevoke}); e == nil {
		t.Fatal("poisoned service accepted later mutation")
	}
	c, e = cfg.NewClient(ctx)
	if e != nil || c.DisableClient(ctx, g) != nil {
		t.Fatal("fixture restore disabled")
	}
	c.Close()
	_ = a
	// Save failure: real chmod on the protected native directory, not an in-memory
	// observer stub. RAM disable is not durable snapshot success.
	faultGateway, faultPassword := faultTargets["save-fault"][0], faultTargets["save-fault"][1]
	if waitProvisionFixture("save-fault") != nil {
		t.Fatal("save fault arm")
	}
	fs, _ := create("")
	fs.cfg.GeneratePassword = noRNG
	_, e = fs.Revoke(ctx, actor, MutationInput{faultGateway, uuid.New(), ActionRevoke})
	if e == nil {
		t.Fatal("save fault success")
	}
	m, e := repo.GetMetadata(ctx, faultGateway)
	if e != nil || m.Status != CredentialRecoveryNeeded {
		t.Fatal("save fault recovery metadata")
	}
	op, e = repo.ResolveCommitAmbiguity(ctx, m.LastOperationID)
	cp, x = repo.ResolveMaintenance(ctx, m.LastOperationID)
	if e != nil || x != nil || !op.Evidence.RAMApplied || op.Evidence.SnapshotObserved || cp.Status != MaintenanceRecoveryNeeded {
		t.Fatal("RAM/snapshot fault evidence")
	}
	c, e = cfg.NewClient(ctx)
	if e != nil {
		t.Fatal("fault RAM query")
	}
	native, e := c.GetClient(ctx, faultGateway)
	c.Close()
	if e != nil || !native.Disabled || cfg.Login(ctx, faultGateway, faultPassword) == nil {
		t.Fatal("RAM disable")
	}
	snapshot, e := cfg.Observe(ctx, faultGateway)
	if e != nil || snapshot.Disabled {
		t.Fatal("old enabled snapshot observation")
	}
	d, e = ctrl.Describe(ctx)
	if e != nil || d.Open {
		t.Fatal("save failure opened")
	}
	if waitProvisionFixture("save-restore") != nil {
		t.Fatal("save restore")
	}
	t.Log("PASS real save fault RAM-only, old enabled file, retained pending recovery CLOSED")
}
