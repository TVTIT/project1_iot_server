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
)

func TestDynSecAdapterPinnedBroker(t *testing.T) {
	if os.Getenv("TASK265C_ISOLATED") != "1" {
		t.Skip("isolated native adapter fixture only")
	}
	var input struct {
		Password, NewPassword, OldPassword string
		Fault                              bool
	}
	if json.NewDecoder(os.Stdin).Decode(&input) != nil {
		t.Fatal("fixture input")
	}
	ctx := context.Background()
	ctrl := ControllerClient{ControlDir: "/control", Timeout: 5 * time.Second, MaxFrameBytes: 2048}
	ca, e := os.ReadFile("/ca.crt")
	if e != nil {
		t.Fatal("fixture CA")
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(ca)
	cfg := DynSecAdapterConfig{Controller: ctrl, ProtectedUsernames: []string{"admin", BackendUsername}, Timeout: 20 * time.Second, RecoveryTimeout: 5 * time.Second}
	cfg.Rejected = func(ctx context.Context, u, p string) error {
		return integrationRejected(ctx, ctrl, roots, u, p)
	}
	cfg.NewClient = func(ctx context.Context) (*DynSecClient, error) {
		for attempt := 0; attempt < 20; attempt++ {
			c, e := NewDynSecClient(ctx, DynSecConfig{BrokerURL: "ssl://localhost:18884", CAPEM: ca, ManagerUsername: "admin", ManagerPassword: input.Password, ProtectedUsernames: []string{BackendUsername}, Timeout: 3 * time.Second, MaxPayloadBytes: 16384, MaxInflight: 4, QueueSize: 8, ValidateTarget: func(u string) bool { return u == "A" || u == "C" || u == "D" }, ValidateRole: func(u, r string) bool { return r == "gateway_"+u }, ManagementDial: ctrl.ManagementDial})
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
	reader, e := NewDynSecSnapshotReader(DynSecSnapshotConfig{Path: "/security/dynsec.json", WriterUID: 1883, MaxBytes: 1 << 20, ValidateTarget: func(u string) bool { return u == "A" || u == "C" }})
	if e != nil {
		t.Fatal(e)
	}
	waitFault := func(name string) error {
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
	revokeSnapshotFault := false
	cfg.Observe = func(ctx context.Context, u string) (DynSecClientMetadata, error) {
		if revokeSnapshotFault && waitFault("revoke-snapshot") != nil {
			return DynSecClientMetadata{}, ErrVerificationFailed
		}
		v, e := reader.Observe(ctx, u)
		if e != nil {
			t.Log("snapshot observation unavailable")
		}
		return v, e
	}
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
		packet.ProtocolName = "MQTT"
		packet.ProtocolVersion = 4
		packet.CleanSession = true
		packet.ClientIdentifier = "adapter-" + uuid.NewString()
		packet.UsernameFlag = true
		packet.PasswordFlag = true
		packet.Username = u
		packet.Password = []byte(p)
		if packet.Write(conn) != nil {
			return ErrVerificationFailed
		}
		reply, e := packets.ReadPacket(conn)
		if e != nil {
			return ErrVerificationFailed
		}
		ack, ok := reply.(*packets.ConnackPacket)
		if !ok || ack.ReturnCode != 0 {
			return ErrVerificationFailed
		}
		return nil
	}
	a, e := NewDynSecAdapter(cfg)
	if e != nil {
		t.Fatal(e)
	}
	op := Operation{OperationID: uuid.New(), ActorUserID: uuid.New(), IdempotencyKey: uuid.New(), GatewayID: "C", Action: ActionProvision, CredentialVersion: 1, Status: OperationPending, Phase: PhaseIntent, DeliveryStatus: DeliveryUnknown}
	partial := op
	partial.GatewayID, partial.OperationID = "D", uuid.New()
	partialResult, e := a.Execute(ctx, partial, func(ctx context.Context) (string, error) {
		// Actual native createClient conflict AFTER preflight: role creation still
		// succeeds, then native createClient rejects. No fake response injection.
		c, e := cfg.NewClient(ctx)
		if e != nil {
			return "", e
		}
		defer c.Close()
		if e = c.CreateClient(ctx, "D"); e != nil {
			return "", e
		}
		return "partial-role-fixture-password", nil
	})
	if e == nil || partialResult.Outcome != ExecutionRecoveryRequired || partialResult.Evidence.RAMApplied {
		t.Fatal("partial role failure false success/unchanged")
	}
	c, e := cfg.NewClient(ctx)
	if e != nil {
		t.Fatal("partial role oracle client")
	}
	native, e := c.GetClient(ctx, "D")
	_, roleErr := c.adapterRequest(ctx, "getRole", map[string]any{"rolename": "gateway_D"})
	c.Close()
	if e != nil || !native.Disabled || roleErr != nil {
		t.Fatal("native partial role recovery oracle")
	}
	if a.ReleaseClosed(ctx, partial.OperationID) != nil {
		t.Fatal("partial role maintenance release")
	}
	t.Log("PASS actual role created then native createClient failure; recovery disabled CLOSED")
	unrelated := op
	unrelated.GatewayID = "A"
	unrelated.OperationID = uuid.New()
	unrelatedResult, e := a.Execute(ctx, unrelated, func(context.Context) (string, error) { return "unrelated-fixture-oracle-secret", nil })
	if e != nil || a.OpenAfterFinalization(ctx, unrelatedResult, unrelated.OperationID) != nil {
		t.Fatal("unrelated fixture provisioning")
	}
	r, e := a.Execute(ctx, op, func(context.Context) (string, error) { return input.NewPassword, nil })
	if e != nil || r.Outcome != ExecutionVerifiedSuccess {
		t.Fatal("native provision verification failed", e)
	}
	d, e := ctrl.Describe(ctx)
	if e != nil || d.Open {
		t.Fatal("adapter prematurely opened")
	}
	// Explicit trusted DB seam: NOT a real service/DB finalization test.
	if a.OpenAfterFinalization(ctx, r, op.OperationID) != nil {
		t.Fatal("trusted fixture acknowledgment")
	}
	// Test the role actually created by the adapter, not a parallel ACL fixture.
	oracleClient, e := cfg.NewClient(ctx)
	if e != nil {
		t.Fatal("ACL publisher oracle")
	}
	_, e = oracleClient.adapterRequest(ctx, "createRole", map[string]any{"rolename": "fixture_publisher", "acls": []adapterACL{{"publishClientSend", "gateways/#", 1, true}}})
	if e == nil {
		// Updating this oracle principal disconnects its current session in 2.0.18.
		_, _ = oracleClient.adapterRequest(ctx, "addClientRole", map[string]any{"username": "admin", "rolename": "fixture_publisher"})
	}
	oracleClient.Close()
	if e != nil {
		t.Fatal("test-only publisher grant", e)
	}
	connect := func(u, password string) net.Conn {
		conn, err := ctrl.DialManagementTLS(ctx, &tls.Config{RootCAs: roots, ServerName: "localhost", MinVersion: tls.VersionTLS12})
		if err != nil {
			t.Fatal("ACL oracle TLS")
		}
		if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
			t.Fatal("ACL oracle deadline failed")
		}
		p := packets.NewControlPacket(packets.Connect).(*packets.ConnectPacket)
		p.ProtocolName, p.ProtocolVersion, p.CleanSession = "MQTT", 4, true
		p.ClientIdentifier, p.UsernameFlag, p.PasswordFlag, p.Username, p.Password = uuid.NewString(), true, true, u, []byte(password)
		if p.Write(conn) != nil {
			t.Fatal("ACL oracle CONNECT")
		}
		v, err := packets.ReadPacket(conn)
		ack, ok := v.(*packets.ConnackPacket)
		if err != nil || !ok || ack.ReturnCode != 0 {
			t.Fatal("ACL oracle authentication")
		}
		return conn
	}
	admin := connect("admin", input.Password)
	defer func() { _ = admin.Close() }() // Close the latest oracle after reconnects.
	gateway := connect("C", input.NewPassword)
	defer func() { _ = gateway.Close() }() // Close the latest oracle after reconnects.
	subscribe := func(conn net.Conn, topic string, allowed bool) {
		if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
			t.Fatal("subscribe deadline failed")
		}
		p := packets.NewControlPacket(packets.Subscribe).(*packets.SubscribePacket)
		p.MessageID, p.Topics, p.Qoss = 1, []string{topic}, []byte{0}
		if p.Write(conn) != nil {
			t.Fatal("ACL subscribe write")
		}
		v, err := packets.ReadPacket(conn)
		ack, ok := v.(*packets.SubackPacket)
		if err != nil || !ok || len(ack.ReturnCodes) != 1 || (ack.ReturnCodes[0] != 0x80) != allowed {
			t.Fatal("ACL subscribe policy", topic)
		}
	}
	delivery := func(sender, receiver net.Conn, topic string, allowed bool) {
		if err := sender.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
			t.Fatal("publish deadline failed")
		}
		p := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
		p.TopicName, p.Payload = topic, []byte("nonsecret-acl-oracle")
		if p.Write(sender) != nil {
			t.Fatal("ACL publish write")
		}
		wait := 250 * time.Millisecond
		if allowed {
			wait = 3 * time.Second
		}
		if err := receiver.SetDeadline(time.Now().Add(wait)); err != nil {
			t.Fatal("delivery deadline failed")
		}
		v, err := packets.ReadPacket(receiver)
		if allowed {
			got, ok := v.(*packets.PublishPacket)
			if err != nil || !ok || got.TopicName != topic || string(got.Payload) != string(p.Payload) {
				t.Fatalf("ACL allowed delivery %s packet=%T error=%T", topic, v, err)
			}
		} else if timeout, ok := err.(net.Error); !ok || !timeout.Timeout() {
			t.Fatal("ACL denied delivery", topic)
		}
	}
	subscribe(admin, "gateways/#", true)
	for _, suffix := range []string{"telemetry/sample", "status", "responses/result"} {
		delivery(gateway, admin, "gateways/C/"+suffix, true)
	}
	for _, topic := range []string{"gateways/A/telemetry/sample", "gateways/C/commands/action", "gateways/C/acks/result"} {
		delivery(gateway, admin, topic, false)
		// A TLS read timeout poisons that connection; never reuse the oracle.
		_ = admin.Close() // Discard the timeout-poisoned TLS oracle.
		admin = connect("admin", input.Password)
		subscribe(admin, "gateways/#", true)
	}
	for _, topic := range []string{"gateways/A/commands/#", "gateways/A/acks/#", "#", "gateways/C/telemetry/#"} {
		subscribe(gateway, topic, false)
	}
	for _, suffix := range []string{"commands", "acks"} {
		subscribe(gateway, "gateways/C/"+suffix+"/#", true)
		delivery(admin, gateway, "gateways/C/"+suffix+"/result", true)
		delivery(admin, gateway, "gateways/A/"+suffix+"/result", false)
		// Drain the admin's own echo from its broad test subscription.
		if err := admin.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
			t.Fatal("echo deadline failed")
		}
		if _, err := packets.ReadPacket(admin); err != nil {
			t.Fatal("admin oracle echo")
		}
		if _, err := packets.ReadPacket(admin); err != nil {
			t.Fatal("admin oracle echo")
		}
		_ = gateway.Close() // Discard the timeout-poisoned TLS oracle.
		gateway = connect("C", input.NewPassword)
	}
	_ = admin.Close() // Best-effort teardown; ACL assertions are complete.
	_ = gateway.Close()
	t.Log("PASS actual adapter role send/receive/subscribe own namespace; cross-Gateway and broad subscribe denied")
	op.Previous = &CredentialSnapshot{Status: CredentialActive, CredentialVersion: 1}
	op.CredentialVersion = 2
	op.Action = ActionRotate
	op.OperationID = uuid.New()
	old := input.NewPassword
	input.NewPassword = input.OldPassword
	r, e = a.Execute(ctx, op, func(context.Context) (string, error) { return input.NewPassword, nil })
	if e != nil || !r.Evidence.FreshPositiveVerified || cfg.Login(ctx, "C", old) == nil {
		t.Fatal("native rotate old/new oracle")
	}
	if a.OpenAfterFinalization(ctx, r, op.OperationID) != nil {
		t.Fatal("rotate acknowledgment")
	}
	op.Action = ActionRevoke
	op.Previous.CredentialVersion = 2
	op.OperationID = uuid.New()
	revokeSnapshotFault = true
	r, e = a.Execute(ctx, op, nil)
	if e == nil || r.Outcome != ExecutionRecoveryRequired || !r.Evidence.RAMApplied || r.Evidence.SnapshotObserved {
		t.Fatal("revoke unreadable native snapshot false success")
	}
	if d, e := ctrl.Describe(ctx); e != nil || d.Open || cfg.Login(ctx, "C", input.NewPassword) == nil {
		t.Fatal("revoke snapshot fault must stay disabled CLOSED")
	}
	if a.ReleaseClosed(ctx, op.OperationID) != nil {
		t.Fatal("revoke snapshot release")
	}
	revokeSnapshotFault = false
	if waitFault("revoke-restore") != nil {
		t.Fatal("snapshot fixture restore")
	}
	t.Log("PASS actual revoke snapshot permission failure; RAM disabled, recovery CLOSED")
	op.Action = ActionProvision
	op.Previous.Status = CredentialRevoked
	op.CredentialVersion = 3
	op.OperationID = uuid.New()
	r, e = a.Execute(ctx, op, func(context.Context) (string, error) { return input.NewPassword, nil })
	if e != nil || a.OpenAfterFinalization(ctx, r, op.OperationID) != nil {
		t.Fatal("trusted fixture recovery reprovision")
	}
	op.Action = ActionRevoke
	op.Previous.Status, op.Previous.CredentialVersion = CredentialActive, 3
	op.OperationID = uuid.New()
	r, e = a.Execute(ctx, op, nil)
	if e != nil || !r.Evidence.SnapshotObserved || cfg.Login(ctx, "C", input.NewPassword) == nil {
		t.Fatal("native revoke")
	}
	if a.OpenAfterFinalization(ctx, r, op.OperationID) != nil {
		t.Fatal("revoke acknowledgment")
	}
	if cfg.Login(ctx, "A", "unrelated-fixture-oracle-secret") != nil {
		t.Fatal("unrelated native principal changed")
	}
	// Actual native save fault: harness removes writer directory permission
	// after CLOSED admission, then cold reload must reject the new password.
	op.Action = ActionProvision
	op.CredentialVersion = 4
	op.Previous.Status = CredentialRevoked
	op.OperationID = uuid.New()
	r, e = a.Execute(ctx, op, func(context.Context) (string, error) {
		if os.WriteFile("/artifacts/fault-ready", []byte("ready"), 0600) != nil {
			return "", ErrVerificationFailed
		}
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			if _, e := os.Stat("/artifacts/fault-armed"); e == nil {
				return "native-save-fault-fixture-secret", nil
			}
			time.Sleep(25 * time.Millisecond)
		}
		return "", ErrVerificationFailed
	})
	if e == nil || r.Outcome != ExecutionRecoveryRequired || !r.Evidence.RAMApplied || r.Evidence.FreshPositiveVerified {
		t.Fatal("native save fault false success")
	}
	d, e = ctrl.Describe(ctx)
	if e != nil || d.Open || cfg.Login(ctx, "C", "native-save-fault-fixture-secret") == nil {
		t.Fatal("cold save fault must reject NEW and stay CLOSED")
	}
	t.Log("PASS native provision/rotate/revoke; CLOSED before explicit trusted finalization seam; fresh old/new oracle")
	t.Log("PASS actual native save fault RAM applied then cold login rejects NEW; recovery CLOSED, no secret result")
	if a.ReleaseClosed(ctx, op.OperationID) != nil {
		t.Fatal("save fault release")
	}
	op.OperationID = uuid.New()
	if waitFault("controller-disconnect") != nil {
		t.Fatal("controller disconnect fixture")
	}
	generated := false
	r, e = a.Execute(ctx, op, func(context.Context) (string, error) { generated = true; return "must-not-generate", nil })
	if e == nil || generated || r.Outcome != ExecutionMaintenanceClosed || a.OpenAfterFinalization(ctx, r, op.OperationID) == nil {
		t.Fatal("disconnected controller allowed mutation or OPEN")
	}
	t.Log("PASS actual controller disconnect rejects before generation; no OPEN")
}
