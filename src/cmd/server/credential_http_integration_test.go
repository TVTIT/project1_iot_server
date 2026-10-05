package main

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"iot-platform/internal/auth"
	"iot-platform/internal/config"
	"iot-platform/internal/gateway"
	"iot-platform/internal/httpserver"
	"iot-platform/internal/mqttcredential"
)

type httpFixtureReadiness bool

func (r httpFixtureReadiness) Ready() bool { return bool(r) }

// Runs the production composition and router behind an owned TLS Nginx. Test
// state changes below are boundary seams, not deployment or SQL wire-loss proof.
func TestCredentialHTTPIsolated(t *testing.T) {
	if os.Getenv("TASK2612_ISOLATED") != "1" {
		t.Skip("owned HTTP PostgreSQL broker fixture required")
	}
	var input struct{ Password, BackendPassword, AppDSN, AdminDSN, Proxy, Broker, JWTSecret string }
	if json.NewDecoder(os.Stdin).Decode(&input) != nil {
		t.Fatal("fixture input")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()
	app, err := pgxpool.New(ctx, input.AppDSN)
	if err != nil {
		t.Fatal("app pool")
	}
	defer app.Close()
	admin, err := pgxpool.New(ctx, input.AdminDSN)
	if err != nil {
		t.Fatal("admin pool")
	}
	defer admin.Close()
	sql := func(q string, args ...any) {
		t.Helper()
		if _, e := admin.Exec(ctx, q, args...); e != nil {
			t.Fatal("fixture SQL failed")
		}
	}
	actor := uuid.New()
	id := "fixture_" + uuid.New().String()[:8]
	other := "fixture_" + uuid.New().String()[:8]
	sql("INSERT INTO profiles(id) VALUES($1)", actor)
	sql("INSERT INTO platform_admins(user_id) VALUES($1)", actor)
	cfg := config.CredentialConfig{Enabled: true, ManagerUsername: "admin", ManagerPassword: input.Password, BackendPassword: input.BackendPassword, ManagementURL: "ssl://localhost:18884", CAFile: "/ca.crt", SnapshotPath: "/security/dynsec.json", ControlDir: "/control", DBTimeout: 2 * time.Second, FinalizeTimeout: 5 * time.Second, ReconcileTimeout: 30 * time.Second, OperationTimeout: 10 * time.Second, RequestTimeout: 40 * time.Second, RecoveryTimeout: 5 * time.Second, ClientTimeout: 3 * time.Second, BatchSize: 64, MaxPages: 16, QueueSize: 16, MaxInflight: 1, MaxPayloadBytes: 65536, MaxSnapshotBytes: 1 << 20}
	ctrl := mqttcredential.ControllerClient{ControlDir: "/control", Timeout: 3 * time.Second, MaxFrameBytes: 4096}
	d, e := ctrl.Describe(ctx)
	if e != nil || d.Open {
		t.Fatal("initial CLOSED required")
	}
	// Private broker listener is loopback inside its own container. A peer on
	// the isolated bridge must not be able to bypass the gated TLS listener.
	for _, port := range []string{"18884", "1883"} {
		conn, err := net.DialTimeout("tcp", input.Broker+":"+port, time.Second)
		if err == nil {
			_ = conn.Close()
			t.Fatal("fixture bridge bypass listener reachable")
		}
	}
	composition, e := composeCredentials(ctx, app, cfg)
	if e != nil {
		t.Fatal("real composition failed", e)
	}
	defer func() {
		if composition.close() != nil {
			t.Error("shutdown CLOSED unconfirmed")
		}
	}()
	for _, g := range []string{id, other} {
		sql("INSERT INTO gateways(gateway_id,name) VALUES($1,'isolated')", g)
	}
	checker, _ := auth.NewPostgresPlatformAdminChecker(app)
	verifier, _ := auth.NewHS256Verifier(auth.VerifierConfig{Secret: input.JWTSecret, Issuer: "https://fixture.invalid/auth/v1", Audience: "authenticated"})
	gr, _ := gateway.NewPostgresRepository(app)
	gs, _ := gateway.NewService(gr, 2*time.Second)
	pr, _ := gateway.NewPostgresProvisioningRepository(app)
	ps, _ := gateway.NewProvisioningService(pr, 2*time.Second)
	deps := httpserver.RouterDependencies{ReadinessChecker: app, ReadinessTimeout: time.Second, AuthorizationTimeout: time.Second, TokenVerifier: verifier, PlatformAdminChecker: checker, GatewayReader: gs, SensorReader: gs, GatewayProvisioner: ps, SensorProvisioner: ps, AdminMaxBodyBytes: 128, CredentialAPIEnabled: true, CredentialRequestTimeout: cfg.RequestTimeout, CredentialManager: composition.manager, CredentialStartupReadiness: composition.ready}
	var mu sync.RWMutex
	var handler http.Handler
	install := func() {
		t.Helper()
		h, e := httpserver.NewRouter(deps)
		if e != nil {
			t.Fatal("router")
		}
		mu.Lock()
		handler = h
		mu.Unlock()
	}
	install()
	listener, e := net.Listen("tcp", ":8080")
	if e != nil {
		t.Fatal("HTTP listener")
	}
	server := &http.Server{ReadHeaderTimeout: time.Second, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.RLock()
		h := handler
		mu.RUnlock()
		h.ServeHTTP(w, r)
	})}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	defer func() {
		if server.Close() != nil || <-done != http.ErrServerClosed {
			t.Error("HTTP cleanup")
		}
	}()
	ca, _ := os.ReadFile("/ca.crt")
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		t.Fatal("CA")
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, ServerName: "localhost", MinVersion: tls.VersionTLS12}}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 45 * time.Second}
	token := func(user uuid.UUID, role string) string {
		t.Helper()
		now := time.Now()
		s, e := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"sub": user.String(), "iss": "https://fixture.invalid/auth/v1", "aud": "authenticated", "iat": now.Unix(), "exp": now.Add(time.Minute).Unix(), "role": role, "is_admin": true}).SignedString([]byte(input.JWTSecret))
		if e != nil {
			t.Fatal("JWT signing")
		}
		return s
	}
	adminToken := token(actor, "authenticated")
	base := "/v1/admin/gateways/" + id + "/mqtt-credential"
	request := func(method, path, bearer, key, body string, want int) map[string]any {
		t.Helper()
		r, e := http.NewRequestWithContext(ctx, method, "https://"+input.Proxy+":8443"+path, strings.NewReader(body))
		if e != nil {
			t.Fatal("request")
		}
		if bearer != "" {
			r.Header.Set("Authorization", "Bearer "+bearer)
		}
		if key != "" {
			r.Header.Set("Idempotency-Key", key)
		}
		response, e := client.Do(r)
		if e != nil {
			t.Fatal("HTTPS transport failed", e)
		}
		defer func() { _ = response.Body.Close() }()
		if response.Header.Get("Cache-Control") != "no-store" || response.Header.Get("Pragma") != "no-cache" {
			t.Fatal("missing no-store")
		}
		var result map[string]any
		if json.NewDecoder(io.LimitReader(response.Body, 65536)).Decode(&result) != nil {
			t.Fatal("response JSON")
		}
		if response.StatusCode != want {
			var status, phase, code string
			_ = admin.QueryRow(ctx, "SELECT status,phase,coalesce(error_code,'') FROM gateway_mqtt_credential_events ORDER BY created_at DESC LIMIT 1").Scan(&status, &phase, &code)
			t.Log("safe operation diagnosis", status, phase, code)
			t.Fatalf("%s expected %d got %d (body redacted)", method, want, response.StatusCode)
		}
		return result
	}
	count := func() int {
		t.Helper()
		var n int
		if admin.QueryRow(ctx, "SELECT count(*) FROM gateway_mqtt_credential_events").Scan(&n) != nil {
			t.Fatal("audit count")
		}
		return n
	}
	routes := []struct{ method, path string }{{"GET", base}, {"POST", base}, {"POST", base + "/rotate"}, {"DELETE", base}}
	t.Run("all-route-authorization-no-effects", func(t *testing.T) {
		before := count()
		for _, role := range []string{"owner", "operator", "viewer", "non-member"} {
			user := uuid.New()
			sql("INSERT INTO profiles(id) VALUES($1)", user)
			if role != "non-member" {
				sql("INSERT INTO user_gateways(user_id,gateway_id,role) VALUES($1,$2,$3)", user, id, role)
			}
			for _, route := range routes {
				request(route.method, route.path, token(user, "authenticated"), uuid.NewString(), "", 403)
			}
		}
		for _, route := range routes {
			for _, bearer := range []string{"", "invalid", token(actor, "service_role")} {
				request(route.method, route.path, bearer, uuid.NewString(), "", 401)
			}
		}
		if count() != before {
			t.Fatal("denied mutation")
		}
	})
	t.Run("parsing-disabled-unready-checker", func(t *testing.T) {
		before := count()
		for _, route := range routes {
			request(route.method, route.path, adminToken, uuid.NewString(), "{}", 400)
			request(route.method, route.path, adminToken, uuid.NewString(), strings.Repeat("x", 129), 413)
		}
		request("POST", base, adminToken, "", "", 400)
		request("POST", base, adminToken, "invalid", "", 400)
		request("POST", "/v1/admin/gateways/backend_service/mqtt-credential", adminToken, uuid.NewString(), "", 400)
		request("POST", "/v1/admin/gateways/fixture_missing/mqtt-credential", adminToken, uuid.NewString(), "", 404)
		for _, route := range routes {
			deps.CredentialAPIEnabled = false
			install()
			request(route.method, route.path, adminToken, uuid.NewString(), "", 503)
			deps.CredentialAPIEnabled = true
			deps.CredentialStartupReadiness = httpFixtureReadiness(false)
			install()
			request(route.method, route.path, adminToken, uuid.NewString(), "", 503)
		}
		deps.CredentialStartupReadiness = composition.ready
		install()
		// A real SQL authorization error; no database-loss policy is inferred.
		sql("REVOKE SELECT ON platform_admins FROM iot_backend")
		for _, route := range routes {
			request(route.method, route.path, adminToken, uuid.NewString(), "", 503)
		}
		sql("GRANT SELECT ON platform_admins TO iot_backend")
		if count() != before {
			t.Fatal("invalid/disabled/unready/checker mutation")
		}
	})
	// The certificate names localhost while Docker DNS routes to the owned broker.
	// The TLS server identity is still verified, never insecure TLS.
	connect := func(user, password string) mqtt.Client {
		t.Helper()
		opts := mqtt.NewClientOptions().AddBroker("ssl://" + input.Broker + ":8883").SetClientID(uuid.NewString()).SetUsername(user).SetPassword(password).SetAutoReconnect(false).SetConnectTimeout(2 * time.Second).SetTLSConfig(&tls.Config{RootCAs: roots, ServerName: "localhost", MinVersion: tls.VersionTLS12})
		opts.SetCustomOpenConnectionFn(func(u *url.URL, _ mqtt.ClientOptions) (net.Conn, error) {
			dialer := tls.Dialer{NetDialer: &net.Dialer{Timeout: 2 * time.Second}, Config: &tls.Config{RootCAs: roots, ServerName: "localhost", MinVersion: tls.VersionTLS12}}
			return dialer.DialContext(ctx, "tcp", u.Host)
		})
		c := mqtt.NewClient(opts)
		tok := c.Connect()
		if !tok.WaitTimeout(3*time.Second) || tok.Error() != nil {
			t.Fatal("fresh gated TLS login")
		}
		return c
	}
	rejected := func(user, password string) {
		t.Helper()
		dialer := tls.Dialer{NetDialer: &net.Dialer{Timeout: 2 * time.Second}, Config: &tls.Config{RootCAs: roots, ServerName: "localhost", MinVersion: tls.VersionTLS12}}
		conn, err := dialer.DialContext(ctx, "tcp", input.Broker+":8883")
		if err != nil {
			t.Fatal("transport failure is not authentication rejection")
		}
		defer func() { _ = conn.Close() }()
		if conn.SetDeadline(time.Now().Add(2*time.Second)) != nil {
			t.Fatal("negative probe deadline")
		}
		packet := packets.NewControlPacket(packets.Connect).(*packets.ConnectPacket)
		packet.ProtocolName, packet.ProtocolVersion, packet.CleanSession = "MQTT", 4, true
		packet.ClientIdentifier, packet.UsernameFlag, packet.PasswordFlag = uuid.NewString(), true, true
		packet.Username, packet.Password = user, []byte(password)
		defer clear(packet.Password)
		if packet.Write(conn) != nil {
			t.Fatal("negative CONNECT transport")
		}
		reply, err := packets.ReadPacket(conn)
		ack, ok := reply.(*packets.ConnackPacket)
		if err != nil || !ok || (ack.ReturnCode != packets.ErrRefusedBadUsernameOrPassword && ack.ReturnCode != packets.ErrRefusedNotAuthorised) {
			t.Fatal("explicit authentication-denied CONNACK required")
		}
	}
	t.Run("TLS-failure-is-not-authentication-rejection", func(t *testing.T) {
		dialer := tls.Dialer{NetDialer: &net.Dialer{Timeout: time.Second}, Config: &tls.Config{RootCAs: roots, ServerName: "mismatch.invalid", MinVersion: tls.VersionTLS12}}
		conn, err := dialer.DialContext(ctx, "tcp", input.Broker+":8883")
		if err == nil {
			_ = conn.Close()
			t.Fatal("hostname mismatch accepted")
		}
	})
	key := uuid.NewString()
	first := request("POST", base, adminToken, key, "", 201)
	password, ok := first["password"].(string)
	if !ok || len(password) != 43 || first["secret_returned"] != true {
		t.Fatal("one-time CSPRNG response")
	}
	secrets := []string{password}
	before := count()
	replay := request("POST", base, adminToken, key, "", 201)
	if replay["password"] != nil || replay["secret_returned"] != false || count() != before {
		t.Fatal("replay repeated secret or mutation")
	}
	request("GET", base, adminToken, "", "", 200)
	request("POST", base, adminToken, uuid.NewString(), "", 409)
	request("POST", base+"/rotate", adminToken, key, "", 409)
	otherBase := "/v1/admin/gateways/" + other + "/mqtt-credential"
	b := request("POST", otherBase, adminToken, uuid.NewString(), "", 201)
	bPassword, ok := b["password"].(string)
	if !ok {
		t.Fatal("Gateway B provision")
	}
	secrets = append(secrets, bPassword)
	t.Run("literal-ACL-isolation", func(t *testing.T) {
		c := connect(id, password)
		defer c.Disconnect(0)
		for _, topic := range []string{"gateways/" + other + "/acks/#", "$CONTROL/dynamic-security/v1/response"} {
			token := c.Subscribe(topic, 0, func(_ mqtt.Client, _ mqtt.Message) {})
			if !token.WaitTimeout(2 * time.Second) {
				t.Fatal("bounded SUBACK")
			}
			codes := token.(*mqtt.SubscribeToken).Result()
			if codes[topic] != 0x80 {
				t.Fatal("cross-Gateway/control subscription allowed")
			}
		}
		token := c.Subscribe("gateways/"+id+"/acks/#", 0, func(_ mqtt.Client, _ mqtt.Message) {})
		if !token.WaitTimeout(2*time.Second) || token.Error() != nil || token.(*mqtt.SubscribeToken).Result()["gateways/"+id+"/acks/#"] == 0x80 {
			t.Fatal("own ACL denied")
		}
	})
	session := connect(id, password)
	defer session.Disconnect(0)
	second := request("POST", base+"/rotate", adminToken, uuid.NewString(), "", 200)
	newPassword, ok := second["password"].(string)
	if !ok || newPassword == password || second["credential_version"] != float64(2) {
		t.Fatal("rotate generation")
	}
	secrets = append(secrets, newPassword)
	rejected(id, password)
	bFresh := connect(other, bPassword)
	bFresh.Disconnect(0)
	deadline := time.Now().Add(3 * time.Second)
	for session.IsConnectionOpen() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if session.IsConnectionOpen() {
		t.Fatal("maintenance session not drained")
	}
	newSession := connect(id, newPassword)
	defer newSession.Disconnect(0)
	revoked := request("DELETE", base, adminToken, uuid.NewString(), "", 200)
	if revoked["status"] != "revoked" {
		t.Fatal("revoke metadata")
	}
	deadline = time.Now().Add(3 * time.Second)
	for newSession.IsConnectionOpen() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if newSession.IsConnectionOpen() {
		t.Fatal("revoke session not disconnected")
	}
	rejected(id, newPassword)
	bFresh = connect(other, bPassword)
	bFresh.Disconnect(0)
	// Lost-response model: discard secret at caller, replay metadata only, then
	// explicitly reprovision a revoked principal with a fresh generation.
	thirdKey := uuid.NewString()
	third := request("POST", base, adminToken, thirdKey, "", 201)
	thirdPassword, ok := third["password"].(string)
	if !ok || third["credential_version"] != float64(3) {
		t.Fatal("reprovision generation")
	}
	secrets = append(secrets, thirdPassword)
	before = count()
	lostReplay := request("POST", base, adminToken, thirdKey, "", 201)
	if lostReplay["password"] != nil || lostReplay["secret_returned"] != false || count() != before {
		t.Fatal("lost response replay must not redeliver")
	}
	fourth := request("POST", base+"/rotate", adminToken, uuid.NewString(), "", 200)
	fourthPassword, ok := fourth["password"].(string)
	if !ok || fourth["credential_version"] != float64(4) {
		t.Fatal("lost response explicit re-rotate")
	}
	secrets = append(secrets, fourthPassword)
	rejected(id, thirdPassword)
	fresh := connect(id, fourthPassword)
	fresh.Disconnect(0)
	var audit int
	if admin.QueryRow(ctx, "SELECT count(*) FROM gateway_mqtt_credential_events WHERE gateway_id=$1 AND status='succeeded' AND snapshot_observed AND delivery_status='unknown'", id).Scan(&audit) != nil || audit != 5 {
		t.Fatal("generation/audit consistency")
	}
	rows, e := admin.Query(ctx, "SELECT row_to_json(e)::text FROM gateway_mqtt_credential_events e UNION ALL SELECT row_to_json(m)::text FROM gateway_mqtt_credentials m")
	if e != nil {
		t.Fatal("DB secrecy readback")
	}
	defer rows.Close()
	for rows.Next() {
		var text string
		if rows.Scan(&text) != nil {
			t.Fatal("DB readback")
		}
		for _, secret := range secrets {
			if strings.Contains(text, secret) {
				t.Fatal("secret persisted")
			}
		}
	}
	if rows.Err() != nil {
		t.Fatal("DB rows")
	}
	// Only fingerprints leave this process for the harness's log secrecy scan.
	// Responses and plaintext Gateway credentials never become artifacts.
	var fingerprints []string
	for _, secret := range secrets {
		digest := sha256.Sum256([]byte(secret))
		fingerprints = append(fingerprints, hex.EncodeToString(digest[:]))
	}
	report, err := json.Marshal(fingerprints)
	if err != nil || os.WriteFile("/artifacts/secret-fingerprints.json", report, 0o644) != nil {
		t.Fatal("redacted secrecy report")
	}
	d, e = ctrl.Describe(ctx)
	if e != nil || !d.Open {
		t.Fatal("verified OPEN after finalization")
	}
	t.Run("HTTP-finalization-SQL-error-recovery-reprovision", func(t *testing.T) {
		faultID := "fixture_" + uuid.New().String()[:8]
		faultBase := "/v1/admin/gateways/" + faultID + "/mqtt-credential"
		sql("INSERT INTO gateways(gateway_id,name) VALUES($1,'finalization fault fixture')", faultID)
		// An actual server-side SQL finalization error, NOT SQL wire/commit-ACK
		// loss. Intent and broker mutation use the unchanged production path.
		sql(`CREATE FUNCTION fixture_reject_active() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.status='active' THEN RAISE EXCEPTION 'isolated finalization fault'; END IF; RETURN NEW; END $$`)
		sql(`CREATE TRIGGER fixture_reject_active BEFORE UPDATE ON gateway_mqtt_credentials FOR EACH ROW EXECUTE FUNCTION fixture_reject_active()`)
		result := request("POST", faultBase, adminToken, uuid.NewString(), "", 503)
		sql(`DROP TRIGGER fixture_reject_active ON gateway_mqtt_credentials`)
		sql(`DROP FUNCTION fixture_reject_active()`)
		if result["password"] != nil {
			t.Fatal("finalization fault returned secret")
		}
		var unresolved int
		if admin.QueryRow(ctx, "SELECT count(*) FROM gateway_mqtt_credential_events WHERE gateway_id=$1 AND status IN ('pending','recovery_needed')", faultID).Scan(&unresolved) != nil || unresolved != 1 {
			t.Fatal("finalization fault lost unresolved intent")
		}
		d, err := ctrl.Describe(ctx)
		if err != nil || d.Open {
			t.Fatal("finalization fault must remain CLOSED")
		}
		if composition.close() != nil {
			t.Fatal("pre-recovery close")
		}
		composition, err = composeCredentials(ctx, app, cfg)
		if err != nil {
			t.Fatal("approved startup recovery failed")
		}
		deps.CredentialManager, deps.CredentialStartupReadiness = composition.manager, composition.ready
		install()
		metadata := request("GET", faultBase, adminToken, "", "", 200)
		if metadata["status"] != "revoked" {
			t.Fatal("recovery must establish revoked disposition")
		}
		reprovision := request("POST", faultBase, adminToken, uuid.NewString(), "", 201)
		newSecret, ok := reprovision["password"].(string)
		if !ok || reprovision["credential_version"] != float64(2) {
			t.Fatal("recovery deliberate fresh generation")
		}
		secrets = append(secrets, newSecret)
		digest := sha256.Sum256([]byte(newSecret))
		fingerprints = append(fingerprints, hex.EncodeToString(digest[:]))
		report, err := json.Marshal(fingerprints)
		if err != nil || os.WriteFile("/artifacts/secret-fingerprints.json", report, 0o644) != nil {
			t.Fatal("recovery redacted secrecy report")
		}
		fresh := connect(faultID, newSecret)
		fresh.Disconnect(0)
	})
	// Only redacted output is allowed; never format response maps or JWTs.
	t.Logf("PASS real HTTP PostgreSQL broker: four routes, authorization, parsing, no-store, replay, versions, sessions; audit=%d", audit)
}
