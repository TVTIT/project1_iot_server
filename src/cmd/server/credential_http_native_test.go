package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"iot-platform/internal/auth"
	"iot-platform/internal/config"
	"iot-platform/internal/gateway"
	"iot-platform/internal/httpserver"
	"iot-platform/internal/mqttcredential"
)

type httpNativeTestInput struct {
	TestPGDSN       string `json:"test_pg_dsn"`
	ManagerUsername string `json:"manager_username"`
	ManagerPassword string `json:"manager_password"`
	BackendPassword string `json:"backend_password"`
	BrokerURL       string `json:"broker_url"`
	CAPath          string `json:"ca_path"`
	StorePath       string `json:"store_path"`
	WriterUID       uint32 `json:"writer_uid"`
	RunMarker       string `json:"run_marker"`
	ProjectBinding  string `json:"project_binding"`
}

var (
	httpNativeSafeRunMarkerPattern    = regexp.MustCompile(`^[a-zA-Z0-9_-]{8,64}$`)
	httpNativeProjectBindingPattern   = regexp.MustCompile(`^dynsec_probe_[a-f0-9]{12}$`)
	errHTTPNativeInvalidInputJSON     = errors.New("invalid native input: malformed or disallowed JSON")
	errHTTPNativeInvalidInputContract = errors.New("invalid native input: parameters do not meet strict runner contract")
	errHTTPNativeInputExceedsLimit    = errors.New("input payload exceeds limit")
)

func parseAndValidateHTTPNativeInput(r io.Reader) (in httpNativeTestInput, retErr error) {
	maxBytes := int64(64 << 10)
	buf := make([]byte, maxBytes+1)
	defer func() {
		for i := range buf {
			buf[i] = 0
		}
	}()

	n, err := io.ReadFull(io.LimitReader(r, maxBytes+1), buf)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return httpNativeTestInput{}, errHTTPNativeInvalidInputContract
	}
	if int64(n) > maxBytes {
		return httpNativeTestInput{}, errHTTPNativeInputExceedsLimit
	}

	payload := buf[:n]

	// Reject duplicate keys and verify JSON structure
	decStrict := json.NewDecoder(bytes.NewReader(payload))
	token, err := decStrict.Token()
	if err != nil || token != json.Delim('{') {
		return httpNativeTestInput{}, errHTTPNativeInvalidInputJSON
	}
	seenKeys := make(map[string]bool)
	for decStrict.More() {
		t, err := decStrict.Token()
		if err != nil {
			return httpNativeTestInput{}, errHTTPNativeInvalidInputJSON
		}
		key, ok := t.(string)
		if !ok {
			return httpNativeTestInput{}, errHTTPNativeInvalidInputJSON
		}
		if seenKeys[key] {
			return httpNativeTestInput{}, errHTTPNativeInvalidInputJSON
		}
		seenKeys[key] = true
		var val json.RawMessage
		if err := decStrict.Decode(&val); err != nil {
			return httpNativeTestInput{}, errHTTPNativeInvalidInputJSON
		}
	}
	tokEnd, err := decStrict.Token()
	if err != nil || tokEnd != json.Delim('}') {
		return httpNativeTestInput{}, errHTTPNativeInvalidInputJSON
	}

	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.DisallowUnknownFields()

	if err := dec.Decode(&in); err != nil {
		return httpNativeTestInput{}, errHTTPNativeInvalidInputJSON
	}

	var extra json.RawMessage
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return httpNativeTestInput{}, errHTTPNativeInvalidInputJSON
	}

	// 1. Validate DSN: host EXACT 'task28a-pg', port 5432, database EXACT 'fixture_service',
	// user 'fixture_admin', sslmode=disable only.
	if in.TestPGDSN == "" {
		return httpNativeTestInput{}, errHTTPNativeInvalidInputContract
	}
	u, err := url.Parse(in.TestPGDSN)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") {
		return httpNativeTestInput{}, errHTTPNativeInvalidInputContract
	}
	if u.Hostname() != "task28a-pg" || u.Port() != "5432" {
		return httpNativeTestInput{}, errHTTPNativeInvalidInputContract
	}
	if strings.TrimPrefix(u.Path, "/") != "fixture_service" {
		return httpNativeTestInput{}, errHTTPNativeInvalidInputContract
	}
	if u.User == nil || u.User.Username() != "fixture_admin" {
		return httpNativeTestInput{}, errHTTPNativeInvalidInputContract
	}
	q := u.Query()
	if q.Get("sslmode") != "disable" {
		return httpNativeTestInput{}, errHTTPNativeInvalidInputContract
	}
	for k := range q {
		if k != "sslmode" {
			return httpNativeTestInput{}, errHTTPNativeInvalidInputContract
		}
	}

	// 2. Validate Manager Credentials
	if in.ManagerUsername == "" || len(in.ManagerUsername) > 128 {
		return httpNativeTestInput{}, errHTTPNativeInvalidInputContract
	}
	if in.ManagerPassword == "" || len(in.ManagerPassword) < 8 || len(in.ManagerPassword) > 256 {
		return httpNativeTestInput{}, errHTTPNativeInvalidInputContract
	}

	// 3. Validate Backend Credentials
	if in.BackendPassword == "" || len(in.BackendPassword) < 8 || len(in.BackendPassword) > 256 {
		return httpNativeTestInput{}, errHTTPNativeInvalidInputContract
	}

	// 4. Validate Broker URL: EXACT ssl://localhost:8883
	if in.BrokerURL != "ssl://localhost:8883" {
		return httpNativeTestInput{}, errHTTPNativeInvalidInputContract
	}

	// 5. Validate Paths
	if in.CAPath != "/security/ca.crt" || in.StorePath != "/security/dynsec.json" {
		return httpNativeTestInput{}, errHTTPNativeInvalidInputContract
	}

	// 6. Validate WriterUID
	if in.WriterUID != 1883 {
		return httpNativeTestInput{}, errHTTPNativeInvalidInputContract
	}

	// 7. Validate RunMarker and Binding
	if !httpNativeSafeRunMarkerPattern.MatchString(in.RunMarker) {
		return httpNativeTestInput{}, errHTTPNativeInvalidInputContract
	}
	if !httpNativeProjectBindingPattern.MatchString(in.ProjectBinding) {
		return httpNativeTestInput{}, errHTTPNativeInvalidInputContract
	}

	return in, nil
}

type httpNativeEOFConn struct {
	net.Conn
	onEOF func()
	once  sync.Once
}

func (c *httpNativeEOFConn) Read(b []byte) (n int, err error) {
	n, err = c.Conn.Read(b)
	if err != nil && isRealHTTPNativeEOF(err) {
		c.once.Do(c.onEOF)
	}
	return n, err
}

func (c *httpNativeEOFConn) Close() error {
	return c.Conn.Close()
}

func isRealHTTPNativeEOF(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, syscall.ECONNRESET) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return false
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		if opErr.Err != nil && errors.Is(opErr.Err, syscall.ECONNRESET) {
			return true
		}
	}
	return false
}

func connectHTTPNativeMQTT(
	ctx context.Context,
	brokerURL string,
	tlsCfg *tls.Config,
	username, password string,
	timeout time.Duration,
) (mqtt.Client, <-chan struct{}, error) {
	u, err := url.Parse(brokerURL)
	if err != nil {
		return nil, nil, err
	}

	eofCh := make(chan struct{}, 1)
	var once sync.Once
	notifyEOF := func() {
		once.Do(func() {
			close(eofCh)
		})
	}

	opts := mqtt.NewClientOptions().
		AddBroker(brokerURL).
		SetClientID("test-http-" + username + "-" + uuid.New().String()[:8]).
		SetUsername(username).
		SetPassword(password).
		SetProtocolVersion(4).
		SetAutoReconnect(false).
		SetConnectRetry(false).
		SetCleanSession(true).
		SetConnectTimeout(timeout).
		SetWriteTimeout(timeout)

	opts.SetCustomOpenConnectionFn(func(_ *url.URL, _ mqtt.ClientOptions) (net.Conn, error) {
		d := tls.Dialer{NetDialer: &net.Dialer{}, Config: tlsCfg.Clone()}
		c, dialErr := d.DialContext(ctx, "tcp", u.Host)
		if dialErr != nil {
			return nil, dialErr
		}
		return &httpNativeEOFConn{Conn: c, onEOF: notifyEOF}, nil
	})

	client := mqtt.NewClient(opts)
	token := client.Connect()
	if !token.WaitTimeout(timeout) {
		client.Disconnect(0)
		return nil, nil, errors.New("connect timeout")
	}
	if err := token.Error(); err != nil {
		client.Disconnect(0)
		return nil, nil, err
	}

	return client, eofCh, nil
}

// TestSimplifiedHTTPNativeRotateRevoke is the opt-in native integration test
// for the real HTTP REST endpoints of credential rotate and revoke.
// Skips by default unless TASK28_HTTP_NATIVE_ISOLATED=1 is set.
func TestSimplifiedHTTPNativeRotateRevoke(t *testing.T) {
	if os.Getenv("TASK28_HTTP_NATIVE_ISOLATED") != "1" {
		t.Skip("skipping native isolated HTTP test: TASK28_HTTP_NATIVE_ISOLATED=1 not set")
	}

	// 1. Strict single JSON input via stdin
	input, err := parseAndValidateHTTPNativeInput(os.Stdin)
	if err != nil {
		t.Fatalf("native HTTP input invalid: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// 2. Setup PostgreSQL connection pool
	pool, err := pgxpool.New(ctx, input.TestPGDSN)
	if err != nil {
		t.Fatalf("failed to connect to test postgres: %v", err)
	}
	defer pool.Close()

	// 3. Seed fixture users and gateways
	adminUID := uuid.New()
	ownerUID := uuid.New()
	targetGW := fmt.Sprintf("gw-http-%s-%s", input.RunMarker[:8], uuid.New().String()[:8])

	var hasAuthUsers bool
	err = pool.QueryRow(ctx, `SELECT to_regclass('auth.users') IS NOT NULL`).Scan(&hasAuthUsers)
	if err != nil {
		t.Fatalf("failed to check auth.users table existence: %v", err)
	}

	if hasAuthUsers {
		_, err = pool.Exec(ctx, `
			INSERT INTO auth.users (id, aud, role, email)
			VALUES ($1, 'authenticated', 'authenticated', $3),
			       ($2, 'authenticated', 'authenticated', $4)
			ON CONFLICT (id) DO NOTHING`,
			adminUID, ownerUID,
			fmt.Sprintf("admin-%s@example.com", adminUID),
			fmt.Sprintf("owner-%s@example.com", ownerUID),
		)
		if err != nil {
			t.Fatalf("failed to seed auth.users: %v", err)
		}
	}

	// Insert profiles
	_, err = pool.Exec(ctx, `
		INSERT INTO public.profiles (id, full_name)
		VALUES ($1, 'Test Admin'), ($2, 'Test Owner')
		ON CONFLICT (id) DO NOTHING`,
		adminUID, ownerUID,
	)
	if err != nil {
		t.Fatalf("failed to seed profiles: %v", err)
	}

	// Insert platform_admins for adminUID ONLY
	_, err = pool.Exec(ctx, `
		INSERT INTO public.platform_admins (user_id)
		VALUES ($1)
		ON CONFLICT (user_id) DO NOTHING`,
		adminUID,
	)
	if err != nil {
		t.Fatalf("failed to seed platform_admins: %v", err)
	}

	// Insert gateway
	_, err = pool.Exec(ctx, `
		INSERT INTO public.gateways (gateway_id, name)
		VALUES ($1, 'HTTP Target Gateway')
		ON CONFLICT (gateway_id) DO NOTHING`,
		targetGW,
	)
	if err != nil {
		t.Fatalf("failed to seed gateway: %v", err)
	}

	// Insert owner membership in user_gateways (owner role, but NOT in platform_admins)
	_, err = pool.Exec(ctx, `
		INSERT INTO public.user_gateways (user_id, gateway_id, role)
		VALUES ($1, $2, 'owner')
		ON CONFLICT (user_id, gateway_id) DO NOTHING`,
		ownerUID, targetGW,
	)
	if err != nil {
		t.Fatalf("failed to seed user_gateways: %v", err)
	}

	// 4. Setup TLS config & Probe for independent MQTT login verification
	caBytes, err := os.ReadFile(input.CAPath)
	if err != nil {
		t.Fatalf("failed to read CA file: %v", err)
	}
	rootCAs := x509.NewCertPool()
	if !rootCAs.AppendCertsFromPEM(caBytes) {
		t.Fatal("failed to append CA certs to pool")
	}

	bu, err := url.Parse(input.BrokerURL)
	if err != nil {
		t.Fatalf("invalid broker URL: %v", err)
	}
	tlsCfg := &tls.Config{
		RootCAs:    rootCAs,
		ServerName: bu.Hostname(),
		MinVersion: tls.VersionTLS12,
	}

	probe, err := mqttcredential.NewProbe(input.BrokerURL, input.CAPath, 3*time.Second)
	if err != nil {
		t.Fatalf("failed to create probe: %v", err)
	}

	// Setup DynSec Snapshot Reader
	snapReader, err := mqttcredential.NewDynSecSnapshotReader(mqttcredential.DynSecSnapshotConfig{
		Path:      input.StorePath,
		WriterUID: input.WriterUID,
		MaxBytes:  16 << 20,
		ValidateTarget: func(u string) bool {
			return u == targetGW
		},
	})
	if err != nil {
		t.Fatalf("failed to create DynSecSnapshotReader: %v", err)
	}

	qual, err := mqttcredential.NewNativeDisableQualification(
		mqttcredential.QualifiedMosquittoImageRef,
		mqttcredential.QualifiedMosquittoConfigID,
		mqttcredential.QualifiedMosquittoPlatform,
	)
	if err != nil {
		t.Fatalf("failed to create qualification descriptor: %v", err)
	}

	// 5. Compose credentials via composeCredentials
	credCfg := config.CredentialConfig{
		Enabled:          true,
		ManagerUsername:  input.ManagerUsername,
		ManagerPassword:  input.ManagerPassword,
		BackendPassword:  input.BackendPassword,
		ManagementURL:    input.BrokerURL,
		CAFile:           input.CAPath,
		SnapshotPath:     input.StorePath,
		WriterUID:        input.WriterUID,
		Qualification:    qual,
		DBTimeout:        5 * time.Second,
		FinalizeTimeout:  5 * time.Second,
		ReconcileTimeout: 10 * time.Second,
		OperationTimeout: 5 * time.Second,
		RequestTimeout:   30 * time.Second,
		RecoveryTimeout:  5 * time.Second,
		ClientTimeout:    3 * time.Second,
		BatchSize:        64,
		MaxPages:         16,
		QueueSize:        8,
		MaxInflight:      4,
		MaxPayloadBytes:  64 << 10,
		MaxSnapshotBytes: 16 << 20,
	}

	composition, err := composeCredentials(ctx, pool, credCfg)
	if err != nil {
		t.Fatalf("composeCredentials failed: %v", err)
	}
	defer func() {
		_ = composition.close()
	}()

	// Assert readiness
	if !composition.ready.Ready() {
		t.Fatal("composition startup readiness reported false")
	}

	// 6. Setup JWT Verifier (random fixture signing secret)
	jwtSecret := "fixture-jwt-secret-min-32-chars-task28-" + input.RunMarker[:8]
	jwtIssuer := "https://fixture.invalid/auth/v1"
	jwtAudience := "authenticated"
	verifier, err := auth.NewHS256Verifier(auth.VerifierConfig{
		Secret:   jwtSecret,
		Issuer:   jwtIssuer,
		Audience: jwtAudience,
	})
	if err != nil {
		t.Fatalf("failed to create JWT verifier: %v", err)
	}

	adminChecker, err := auth.NewPostgresPlatformAdminChecker(pool)
	if err != nil {
		t.Fatalf("failed to create platform admin checker: %v", err)
	}

	gatewayRepo, err := gateway.NewPostgresRepository(pool)
	if err != nil {
		t.Fatalf("failed to create gateway repository: %v", err)
	}
	gatewaySvc, err := gateway.NewService(gatewayRepo, 2*time.Second)
	if err != nil {
		t.Fatalf("failed to create gateway service: %v", err)
	}
	provRepo, err := gateway.NewPostgresProvisioningRepository(pool)
	if err != nil {
		t.Fatalf("failed to create provisioning repository: %v", err)
	}
	provSvc, err := gateway.NewProvisioningService(provRepo, 2*time.Second)
	if err != nil {
		t.Fatalf("failed to create provisioning service: %v", err)
	}
	ownerSensorRepo, err := gateway.NewPostgresOwnerSensorProvisioningRepository(pool)
	if err != nil {
		t.Fatalf("failed to create owner sensor repo: %v", err)
	}
	ownerSensorSvc, err := gateway.NewOwnerSensorProvisioningService(ownerSensorRepo, 2*time.Second)
	if err != nil {
		t.Fatalf("failed to create owner sensor service: %v", err)
	}
	adminGatewayRepo, err := gateway.NewPostgresAdminGatewayRepository(pool)
	if err != nil {
		t.Fatalf("failed to create admin gateway repo: %v", err)
	}
	adminGatewaySvc, err := gateway.NewAdminGatewayService(adminGatewayRepo, 2*time.Second, 100, 200)
	if err != nil {
		t.Fatalf("failed to create admin gateway service: %v", err)
	}

	routerHandler, err := httpserver.NewRouter(httpserver.RouterDependencies{
		CredentialAPIEnabled:       true,
		CredentialRequestTimeout:   credCfg.RequestTimeout,
		CredentialManager:          composition.manager,
		CredentialStartupReadiness: composition.ready,
		GatewayProvisioner:         provSvc,
		SensorProvisioner:          provSvc,
		OwnerSensorProvisioner:     ownerSensorSvc,
		AdminGatewayLister:         adminGatewaySvc,
		AdminInventoryDefaultLimit: 100,
		AdminInventoryMaxLimit:     200,
		AdminMaxBodyBytes:          128,
		GatewayReader:              gatewaySvc,
		SensorReader:               gatewaySvc,
		ReadinessChecker:           pool,
		ReadinessTimeout:           2 * time.Second,
		AuthorizationTimeout:       2 * time.Second,
		TokenVerifier:              verifier,
		PlatformAdminChecker:       adminChecker,
	})
	if err != nil {
		t.Fatalf("failed to create HTTP router: %v", err)
	}

	// 7. Start real HTTP TCP listener using httptest.Server
	ts := httptest.NewServer(routerHandler)
	defer ts.Close()

	httpClient := ts.Client()
	httpClient.Timeout = 15 * time.Second

	// Helper to sign JWTs (verifier-seam proof NOT real Supabase login)
	signJWT := func(sub uuid.UUID, role string) string {
		now := time.Now()
		tok := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
			"sub":  sub.String(),
			"iss":  jwtIssuer,
			"aud":  jwtAudience,
			"iat":  now.Unix(),
			"exp":  now.Add(5 * time.Minute).Unix(),
			"role": role,
		})
		s, err := tok.SignedString([]byte(jwtSecret))
		if err != nil {
			t.Fatalf("failed to sign test JWT: %v", err)
		}
		return s
	}

	adminJWT := signJWT(adminUID, "authenticated")
	ownerJWT := signJWT(ownerUID, "authenticated")

	baseEndpoint := ts.URL + "/v1/admin/gateways/" + targetGW + "/mqtt-credential"

	sendReq := func(method, endpoint, bearerToken, idempotencyKey string, wantStatus int) map[string]any {
		req, err := http.NewRequestWithContext(ctx, method, endpoint, nil)
		if err != nil {
			t.Fatalf("failed to build HTTP request: %v", err)
		}
		if bearerToken != "" {
			req.Header.Set("Authorization", "Bearer "+bearerToken)
		}
		if idempotencyKey != "" {
			req.Header.Set("Idempotency-Key", idempotencyKey)
		}
		resp, err := httpClient.Do(req)
		if err != nil {
			t.Fatalf("HTTP request failed (%s %s): %v", method, endpoint, err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.Header.Get("Cache-Control") != "no-store" || resp.Header.Get("Pragma") != "no-cache" {
			t.Fatalf("missing no-store/no-cache headers on %s %s", method, endpoint)
		}

		bodyBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("failed to read response body: %v", err)
		}

		if resp.StatusCode != wantStatus {
			t.Fatalf("%s %s expected status %d, got %d (body: %s)", method, endpoint, wantStatus, resp.StatusCode, string(bodyBytes))
		}

		var parsed map[string]any
		if len(bodyBytes) > 0 {
			if err := json.Unmarshal(bodyBytes, &parsed); err != nil {
				t.Fatalf("failed to parse JSON response: %v", err)
			}
		}
		return parsed
	}

	// -------------------------------------------------------------------------
	// A. Baseline Provision via HTTP POST
	// -------------------------------------------------------------------------
	provKey := uuid.New().String()
	provResp := sendReq("POST", baseEndpoint, adminJWT, provKey, http.StatusCreated)

	baselineSecret, ok := provResp["password"].(string)
	if !ok || baselineSecret == "" {
		t.Fatal("baseline provision HTTP did not return password in JSON response")
	}
	if provResp["secret_returned"] != true {
		t.Fatal("expected secret_returned: true on baseline provision")
	}

	// Verify baseline login works via Probe
	if err := probe.Login(ctx, targetGW, baselineSecret); err != nil {
		t.Fatalf("probe login target baseline secret failed: %v", err)
	}

	// Establish target active MQTT session for EOF assertion later
	targetClient, targetEOF, err := connectHTTPNativeMQTT(ctx, input.BrokerURL, tlsCfg, targetGW, baselineSecret, 3*time.Second)
	if err != nil {
		t.Fatalf("failed to connect target initial MQTT session: %v", err)
	}
	defer targetClient.Disconnect(0)

	// Record baseline state
	metaBefore := sendReq("GET", baseEndpoint, adminJWT, "", http.StatusOK)
	versionBefore := metaBefore["credential_version"]

	var journalCountBefore int
	err = pool.QueryRow(ctx, `SELECT count(*) FROM public.gateway_mqtt_credential_events WHERE gateway_id = $1`, targetGW).Scan(&journalCountBefore)
	if err != nil {
		t.Fatalf("failed to query journal count: %v", err)
	}

	// -------------------------------------------------------------------------
	// B. Owner WITHOUT Admin Grant: Rotate and Revoke rejected 403 before mutation
	// -------------------------------------------------------------------------
	ownerRotateKey := uuid.New().String()
	sendReq("POST", baseEndpoint+"/rotate", ownerJWT, ownerRotateKey, http.StatusForbidden)

	ownerRevokeKey := uuid.New().String()
	sendReq("DELETE", baseEndpoint, ownerJWT, ownerRevokeKey, http.StatusForbidden)

	// Assert version, journal, native state unchanged, and baseline password still functional
	metaAfterDenied := sendReq("GET", baseEndpoint, adminJWT, "", http.StatusOK)
	if metaAfterDenied["credential_version"] != versionBefore {
		t.Fatalf("credential version changed after denied owner requests: before=%v, after=%v", versionBefore, metaAfterDenied["credential_version"])
	}

	var journalCountAfterDenied int
	err = pool.QueryRow(ctx, `SELECT count(*) FROM public.gateway_mqtt_credential_events WHERE gateway_id = $1`, targetGW).Scan(&journalCountAfterDenied)
	if err != nil {
		t.Fatalf("failed to query journal count: %v", err)
	}
	if journalCountAfterDenied != journalCountBefore {
		t.Fatalf("journal count mutated after denied owner requests: before=%d, after=%d", journalCountBefore, journalCountAfterDenied)
	}

	if err := probe.Login(ctx, targetGW, baselineSecret); err != nil {
		t.Fatalf("baseline password ceased to function after owner 403 checks: %v", err)
	}

	// -------------------------------------------------------------------------
	// C. Invalid JWT: 401 Unauthorized
	// -------------------------------------------------------------------------
	sendReq("POST", baseEndpoint+"/rotate", "invalid-or-malformed-token", uuid.New().String(), http.StatusUnauthorized)
	sendReq("DELETE", baseEndpoint, "", uuid.New().String(), http.StatusUnauthorized)

	// -------------------------------------------------------------------------
	// D. Admin Rotate with new key
	// -------------------------------------------------------------------------
	adminRotateKey := uuid.New().String()
	rotResp := sendReq("POST", baseEndpoint+"/rotate", adminJWT, adminRotateKey, http.StatusOK)

	newSecret, ok := rotResp["password"].(string)
	if !ok || newSecret == "" || newSecret == baselineSecret {
		t.Fatal("rotate HTTP did not return new distinct password")
	}
	if rotResp["secret_returned"] != true {
		t.Fatal("expected secret_returned: true on rotate")
	}

	// Metadata version incremented
	metaAfterRotate := sendReq("GET", baseEndpoint, adminJWT, "", http.StatusOK)
	rotVer, ok := metaAfterRotate["credential_version"].(float64)
	if !ok || int(rotVer) != 2 {
		t.Fatalf("expected credential version 2 after rotate, got %v", metaAfterRotate["credential_version"])
	}

	// Fresh new-password login PASSES
	if err := probe.Login(ctx, targetGW, newSecret); err != nil {
		t.Fatalf("probe login target with new secret failed: %v", err)
	}

	// Old-password explicit denial
	if err := probe.Rejected(ctx, targetGW, baselineSecret); err != nil {
		t.Fatalf("probe rejected target with old secret failed: %v", err)
	}

	// Active session observes real transport EOF / reset
	select {
	case <-targetEOF:
		// Success: active session observed connection reset/drop
	case <-time.After(5 * time.Second):
		t.Fatal("previous target session did not observe EOF after rotate within 5s")
	}

	// -------------------------------------------------------------------------
	// E. Same-key Rotate Replay: metadata-only, no secret
	// -------------------------------------------------------------------------
	rotReplayResp := sendReq("POST", baseEndpoint+"/rotate", adminJWT, adminRotateKey, http.StatusOK)
	if rotReplayResp["password"] != nil && rotReplayResp["password"] != "" {
		t.Fatal("rotate replay unexpectedly returned secret password")
	}
	if rotReplayResp["secret_returned"] == true {
		t.Fatal("rotate replay reported secret_returned: true")
	}
	replayVer, ok := rotReplayResp["credential_version"].(float64)
	if !ok || int(replayVer) != 2 {
		t.Fatalf("expected version 2 on rotate replay, got %v", rotReplayResp["credential_version"])
	}

	// Connect active session with new secret before revoking
	targetRotatedClient, targetRotatedEOF, err := connectHTTPNativeMQTT(ctx, input.BrokerURL, tlsCfg, targetGW, newSecret, 3*time.Second)
	if err != nil {
		t.Fatalf("failed to connect rotated target session: %v", err)
	}
	defer targetRotatedClient.Disconnect(0)

	// -------------------------------------------------------------------------
	// F. Admin Revoke via HTTP DELETE: no secret, disabled/role/ACL store verification
	// -------------------------------------------------------------------------
	adminRevokeKey := uuid.New().String()
	revResp := sendReq("DELETE", baseEndpoint, adminJWT, adminRevokeKey, http.StatusOK)

	if revResp["password"] != nil && revResp["password"] != "" {
		t.Fatal("revoke unexpectedly returned secret password")
	}
	if revResp["secret_returned"] == true {
		t.Fatal("revoke reported secret_returned: true")
	}
	revStatus, ok := revResp["status"].(string)
	if !ok || revStatus != "revoked" {
		t.Fatalf("expected status 'revoked', got %v", revResp["status"])
	}

	// Target fresh connect explicit auth denial
	if err := probe.Rejected(ctx, targetGW, newSecret); err != nil {
		t.Fatalf("probe rejected target with rotated secret after revoke failed: %v", err)
	}

	// Active target session closes (real transport EOF / reset)
	select {
	case <-targetRotatedEOF:
		// Success: active session observed connection reset/drop
	case <-time.After(5 * time.Second):
		t.Fatal("active target session did not observe EOF after revoke within 5s")
	}

	// Verify native store via snapReader directly
	if err := snapReader.VerifyRevoked(ctx, targetGW); err != nil {
		t.Fatalf("snapReader.VerifyRevoked failed after HTTP revoke: %v", err)
	}
}
