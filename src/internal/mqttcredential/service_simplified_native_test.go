package mqttcredential

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
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// nativeTestInput defines the strict single JSON contract received via stdin.
// Host/database validation prevents targeting arbitrary or production databases.
type nativeTestInput struct {
	TestPGDSN       string `json:"test_pg_dsn"`
	ManagerUsername string `json:"manager_username"`
	ManagerPassword string `json:"manager_password"`
	BrokerURL       string `json:"broker_url"`
	CAPath          string `json:"ca_path"`
	StorePath       string `json:"store_path"`
	WriterUID       uint32 `json:"writer_uid"`
	RunMarker       string `json:"run_marker"`
	ProjectBinding  string `json:"project_binding"`
}

var (
	safeRunMarkerPattern     = regexp.MustCompile(`^[a-zA-Z0-9_-]{8,64}$`)
	projectBindingPattern    = regexp.MustCompile(`^dynsec_probe_[a-f0-9]{12}$`)
	errInvalidInputJSON      = errors.New("invalid native input: malformed or disallowed JSON")
	errInvalidInputContract  = errors.New("invalid native input: parameters do not meet strict runner contract")
	errInputExceedsSizeLimit = errors.New("input payload exceeds limit")
)

// parseAndValidateNativeInput reads a single, bounded JSON payload from r and validates
// that all fields conform to the exact runner contract.
// The buffer is cleared on return, errors are generic without echoing raw inputs.
func parseAndValidateNativeInput(r io.Reader) (in nativeTestInput, retErr error) {
	maxBytes := int64(64 << 10)
	buf := make([]byte, maxBytes+1)
	defer func() {
		for i := range buf {
			buf[i] = 0
		}
	}()

	n, err := io.ReadFull(io.LimitReader(r, maxBytes+1), buf)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return nativeTestInput{}, errInvalidInputContract
	}
	if int64(n) > maxBytes {
		return nativeTestInput{}, errInputExceedsSizeLimit
	}

	payload := buf[:n]

	// Reject duplicate JSON keys and structural anomalies
	if err := strictDynSecJSON(payload); err != nil {
		return nativeTestInput{}, errInvalidInputJSON
	}

	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.DisallowUnknownFields()

	if err := dec.Decode(&in); err != nil {
		return nativeTestInput{}, errInvalidInputJSON
	}

	var extra json.RawMessage
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return nativeTestInput{}, errInvalidInputJSON
	}

	// 1. Validate DSN: host EXACT 'task28a-pg', port 5432, database EXACT 'fixture_service',
	// user 'fixture_admin', sslmode=disable only, no multi-host/hostaddr/service/query overrides.
	if in.TestPGDSN == "" {
		return nativeTestInput{}, errInvalidInputContract
	}
	u, err := url.Parse(in.TestPGDSN)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") {
		return nativeTestInput{}, errInvalidInputContract
	}
	if u.Hostname() != "task28a-pg" || u.Port() != "5432" {
		return nativeTestInput{}, errInvalidInputContract
	}
	if strings.TrimPrefix(u.Path, "/") != "fixture_service" {
		return nativeTestInput{}, errInvalidInputContract
	}
	if u.User == nil || u.User.Username() != "fixture_admin" {
		return nativeTestInput{}, errInvalidInputContract
	}
	// Query params: sslmode=disable only
	q := u.Query()
	if q.Get("sslmode") != "disable" {
		return nativeTestInput{}, errInvalidInputContract
	}
	// No unexpected query parameters or host overrides
	for k := range q {
		if k != "sslmode" {
			return nativeTestInput{}, errInvalidInputContract
		}
	}

	// 2. Validate Manager Credentials
	if in.ManagerUsername == "" || len(in.ManagerUsername) > 128 {
		return nativeTestInput{}, errInvalidInputContract
	}
	if in.ManagerPassword == "" || len(in.ManagerPassword) < 8 || len(in.ManagerPassword) > 256 {
		return nativeTestInput{}, errInvalidInputContract
	}

	// 3. Validate Broker URL: EXACT ssl://localhost:8883
	if in.BrokerURL != "ssl://localhost:8883" {
		return nativeTestInput{}, errInvalidInputContract
	}

	// 4. Validate Paths: ca_path EXACT '/security/ca.crt', store_path EXACT '/security/dynsec.json'
	if in.CAPath != "/security/ca.crt" || in.StorePath != "/security/dynsec.json" {
		return nativeTestInput{}, errInvalidInputContract
	}

	// 5. Validate WriterUID: explicit 1883 for pinned fixture
	if in.WriterUID != 1883 {
		return nativeTestInput{}, errInvalidInputContract
	}

	// 6. Validate Marker and Binding: marker>=8 safe chars, binding 'dynsec_probe_<12lowerhex>'
	if !safeRunMarkerPattern.MatchString(in.RunMarker) {
		return nativeTestInput{}, errInvalidInputContract
	}
	if !projectBindingPattern.MatchString(in.ProjectBinding) {
		return nativeTestInput{}, errInvalidInputContract
	}

	return in, nil
}

// extractSecretFromResponse privately extracts the generated password from a SecretResult
// via its explicit JSON marshaling seam, avoiding exposing or logging secrets.
func extractSecretFromResponse(t *testing.T, s *SecretResult) string {
	t.Helper()
	if s == nil {
		t.Fatal("secret result is nil")
	}
	b, err := json.Marshal(s)
	if err != nil {
		t.Fatalf("failed to marshal SecretResult: %v", err)
	}
	var out struct {
		Password       string `json:"password"`
		SecretReturned bool   `json:"secret_returned"`
	}
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("failed to unmarshal SecretResult JSON: %v", err)
	}
	if !out.SecretReturned || out.Password == "" {
		t.Fatal("secret was not returned in JSON representation")
	}
	return out.Password
}

// countingIntentAssertingExecutor wraps SimplifiedExecutor to assert D1 intent in
// the real journal table (public.gateway_mqtt_credential_events) and records execution calls.
type countingIntentAssertingExecutor struct {
	inner       SimplifiedExecutor
	pool        *pgxpool.Pool
	t           *testing.T
	execCounter atomic.Int64
}

func (w *countingIntentAssertingExecutor) Execute(ctx context.Context, o Operation, gen func(context.Context) (string, error)) (SimplifiedExecutionResult, error) {
	w.execCounter.Add(1)

	// D1 Check: Assert pending INTENT row is visible in real journal table before MQTT mutations
	var (
		status            string
		phase             string
		executionContract string
	)
	err := w.pool.QueryRow(ctx,
		`SELECT status, phase, execution_contract FROM public.gateway_mqtt_credential_events WHERE operation_id = $1`,
		o.OperationID,
	).Scan(&status, &phase, &executionContract)
	if err != nil {
		w.t.Fatalf("countingIntentAssertingExecutor: failed to find pending intent row in gateway_mqtt_credential_events for operation %s: %v", o.OperationID, err)
	}
	if status != string(OperationPending) {
		w.t.Fatalf("countingIntentAssertingExecutor: expected status %q, got %q", OperationPending, status)
	}
	if phase != string(PhaseIntent) {
		w.t.Fatalf("countingIntentAssertingExecutor: expected phase %q, got %q", PhaseIntent, phase)
	}
	if ExecutionContract(executionContract) != ContractSimplified {
		w.t.Fatalf("countingIntentAssertingExecutor: expected contract %q, got %q", ContractSimplified, executionContract)
	}

	return w.inner.Execute(ctx, o, gen)
}

func (w *countingIntentAssertingExecutor) CheckRevoked(ctx context.Context, u string) error {
	return w.inner.CheckRevoked(ctx, u)
}

func (w *countingIntentAssertingExecutor) ExecuteCount() int64 {
	return w.execCounter.Load()
}

// denyResetPrereq satisfies ResetPrerequisite failing closed with CodeServiceUnavailable.
type denyResetPrereq struct{}

func (denyResetPrereq) SupervisedQuiescencePrerequisite(ctx context.Context, g string) error {
	return &DomainError{Code: CodeServiceUnavailable}
}

// isRealEOFOrReset determines whether err is a true io.EOF, io.ErrUnexpectedEOF, or syscall.ECONNRESET.
// Local close, timeouts, or other operational errors return false.
func isRealEOFOrReset(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	if errors.Is(err, syscall.ECONNRESET) {
		return true
	}
	// Check net.OpError unwrapping to syscall.ECONNRESET or explicit read EOF
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

// eofMonitoringConn wraps net.Conn to monitor transport-level EOF or ECONNRESET on Read.
// Close() or timeouts never trigger onEOF.
type eofMonitoringConn struct {
	net.Conn
	onEOF func()
	once  sync.Once
}

func (c *eofMonitoringConn) Read(b []byte) (n int, err error) {
	n, err = c.Conn.Read(b)
	if err != nil && isRealEOFOrReset(err) {
		c.once.Do(c.onEOF)
	}
	return n, err
}

func (c *eofMonitoringConn) Close() error {
	// Close does NOT signal EOF.
	return c.Conn.Close()
}

// connectPahoSession establishes a real TLS MQTT session for an active client and returns
// the client and an EOF notification channel that closes ONLY when the transport observes real EOF/reset.
func connectPahoSession(
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
		SetClientID("test-" + username + "-" + uuid.New().String()[:8]).
		SetUsername(username).
		SetPassword(password).
		SetProtocolVersion(4).
		SetAutoReconnect(false).
		SetConnectRetry(false).
		SetCleanSession(true).
		SetConnectTimeout(timeout).
		SetWriteTimeout(timeout)
	// ConnectionLostHandler does NOT count as EOF without actual read observation.

	opts.SetCustomOpenConnectionFn(func(_ *url.URL, _ mqtt.ClientOptions) (net.Conn, error) {
		d := tls.Dialer{NetDialer: &net.Dialer{}, Config: tlsCfg.Clone()}
		c, dialErr := d.DialContext(ctx, "tcp", u.Host)
		if dialErr != nil {
			return nil, dialErr
		}
		wrapped := &eofMonitoringConn{Conn: c, onEOF: notifyEOF}
		return wrapped, nil
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

// TestSimplifiedServiceNativeRotateRevoke is the opt-in native integration test for A8a.
// It skips by default unless TASK28A_NATIVE_ISOLATED=1 is set.
func TestSimplifiedServiceNativeRotateRevoke(t *testing.T) {
	if os.Getenv("TASK28A_NATIVE_ISOLATED") != "1" {
		t.Skip("skipping native isolated test: TASK28A_NATIVE_ISOLATED=1 not set")
	}

	// 1. Strict single JSON input via stdin
	input, err := parseAndValidateNativeInput(os.Stdin)
	if err != nil {
		t.Fatalf("native input invalid: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	// 2. Setup PostgreSQL connection pool
	pool, err := pgxpool.New(ctx, input.TestPGDSN)
	if err != nil {
		t.Fatalf("failed to connect to test postgres: %v", err)
	}
	defer pool.Close()

	repo, err := NewPostgresRepository(pool, 5*time.Second)
	if err != nil {
		t.Fatalf("failed to create postgres repository: %v", err)
	}

	// 3. Seed fixture users and gateways
	// Respect foreign key: profiles -> auth.users if auth.users table exists
	// Emails use reserved example domain (.example.com) to avoid personal deployment values.
	adminUID := uuid.New()
	nonAdminUID := uuid.New()
	targetGW := fmt.Sprintf("gw-target-%s-%s", input.RunMarker[:8], uuid.New().String()[:8])
	observerGW := fmt.Sprintf("gw-obs-%s-%s", input.RunMarker[:8], uuid.New().String()[:8])

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
			adminUID, nonAdminUID,
			fmt.Sprintf("admin-%s@example.com", adminUID),
			fmt.Sprintf("user-%s@example.com", nonAdminUID),
		)
		if err != nil {
			t.Fatalf("failed to seed auth.users: %v", err)
		}
	}

	// Insert profiles
	_, err = pool.Exec(ctx, `
		INSERT INTO public.profiles (id, full_name)
		VALUES ($1, 'Test Admin'), ($2, 'Test NonAdmin')
		ON CONFLICT (id) DO NOTHING`,
		adminUID, nonAdminUID,
	)
	if err != nil {
		t.Fatalf("failed to seed profiles: %v", err)
	}

	// Insert platform_admin for adminUID ONLY
	_, err = pool.Exec(ctx, `
		INSERT INTO public.platform_admins (user_id)
		VALUES ($1)
		ON CONFLICT (user_id) DO NOTHING`,
		adminUID,
	)
	if err != nil {
		t.Fatalf("failed to seed platform_admins: %v", err)
	}

	// Insert gateways
	_, err = pool.Exec(ctx, `
		INSERT INTO public.gateways (gateway_id, name)
		VALUES ($1, 'Target Gateway'), ($2, 'Observer Gateway')
		ON CONFLICT (gateway_id) DO NOTHING`,
		targetGW, observerGW,
	)
	if err != nil {
		t.Fatalf("failed to seed gateways: %v", err)
	}

	// 4. Setup TLS config & Probe
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

	probe, err := NewProbe(input.BrokerURL, input.CAPath, 3*time.Second)
	if err != nil {
		t.Fatalf("failed to create probe: %v", err)
	}

	// 5. Setup DynSec Snapshot Reader & P1 VerifyRevoked
	snapReader, err := NewDynSecSnapshotReader(DynSecSnapshotConfig{
		Path:      input.StorePath,
		WriterUID: input.WriterUID,
		MaxBytes:  16 << 20,
		ValidateTarget: func(u string) bool {
			return u == targetGW || u == observerGW
		},
	})
	if err != nil {
		t.Fatalf("failed to create DynSecSnapshotReader: %v", err)
	}

	// 6. Setup Real SimplifiedDynSecExecutor
	qual, err := NewNativeDisableQualification(
		QualifiedMosquittoImageRef,
		QualifiedMosquittoConfigID,
		QualifiedMosquittoPlatform,
	)
	if err != nil {
		t.Fatalf("failed to create qualification descriptor: %v", err)
	}

	executorCfg := SimplifiedDynSecConfig{
		NewClient: func(c context.Context) (SimplifiedDynSecClient, error) {
			return NewDynSecClient(c, DynSecConfig{
				BrokerURL:          input.BrokerURL,
				CAPEM:              caBytes,
				ManagerUsername:    input.ManagerUsername,
				ManagerPassword:    input.ManagerPassword,
				ProtectedUsernames: []string{input.ManagerUsername, BackendUsername},
				Timeout:            3 * time.Second,
				MaxPayloadBytes:    64 << 10,
				MaxInflight:        4,
				QueueSize:          8,
				ValidateTarget: func(u string) bool {
					return u == targetGW || u == observerGW
				},
				ValidateRole: func(u, r string) bool {
					return r == "gateway_"+u
				},
			})
		},
		Login:              probe.Login,
		Rejected:           probe.Rejected,
		ProtectedUsernames: []string{input.ManagerUsername, BackendUsername},
		Timeout:            5 * time.Second,
		VerifyRevoked:      snapReader.VerifyRevoked,
		Qualification:      qual,
	}

	realExecutor, err := NewSimplifiedDynSecExecutor(executorCfg)
	if err != nil {
		t.Fatalf("failed to create SimplifiedDynSecExecutor: %v", err)
	}

	// Wrap executor with D1 intent assertion and call counter
	wrappedExecutor := &countingIntentAssertingExecutor{
		inner: realExecutor,
		pool:  pool,
		t:     t,
	}

	// 7. Setup Real ProvisionService
	svc, err := NewSimplifiedProvisionService(
		repo,
		wrappedExecutor,
		denyResetPrereq{},
		SimplifiedProvisionServiceOptions{
			RecoveryTimeout: 5 * time.Second,
		},
	)
	if err != nil {
		t.Fatalf("failed to create ProvisionService: %v", err)
	}

	// -------------------------------------------------------------------------
	// BASELINE: Provision targetGW and observerGW
	// -------------------------------------------------------------------------
	targetProvKey := uuid.New()
	targetProvResp, err := svc.Provision(ctx, adminUID, MutationInput{
		GatewayID:      targetGW,
		IdempotencyKey: targetProvKey,
		Action:         ActionProvision,
	})
	if err != nil {
		t.Fatalf("baseline provision target failed: %v", err)
	}
	if targetProvResp.Secret == nil || !targetProvResp.SecretReturned {
		t.Fatal("target provision did not return secret")
	}
	targetInitialSecret := extractSecretFromResponse(t, targetProvResp.Secret)
	if targetInitialSecret == "" {
		t.Fatal("target initial secret is empty")
	}

	observerProvKey := uuid.New()
	obsProvResp, err := svc.Provision(ctx, adminUID, MutationInput{
		GatewayID:      observerGW,
		IdempotencyKey: observerProvKey,
		Action:         ActionProvision,
	})
	if err != nil {
		t.Fatalf("baseline provision observer failed: %v", err)
	}
	if obsProvResp.Secret == nil || !obsProvResp.SecretReturned {
		t.Fatal("observer provision did not return secret")
	}
	obsSecret := extractSecretFromResponse(t, obsProvResp.Secret)

	// Verify both can authenticate via Probe
	if err := probe.Login(ctx, targetGW, targetInitialSecret); err != nil {
		t.Fatalf("probe login target initial failed: %v", err)
	}
	if err := probe.Login(ctx, observerGW, obsSecret); err != nil {
		t.Fatalf("probe login observer failed: %v", err)
	}

	// Baseline count: target provision (1) + observer provision (1) = 2
	expectedExecCount := int64(2)
	if got := wrappedExecutor.ExecuteCount(); got != expectedExecCount {
		t.Fatalf("expected executor call count %d after baseline provision, got %d", expectedExecCount, got)
	}

	// -------------------------------------------------------------------------
	// SECURITY SEAM: Non-admin rejected before executor
	// -------------------------------------------------------------------------
	_, nonAdminErr := svc.Rotate(ctx, nonAdminUID, MutationInput{
		GatewayID:      targetGW,
		IdempotencyKey: uuid.New(),
		Action:         ActionRotate,
	})
	var domainErr *DomainError
	if !errors.As(nonAdminErr, &domainErr) || domainErr.Code != CodeForbidden {
		t.Fatalf("expected CodeForbidden for non-admin rotate, got: %v", nonAdminErr)
	}
	if got := wrappedExecutor.ExecuteCount(); got != expectedExecCount {
		t.Fatalf("executor was invoked during non-admin rotate (count: %d, want: %d)", got, expectedExecCount)
	}

	// -------------------------------------------------------------------------
	// TARGET SESSION ESTABLISHMENT: Establish active target MQTT session
	// -------------------------------------------------------------------------
	targetClient, targetEOF, err := connectPahoSession(ctx, input.BrokerURL, tlsCfg, targetGW, targetInitialSecret, 3*time.Second)
	if err != nil {
		t.Fatalf("failed to connect target initial MQTT session: %v", err)
	}
	defer targetClient.Disconnect(0)

	// -------------------------------------------------------------------------
	// OBSERVER TRAFFIC: Established session publish QoS1 own telemetry receives PUBACK.
	// Protocol/auth isolation verification ONLY; not receiver delivery proof.
	// Telemetry subscription is forbidden by adapterGatewayACLs.
	// -------------------------------------------------------------------------
	obsClient, _, err := connectPahoSession(ctx, input.BrokerURL, tlsCfg, observerGW, obsSecret, 3*time.Second)
	if err != nil {
		t.Fatalf("failed to connect observer MQTT session: %v", err)
	}
	defer obsClient.Disconnect(0)

	obsTelemetryTopic := fmt.Sprintf("gateways/%s/telemetry/sample", observerGW)
	pubToken := obsClient.Publish(obsTelemetryTopic, 1, false, "obs-test-payload-1")
	if !pubToken.WaitTimeout(3*time.Second) || pubToken.Error() != nil {
		t.Fatalf("observer initial QoS1 publish own telemetry failed: %v", pubToken.Error())
	}

	// -------------------------------------------------------------------------
	// ROTATE TARGET: Ordered adapter flow (Disable -> Readbacks -> SetPassword -> Enable)
	// -------------------------------------------------------------------------
	targetRotateKey := uuid.New()
	rotResp, err := svc.Rotate(ctx, adminUID, MutationInput{
		GatewayID:      targetGW,
		IdempotencyKey: targetRotateKey,
		Action:         ActionRotate,
	})
	if err != nil {
		t.Fatalf("rotate target failed: %v", err)
	}
	if rotResp.Secret == nil || !rotResp.SecretReturned {
		t.Fatal("rotate target did not return secret")
	}
	targetNewSecret := extractSecretFromResponse(t, rotResp.Secret)
	if targetNewSecret == "" || targetNewSecret == targetInitialSecret {
		t.Fatal("rotate returned invalid or unchanged secret")
	}
	if rotResp.Metadata.CredentialVersion != 2 {
		t.Fatalf("expected credential version 2 after rotate, got %d", rotResp.Metadata.CredentialVersion)
	}
	expectedExecCount++ // rotate executed (3)
	if got := wrappedExecutor.ExecuteCount(); got != expectedExecCount {
		t.Fatalf("expected executor call count %d after rotate, got %d", expectedExecCount, got)
	}

	// 1. Fresh TLS connect with NEW secret PASSES
	if err := probe.Login(ctx, targetGW, targetNewSecret); err != nil {
		t.Fatalf("probe login target with new secret failed: %v", err)
	}

	// 2. Exact OLD secret is auth denied
	if err := probe.Rejected(ctx, targetGW, targetInitialSecret); err != nil {
		t.Fatalf("probe rejected target with old secret failed: %v", err)
	}

	// 3. Previous target session observes real transport EOF / reset (not timeout or local close)
	select {
	case <-targetEOF:
		// Success: active session observed connection reset/drop
	case <-time.After(5 * time.Second):
		t.Fatal("previous target session did not observe EOF after rotate within 5s")
	}

	// 4. Observer isolation proof:
	// Existing observer session remains open and publishes QoS1 to own telemetry receiving PUBACK.
	// Fresh TLS login for observer continues to succeed.
	pubToken2 := obsClient.Publish(obsTelemetryTopic, 1, false, "obs-test-payload-2")
	if !pubToken2.WaitTimeout(3*time.Second) || pubToken2.Error() != nil {
		t.Fatalf("observer QoS1 publish own telemetry after target rotate failed: %v", pubToken2.Error())
	}
	if err := probe.Login(ctx, observerGW, obsSecret); err != nil {
		t.Fatalf("fresh TLS login for observer failed after target rotate: %v", err)
	}

	// 5. Rotate replay with same key: returns metadata only, execute=false, no password redisclosure
	rotReplayResp, err := svc.Rotate(ctx, adminUID, MutationInput{
		GatewayID:      targetGW,
		IdempotencyKey: targetRotateKey,
		Action:         ActionRotate,
	})
	if err != nil {
		t.Fatalf("rotate replay failed: %v", err)
	}
	if rotReplayResp.Secret != nil || rotReplayResp.SecretReturned {
		t.Fatal("rotate replay unexpectedly returned secret")
	}
	if rotReplayResp.Metadata.CredentialVersion != 2 {
		t.Fatalf("expected replay credential version 2, got %d", rotReplayResp.Metadata.CredentialVersion)
	}
	// Assert executor was NOT called on idempotent replay
	if got := wrappedExecutor.ExecuteCount(); got != expectedExecCount {
		t.Fatalf("executor was called during idempotent rotate replay (count: %d, want: %d)", got, expectedExecCount)
	}

	// -------------------------------------------------------------------------
	// REVOKE TARGET: Public qualified revoke with VerifyRevoked
	// -------------------------------------------------------------------------
	targetRevokeKey := uuid.New()

	// Connect active session with new secret before revoking
	targetClientRotated, targetRotatedEOF, err := connectPahoSession(ctx, input.BrokerURL, tlsCfg, targetGW, targetNewSecret, 3*time.Second)
	if err != nil {
		t.Fatalf("failed to connect rotated target session: %v", err)
	}
	defer targetClientRotated.Disconnect(0)

	revResp, err := svc.Revoke(ctx, adminUID, MutationInput{
		GatewayID:      targetGW,
		IdempotencyKey: targetRevokeKey,
		Action:         ActionRevoke,
	})
	if err != nil {
		t.Fatalf("revoke target failed: %v", err)
	}
	expectedExecCount++ // revoke executed (4)
	if got := wrappedExecutor.ExecuteCount(); got != expectedExecCount {
		t.Fatalf("expected executor call count %d after revoke, got %d", expectedExecCount, got)
	}

	// 1. NO returned secret (Revoke returns MutationResult which has no Secret field)
	if revResp.SecretReturned {
		t.Fatal("revoke unexpectedly reported secret returned")
	}

	// 2. SAME credential version
	if revResp.Metadata.CredentialVersion != 2 {
		t.Fatalf("expected version 2 to remain on revoke, got %d", revResp.Metadata.CredentialVersion)
	}
	if revResp.Metadata.Status != CredentialRevoked {
		t.Fatalf("expected status %q, got %q", CredentialRevoked, revResp.Metadata.Status)
	}

	// 3. Target fresh connect explicit auth denial
	if err := probe.Rejected(ctx, targetGW, targetNewSecret); err != nil {
		t.Fatalf("probe rejected target with rotated secret after revoke failed: %v", err)
	}

	// 4. Active target session closes (real transport EOF / reset)
	select {
	case <-targetRotatedEOF:
		// Success: active session observed connection reset/drop
	case <-time.After(5 * time.Second):
		t.Fatal("active target session did not observe EOF after revoke within 5s")
	}

	// 5. P1 VerifyRevoked reader success directly
	if err := snapReader.VerifyRevoked(ctx, targetGW); err != nil {
		t.Fatalf("snapReader.VerifyRevoked failed directly: %v", err)
	}

	// 6. DB checks: verify event and metadata via REAL repo methods & journal columns
	// Using ResolveCommitAmbiguity on the revoke operation
	revOp, err := repo.ResolveCommitAmbiguity(ctx, revResp.OperationID)
	if err != nil {
		t.Fatalf("repo.ResolveCommitAmbiguity failed for revoke operation %s: %v", revResp.OperationID, err)
	}
	if revOp.Status != OperationSucceeded {
		t.Fatalf("expected revoke event status %q, got %q", OperationSucceeded, revOp.Status)
	}
	if revOp.Action != ActionRevoke {
		t.Fatalf("expected revoke event action %q, got %q", ActionRevoke, revOp.Action)
	}
	if revOp.Phase != PhaseFinalize {
		t.Fatalf("expected revoke event phase %q, got %q", PhaseFinalize, revOp.Phase)
	}
	if !revOp.Evidence.RAMApplied {
		t.Fatal("expected revoke event RAMApplied == true")
	}
	if !revOp.Evidence.SnapshotObserved {
		t.Fatal("expected revoke event SnapshotObserved == true")
	}
	if revOp.CredentialVersion != 2 {
		t.Fatalf("expected revoke event credential version 2, got %d", revOp.CredentialVersion)
	}

	// Using GetMetadata on the target gateway
	targetMeta, err := repo.GetMetadata(ctx, targetGW)
	if err != nil {
		t.Fatalf("repo.GetMetadata failed for target %s: %v", targetGW, err)
	}
	if targetMeta.Status != CredentialRevoked {
		t.Fatalf("expected target metadata status %q, got %q", CredentialRevoked, targetMeta.Status)
	}
	if targetMeta.CredentialVersion != 2 {
		t.Fatalf("expected target metadata credential version 2, got %d", targetMeta.CredentialVersion)
	}
	if targetMeta.LastOperationID != revResp.OperationID {
		t.Fatalf("expected target metadata LastOperationID %s, got %s", revResp.OperationID, targetMeta.LastOperationID)
	}
	if targetMeta.LastErrorCode != nil {
		t.Fatalf("unexpected target metadata last_error_code: %v", *targetMeta.LastErrorCode)
	}

	// Assert no secret material in database tables
	var leakedInEvents bool
	err = pool.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM public.gateway_mqtt_credential_events
			WHERE gateway_id = $1 AND (
				strpos(coalesce(error_code,''), $2) > 0 OR
				strpos(coalesce(error_code,''), $3) > 0
			)
		)`, targetGW, targetNewSecret, targetInitialSecret).Scan(&leakedInEvents)
	if err != nil {
		t.Fatalf("failed to query credential events for secret leakage: %v", err)
	}
	if leakedInEvents {
		t.Fatal("found secret data in gateway_mqtt_credential_events!")
	}

	// 7. Revoke replay with same key: returns metadata only, no error
	revReplayResp, err := svc.Revoke(ctx, adminUID, MutationInput{
		GatewayID:      targetGW,
		IdempotencyKey: targetRevokeKey,
		Action:         ActionRevoke,
	})
	if err != nil {
		t.Fatalf("revoke replay failed: %v", err)
	}
	if revReplayResp.SecretReturned {
		t.Fatal("revoke replay unexpectedly reported secret returned")
	}
	if revReplayResp.Metadata.Status != CredentialRevoked || revReplayResp.Metadata.CredentialVersion != 2 {
		t.Fatalf("unexpected metadata in revoke replay: %+v", revReplayResp.Metadata)
	}
	// Assert executor was NOT called on idempotent replay
	if got := wrappedExecutor.ExecuteCount(); got != expectedExecCount {
		t.Fatalf("executor was called during idempotent revoke replay (count: %d, want: %d)", got, expectedExecCount)
	}

	// 8. Observer isolation proof:
	// Existing observer session remains open and publishes QoS1 to own telemetry receiving PUBACK.
	// Fresh TLS login for observer continues to succeed.
	pubToken3 := obsClient.Publish(obsTelemetryTopic, 1, false, "obs-test-payload-3")
	if !pubToken3.WaitTimeout(3*time.Second) || pubToken3.Error() != nil {
		t.Fatalf("observer QoS1 publish own telemetry after target revoke failed: %v", pubToken3.Error())
	}
	if err := probe.Login(ctx, observerGW, obsSecret); err != nil {
		t.Fatalf("fresh TLS login for observer failed after target revoke: %v", err)
	}
}
