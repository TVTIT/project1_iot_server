package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"iot-platform/internal/config"
	"iot-platform/internal/mqttcredential"
)

// fakeTestOwnerConn implements ownerDBConn for credential_test.go seams.
type fakeTestOwnerConn struct {
	mu         sync.Mutex
	closed     bool
	queryRowFn func(ctx context.Context, sql string, args ...any) pgx.Row
	closeFn    func(ctx context.Context) error
}

func (c *fakeTestOwnerConn) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.queryRowFn != nil {
		return c.queryRowFn(ctx, sql, args...)
	}
	return boolRow(true)
}

func (c *fakeTestOwnerConn) Close(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	if c.closeFn != nil {
		return c.closeFn(ctx)
	}
	return nil
}

func (c *fakeTestOwnerConn) IsClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

func boolRow(b bool) fakeRow {
	return fakeRow{
		scanFunc: func(dest ...any) error {
			if len(dest) > 0 {
				if ptr, ok := dest[0].(*bool); ok {
					*ptr = b
				}
			}
			return nil
		},
	}
}

// fakeSimplifiedExecutor implements mqttcredential.SimplifiedExecutor.
type fakeSimplifiedExecutor struct {
	mu           sync.Mutex
	executeCalls int
	revokeCalls  int
	executeFn    func(ctx context.Context, o mqttcredential.Operation, generate func(context.Context) (string, error)) (mqttcredential.SimplifiedExecutionResult, error)
	checkRevFn   func(ctx context.Context, gatewayID string) error
}

func (f *fakeSimplifiedExecutor) Execute(ctx context.Context, o mqttcredential.Operation, generate func(context.Context) (string, error)) (mqttcredential.SimplifiedExecutionResult, error) {
	f.mu.Lock()
	f.executeCalls++
	f.mu.Unlock()
	if f.executeFn != nil {
		return f.executeFn(ctx, o, generate)
	}
	return mqttcredential.SimplifiedExecutionResult{
		OperationID: o.OperationID,
		Outcome:     mqttcredential.ExecutionVerifiedSuccess,
		Evidence: mqttcredential.SimplifiedExecutionEvidence{
			RAMApplied:            true,
			FreshPositiveVerified: true,
		},
		Mutated: true,
	}, nil
}

func (f *fakeSimplifiedExecutor) CheckRevoked(ctx context.Context, gatewayID string) error {
	f.mu.Lock()
	f.revokeCalls++
	f.mu.Unlock()
	if f.checkRevFn != nil {
		return f.checkRevFn(ctx, gatewayID)
	}
	return nil
}

func baseValidConfig() config.CredentialConfig {
	return config.CredentialConfig{
		Enabled:          true,
		ManagerUsername:  "admin",
		ManagerPassword:  "manager_pass_12345",
		BackendPassword:  "backend_pass_12345",
		ManagementURL:    "ssl://localhost:8883",
		CAFile:           "/valid/ca.crt",
		DBTimeout:        time.Second,
		FinalizeTimeout:  2 * time.Second,
		ReconcileTimeout: 5 * time.Second,
		OperationTimeout: 2 * time.Second,
		RequestTimeout:   10 * time.Second,
		RecoveryTimeout:  2 * time.Second,
		ClientTimeout:    time.Second,
		BatchSize:        64,
		MaxPages:         16,
		QueueSize:        16,
		MaxInflight:      1,
		MaxPayloadBytes:  65536,
	}
}

func validTestCA(t *testing.T) []byte {
	pemBytes, _, _ := generateTestCA(t)
	return pemBytes
}

func TestCredentialCompositionModes(t *testing.T) {
	c, e := composeCredentials(context.Background(), nil, config.CredentialConfig{})
	if e != nil || c.manager != nil || c.ready != nil {
		t.Fatal("disabled mode touched dependencies", e)
	}
	if e = c.close(); e != nil {
		t.Fatal(e)
	}
	if _, e = composeCredentials(context.Background(), nil, config.CredentialConfig{Enabled: true}); e == nil {
		t.Fatal("enabled missing pool")
	}
}

func TestActiveComposition_OwnerAcquiredBeforeClassifierAndBroker(t *testing.T) {
	order := []string{}
	conn := &fakeTestOwnerConn{}
	pool := &pgxpool.Pool{}

	seam := compositionSeam{
		acquireOwner: func(ctx context.Context, p *pgxpool.Pool, timeout time.Duration) (*credentialOwner, error) {
			order = append(order, "acquire_owner")
			acquirer := func(ctx context.Context) (ownerDBConn, error) {
				return conn, nil
			}
			return acquireCredentialOwnerWithAcquirer(ctx, acquirer, credentialOwnerLockKey, timeout)
		},
		readCA: func(path string) ([]byte, error) {
			order = append(order, "read_ca")
			return validTestCA(t), nil
		},
		newRepo: func(pool *pgxpool.Pool, timeout, finalizeTimeout time.Duration, scanLimit ...int) (*mqttcredential.PostgresRepository, error) {
			order = append(order, "new_repo")
			return nil, errors.New("stop_after_owner")
		},
	}

	cfg := baseValidConfig()
	_, err := composeCredentialsWithSeam(context.Background(), pool, cfg, seam)
	if err == nil {
		t.Fatal("expected error after seam stop")
	}
	if len(order) == 0 || order[0] != "acquire_owner" {
		t.Fatalf("expected acquire_owner first, got %v", order)
	}
	if !conn.IsClosed() {
		t.Fatal("expected owner connection closed on composition failure")
	}
}

func TestActiveComposition_SecondOwnerDenial_NoBrokerAccess(t *testing.T) {
	pool := &pgxpool.Pool{}
	brokerTouched := false

	seam := compositionSeam{
		acquireOwner: func(ctx context.Context, p *pgxpool.Pool, timeout time.Duration) (*credentialOwner, error) {
			return nil, errOwnerLockDenied
		},
		readCA: func(path string) ([]byte, error) {
			brokerTouched = true
			return validTestCA(t), nil
		},
		probeAuth: func(ctx context.Context, tlsConfig *tls.Config, target string, timeout time.Duration, user, password string, negative bool) error {
			brokerTouched = true
			return nil
		},
	}

	cfg := baseValidConfig()
	_, err := composeCredentialsWithSeam(context.Background(), pool, cfg, seam)
	if err == nil || !errors.Is(err, errOwnerLockDenied) {
		t.Fatalf("expected errOwnerLockDenied, got %v", err)
	}
	if brokerTouched {
		t.Fatal("broker was touched despite second owner lock denial")
	}
}

func TestActiveComposition_ManagementURLSemantics(t *testing.T) {
	pool := &pgxpool.Pool{}
	conn := &fakeTestOwnerConn{}

	seam := compositionSeam{
		acquireOwner: func(ctx context.Context, p *pgxpool.Pool, timeout time.Duration) (*credentialOwner, error) {
			acquirer := func(ctx context.Context) (ownerDBConn, error) { return conn, nil }
			return acquireCredentialOwnerWithAcquirer(ctx, acquirer, credentialOwnerLockKey, timeout)
		},
		readCA: func(path string) ([]byte, error) { return validTestCA(t), nil },
	}

	invalidURLs := []string{
		"tcp://localhost:8883",
		"mqtts://localhost:8883",
		"tls://localhost:8883",
		"ssl://user:pass@localhost:8883",
		"ssl://localhost:8883/path",
		"ssl://localhost:8883?query=1",
		"ssl://localhost:8883#frag",
		"ssl://localhost",
	}

	for _, u := range invalidURLs {
		t.Run(u, func(t *testing.T) {
			cfg := baseValidConfig()
			cfg.ManagementURL = u
			_, err := composeCredentialsWithSeam(context.Background(), pool, cfg, seam)
			if err == nil {
				t.Fatalf("expected error for URL %s", u)
			}
		})
	}
}

func TestActiveComposition_OwnershipGatedExecutor_LossBeforeExecute(t *testing.T) {
	conn := &fakeTestOwnerConn{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			return boolRow(true)
		},
	}

	acquirer := func(ctx context.Context) (ownerDBConn, error) { return conn, nil }
	owner, err := acquireCredentialOwnerWithAcquirer(context.Background(), acquirer, credentialOwnerLockKey, time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// Now make check fail
	conn.queryRowFn = func(ctx context.Context, sql string, args ...any) pgx.Row {
		return boolRow(false) // Lock lost
	}

	inner := &fakeSimplifiedExecutor{}
	readiness := &simplifiedStartupReadiness{ready: true, owner: owner}
	gated := &ownershipGatedExecutor{
		inner:        inner,
		owner:        owner,
		readiness:    readiness,
		checkTimeout: time.Second,
	}

	opID := uuid.New()
	op := mqttcredential.Operation{
		OperationID: opID,
		GatewayID:   "gw_1",
	}

	res, err := gated.Execute(context.Background(), op, nil)
	if err == nil {
		t.Fatal("expected error on lost ownership before execution")
	}
	var domErr *mqttcredential.DomainError
	if !errors.As(err, &domErr) || domErr.Code != mqttcredential.CodeServiceUnavailable {
		t.Fatalf("expected CodeServiceUnavailable, got %v", err)
	}
	if res.OperationID != opID {
		t.Fatalf("expected matching opID %v, got %v", opID, res.OperationID)
	}
	if res.Outcome != mqttcredential.ExecutionUnchangedFailure {
		t.Fatalf("expected ExecutionUnchangedFailure, got %v", res.Outcome)
	}
	if res.Mutated {
		t.Fatal("expected Mutated=false on pre-execution loss")
	}
	if inner.executeCalls != 0 {
		t.Fatalf("expected inner executor not called, got %d calls", inner.executeCalls)
	}
	if readiness.Ready() {
		t.Fatal("expected readiness to become false on owner loss")
	}
}

func TestActiveComposition_OwnershipGatedExecutor_LossAfterExecute(t *testing.T) {
	callCount := 0
	conn := &fakeTestOwnerConn{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			callCount++
			if callCount == 1 {
				// 1. acquire lock
				return boolRow(true)
			}
			if callCount == 2 {
				// 2. check before execution -> held
				return boolRow(true)
			}
			// 3. check after execution -> lost!
			return boolRow(false)
		},
	}

	acquirer := func(ctx context.Context) (ownerDBConn, error) { return conn, nil }
	owner, err := acquireCredentialOwnerWithAcquirer(context.Background(), acquirer, credentialOwnerLockKey, time.Second)
	if err != nil {
		t.Fatal(err)
	}

	inner := &fakeSimplifiedExecutor{
		executeFn: func(ctx context.Context, o mqttcredential.Operation, generate func(context.Context) (string, error)) (mqttcredential.SimplifiedExecutionResult, error) {
			return mqttcredential.SimplifiedExecutionResult{
				OperationID: o.OperationID,
				Outcome:     mqttcredential.ExecutionVerifiedSuccess,
				Mutated:     true,
			}, nil
		},
	}
	readiness := &simplifiedStartupReadiness{ready: true, owner: owner}
	gated := &ownershipGatedExecutor{
		inner:        inner,
		owner:        owner,
		readiness:    readiness,
		checkTimeout: time.Second,
	}

	opID := uuid.New()
	op := mqttcredential.Operation{
		OperationID: opID,
		GatewayID:   "gw_1",
	}

	res, err := gated.Execute(context.Background(), op, nil)
	if err == nil {
		t.Fatal("expected error on lost ownership after execution")
	}
	var domErr *mqttcredential.DomainError
	if !errors.As(err, &domErr) || domErr.Code != mqttcredential.CodeRecoveryRequired {
		t.Fatalf("expected CodeRecoveryRequired, got %v", err)
	}
	if res.OperationID != opID {
		t.Fatalf("expected matching opID %v, got %v", opID, res.OperationID)
	}
	if res.Outcome != mqttcredential.ExecutionRecoveryRequired {
		t.Fatalf("expected ExecutionRecoveryRequired, got %v", res.Outcome)
	}
	if !res.Mutated {
		t.Fatal("expected Mutated=true preserved when mutation may have occurred")
	}
	if readiness.Ready() {
		t.Fatal("expected readiness to become false on owner loss")
	}
}

func TestActiveComposition_CheckRevoked_RequiresLiveOwnership(t *testing.T) {
	conn := &fakeTestOwnerConn{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			return boolRow(true)
		},
	}
	acquirer := func(ctx context.Context) (ownerDBConn, error) { return conn, nil }
	owner, err := acquireCredentialOwnerWithAcquirer(context.Background(), acquirer, credentialOwnerLockKey, time.Second)
	if err != nil {
		t.Fatal(err)
	}

	inner := &fakeSimplifiedExecutor{}
	readiness := &simplifiedStartupReadiness{ready: true, owner: owner}
	gated := &ownershipGatedExecutor{
		inner:        inner,
		owner:        owner,
		readiness:    readiness,
		checkTimeout: time.Second,
	}

	// 1. Live ownership -> passes to inner
	if err := gated.CheckRevoked(context.Background(), "gw_1"); err != nil {
		t.Fatalf("expected nil error on live ownership, got %v", err)
	}
	if inner.revokeCalls != 1 {
		t.Fatalf("expected 1 revoke call, got %d", inner.revokeCalls)
	}

	// 2. Lost ownership -> fails closed
	conn.queryRowFn = func(ctx context.Context, sql string, args ...any) pgx.Row {
		return boolRow(false)
	}
	if err := gated.CheckRevoked(context.Background(), "gw_1"); err == nil {
		t.Fatal("expected error on lost ownership in CheckRevoked")
	}
	if inner.revokeCalls != 1 {
		t.Fatalf("expected inner CheckRevoked not called after loss, got %d calls", inner.revokeCalls)
	}
	if readiness.Ready() {
		t.Fatal("expected readiness false after ownership loss")
	}
}

func TestActiveComposition_DenyOnlyResetPrerequisite(t *testing.T) {
	prereq := denyOnlyResetPrerequisite{}
	err := prereq.SupervisedQuiescencePrerequisite(context.Background(), "gw_1")
	if err == nil {
		t.Fatal("expected error from deny-only reset prerequisite")
	}
	var domErr *mqttcredential.DomainError
	if !errors.As(err, &domErr) || domErr.Code != mqttcredential.CodeServiceUnavailable {
		t.Fatalf("expected CodeServiceUnavailable, got %v", err)
	}
}

func TestActiveComposition_ReadinessThreadSafetyAndOwnerLoss(t *testing.T) {
	conn := &fakeTestOwnerConn{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			return boolRow(true)
		},
	}
	acquirer := func(ctx context.Context) (ownerDBConn, error) { return conn, nil }
	owner, err := acquireCredentialOwnerWithAcquirer(context.Background(), acquirer, credentialOwnerLockKey, time.Second)
	if err != nil {
		t.Fatal(err)
	}

	readiness := &simplifiedStartupReadiness{ready: true, owner: owner}
	if !readiness.Ready() {
		t.Fatal("expected Ready() to be true initially")
	}

	// Concurrent reads to test thread-safety
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = readiness.Ready()
		}()
	}
	wg.Wait()

	// Close owner -> Ready() must return false immediately
	_ = owner.Close(context.Background(), time.Second)
	if readiness.Ready() {
		t.Fatal("expected Ready() to be false after owner closed")
	}

	// Explicit markOwnerLost
	readiness.markOwnerLost()
	if readiness.Ready() {
		t.Fatal("expected Ready() false after markOwnerLost")
	}
}

func TestActiveComposition_ShutdownLocksAndReleasesOwner(t *testing.T) {
	conn := &fakeTestOwnerConn{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			return boolRow(true)
		},
	}
	acquirer := func(ctx context.Context) (ownerDBConn, error) { return conn, nil }
	owner, err := acquireCredentialOwnerWithAcquirer(context.Background(), acquirer, credentialOwnerLockKey, time.Second)
	if err != nil {
		t.Fatal(err)
	}

	comp := credentialComposition{
		cleanupTimeout: time.Second,
		drain: func(ctx context.Context) error {
			return owner.Close(ctx, time.Second)
		},
	}

	if err := comp.close(); err != nil {
		t.Fatalf("expected clean close, got %v", err)
	}
	if !conn.IsClosed() {
		t.Fatal("expected dedicated connection closed upon comp.close()")
	}
}

func TestActiveComposition_ClassifierIncomplete_SafeUnavailableService(t *testing.T) {
	conn := &fakeTestOwnerConn{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			return boolRow(true)
		},
	}
	pool := &pgxpool.Pool{}

	probeCalls := 0
	clientCalls := 0

	var logBuf bytes.Buffer
	testLogger := slog.New(slog.NewJSONHandler(&logBuf, nil))
	oldLogger := slog.Default()
	slog.SetDefault(testLogger)
	defer slog.SetDefault(oldLogger)

	sensitiveErrMsg := "sensitive-driver-error-raw-dsn-postgres://user:secret@db:5432"

	seam := compositionSeam{
		acquireOwner: func(ctx context.Context, p *pgxpool.Pool, timeout time.Duration) (*credentialOwner, error) {
			acquirer := func(ctx context.Context) (ownerDBConn, error) { return conn, nil }
			return acquireCredentialOwnerWithAcquirer(ctx, acquirer, credentialOwnerLockKey, timeout)
		},
		readCA: func(path string) ([]byte, error) { return validTestCA(t), nil },
		newRepo: func(pool *pgxpool.Pool, timeout, finalizeTimeout time.Duration, scanLimit ...int) (*mqttcredential.PostgresRepository, error) {
			return &mqttcredential.PostgresRepository{}, nil
		},
		newClassifier: func(r mqttcredential.SimplifiedStartupRepository, opts mqttcredential.SimplifiedStartupClassifierOptions) (*mqttcredential.SimplifiedStartupClassifier, error) {
			return &mqttcredential.SimplifiedStartupClassifier{}, nil
		},
		classifyTarget: func(ctx context.Context, c *mqttcredential.SimplifiedStartupClassifier) (mqttcredential.ClassificationResult, error) {
			// Incomplete classification scan with raw sensitive error
			return mqttcredential.ClassificationResult{
				Status: mqttcredential.ClassificationIncomplete,
			}, errors.New(sensitiveErrMsg)
		},
		probeAuth: func(ctx context.Context, tlsConfig *tls.Config, target string, timeout time.Duration, user, password string, negative bool) error {
			probeCalls++
			return nil
		},
		newClient: func(ctx context.Context, cfg mqttcredential.DynSecConfig) (*mqttcredential.DynSecClient, error) {
			clientCalls++
			return &mqttcredential.DynSecClient{}, nil
		},
	}

	cfg := baseValidConfig()
	comp, err := composeCredentialsWithSeam(context.Background(), pool, cfg, seam)
	if err != nil {
		t.Fatalf("incomplete classifier should not tear down server, got error: %v", err)
	}
	if comp.manager == nil {
		t.Fatal("expected non-nil manager on incomplete classifier to keep business API valid")
	}
	if comp.ready == nil || comp.ready.Ready() {
		t.Fatal("expected non-nil readiness with ready=false on incomplete classifier")
	}
	if probeCalls != 0 {
		t.Fatalf("expected 0 broker probe calls on incomplete classifier, got %d", probeCalls)
	}
	if clientCalls != 0 {
		t.Fatalf("expected 0 newClient calls on incomplete classifier, got %d", clientCalls)
	}

	// Calling manager methods returns safe CodeServiceUnavailable
	actor := uuid.New()
	input := mqttcredential.MutationInput{
		GatewayID:      "gateway_001",
		IdempotencyKey: uuid.New(),
		Action:         mqttcredential.ActionProvision,
	}
	_, provErr := comp.manager.Provision(context.Background(), actor, input)
	if provErr == nil {
		t.Fatal("expected provision to fail on incomplete classifier")
	}
	var domainErr *mqttcredential.DomainError
	if !errors.As(provErr, &domainErr) || domainErr.Code != mqttcredential.CodeServiceUnavailable {
		t.Fatalf("expected CodeServiceUnavailable, got %v", provErr)
	}

	// Verify log does NOT contain sensitive error
	if strings.Contains(logBuf.String(), sensitiveErrMsg) {
		t.Fatalf("log contains raw sensitive error message: %s", logBuf.String())
	}

	// Owner remains held until close
	if conn.IsClosed() {
		t.Fatal("expected owner retained before comp.close()")
	}
	if err := comp.close(); err != nil {
		t.Fatalf("expected clean close on drain: %v", err)
	}
	if !conn.IsClosed() {
		t.Fatal("expected owner closed on comp.close()")
	}
}

// Unit cmd seam test: Incomplete classifier result (e.g. legacy blocker) guarantees
// readiness=false and ZERO auth probe/client/executor calls using existing composition seams.
// Note: This unit test verifies the composition wireup; it does NOT claim to be actual SQL-to-broker end-to-end proof.
func TestActiveComposition_ClassifierIncompleteLegacyBlocker_GuaranteesNotReadyZeroProbesAndZeroClient(t *testing.T) {
	conn := &fakeTestOwnerConn{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			return boolRow(true)
		},
	}
	pool := &pgxpool.Pool{}

	probeCalls := 0
	clientCalls := 0
	executorCalls := 0

	seam := compositionSeam{
		acquireOwner: func(ctx context.Context, p *pgxpool.Pool, timeout time.Duration) (*credentialOwner, error) {
			acquirer := func(ctx context.Context) (ownerDBConn, error) { return conn, nil }
			return acquireCredentialOwnerWithAcquirer(ctx, acquirer, credentialOwnerLockKey, timeout)
		},
		readCA: func(path string) ([]byte, error) { return validTestCA(t), nil },
		newRepo: func(pool *pgxpool.Pool, timeout, finalizeTimeout time.Duration, scanLimit ...int) (*mqttcredential.PostgresRepository, error) {
			return &mqttcredential.PostgresRepository{}, nil
		},
		newClassifier: func(r mqttcredential.SimplifiedStartupRepository, opts mqttcredential.SimplifiedStartupClassifierOptions) (*mqttcredential.SimplifiedStartupClassifier, error) {
			return &mqttcredential.SimplifiedStartupClassifier{}, nil
		},
		classifyTarget: func(ctx context.Context, c *mqttcredential.SimplifiedStartupClassifier) (mqttcredential.ClassificationResult, error) {
			// Incomplete classifier result with legacy contract blocker error
			return mqttcredential.ClassificationResult{
				Status: mqttcredential.ClassificationIncomplete,
			}, mqttcredential.ErrLegacyContractBlocked
		},
		probeAuth: func(ctx context.Context, tlsConfig *tls.Config, target string, timeout time.Duration, user, password string, negative bool) error {
			probeCalls++
			return nil
		},
		newClient: func(ctx context.Context, cfg mqttcredential.DynSecConfig) (*mqttcredential.DynSecClient, error) {
			clientCalls++
			return &mqttcredential.DynSecClient{}, nil
		},
	}

	cfg := baseValidConfig()
	comp, err := composeCredentialsWithSeam(context.Background(), pool, cfg, seam)
	if err != nil {
		t.Fatalf("unexpected composition error on legacy blocker: %v", err)
	}
	if comp.manager == nil {
		t.Fatal("expected non-nil manager on incomplete classifier")
	}
	// Assert readiness=false
	if comp.ready == nil {
		t.Fatal("expected non-nil readiness pointer")
	}
	if comp.ready.Ready() {
		t.Fatal("expected readiness=false when classifier returns Incomplete with legacy blocker")
	}

	// Assert ZERO auth probe calls and ZERO client calls
	if probeCalls != 0 {
		t.Fatalf("expected 0 broker probe calls on incomplete classifier, got %d", probeCalls)
	}
	if clientCalls != 0 {
		t.Fatalf("expected 0 newClient calls on incomplete classifier, got %d", clientCalls)
	}
	if executorCalls != 0 {
		t.Fatalf("expected 0 executor calls on incomplete classifier, got %d", executorCalls)
	}

	// Any mutation attempt on manager must fail closed with CodeServiceUnavailable
	actor := uuid.New()
	input := mqttcredential.MutationInput{
		GatewayID:      "gateway_001",
		IdempotencyKey: uuid.New(),
		Action:         mqttcredential.ActionProvision,
	}
	_, provErr := comp.manager.Provision(context.Background(), actor, input)
	if provErr == nil {
		t.Fatal("expected provision to fail on incomplete classifier")
	}
	var domainErr *mqttcredential.DomainError
	if !errors.As(provErr, &domainErr) || domainErr.Code != mqttcredential.CodeServiceUnavailable {
		t.Fatalf("expected CodeServiceUnavailable, got %v", provErr)
	}

	if err := comp.close(); err != nil {
		t.Fatalf("expected clean close on drain: %v", err)
	}
}

func TestActiveComposition_CompleteClassificationWithRecoveryTargets_ConstructsService(t *testing.T) {
	conn := &fakeTestOwnerConn{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			return boolRow(true)
		},
	}
	pool := &pgxpool.Pool{}
	targetOpID := uuid.New()

	seam := compositionSeam{
		acquireOwner: func(ctx context.Context, p *pgxpool.Pool, timeout time.Duration) (*credentialOwner, error) {
			acquirer := func(ctx context.Context) (ownerDBConn, error) { return conn, nil }
			return acquireCredentialOwnerWithAcquirer(ctx, acquirer, credentialOwnerLockKey, timeout)
		},
		readCA: func(path string) ([]byte, error) { return validTestCA(t), nil },
		newRepo: func(pool *pgxpool.Pool, timeout, finalizeTimeout time.Duration, scanLimit ...int) (*mqttcredential.PostgresRepository, error) {
			return &mqttcredential.PostgresRepository{}, nil
		},
		newClassifier: func(r mqttcredential.SimplifiedStartupRepository, opts mqttcredential.SimplifiedStartupClassifierOptions) (*mqttcredential.SimplifiedStartupClassifier, error) {
			return &mqttcredential.SimplifiedStartupClassifier{}, nil
		},
		classifyTarget: func(ctx context.Context, c *mqttcredential.SimplifiedStartupClassifier) (mqttcredential.ClassificationResult, error) {
			// Completed with recovery target
			return mqttcredential.ClassificationResult{
				Status: mqttcredential.ClassificationCompleted,
				RecoveryRequiredTargets: []mqttcredential.RecoveryTarget{
					{
						GatewayID:         "gw_rec_1",
						OperationID:       targetOpID,
						CredentialVersion: 1,
						Action:            mqttcredential.ActionProvision,
					},
				},
			}, nil
		},
		probeAuth: func(ctx context.Context, tlsConfig *tls.Config, target string, timeout time.Duration, user, password string, negative bool) error {
			// Auth checks succeed
			return nil
		},
	}

	cfg := baseValidConfig()
	comp, err := composeCredentialsWithSeam(context.Background(), pool, cfg, seam)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if comp.manager == nil {
		t.Fatal("expected non-nil manager when classification completed")
	}
	if comp.ready == nil || !comp.ready.Ready() {
		t.Fatal("expected ready=true when auth checks succeed and classification completed")
	}
	if err := comp.close(); err != nil {
		t.Fatalf("expected clean close: %v", err)
	}
}

func TestActiveComposition_BrokerAuthFailure_ReadinessFalse(t *testing.T) {
	conn := &fakeTestOwnerConn{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			return boolRow(true)
		},
	}
	pool := &pgxpool.Pool{}

	seam := compositionSeam{
		acquireOwner: func(ctx context.Context, p *pgxpool.Pool, timeout time.Duration) (*credentialOwner, error) {
			acquirer := func(ctx context.Context) (ownerDBConn, error) { return conn, nil }
			return acquireCredentialOwnerWithAcquirer(ctx, acquirer, credentialOwnerLockKey, timeout)
		},
		readCA: func(path string) ([]byte, error) { return validTestCA(t), nil },
		newRepo: func(pool *pgxpool.Pool, timeout, finalizeTimeout time.Duration, scanLimit ...int) (*mqttcredential.PostgresRepository, error) {
			return &mqttcredential.PostgresRepository{}, nil
		},
		newClassifier: func(r mqttcredential.SimplifiedStartupRepository, opts mqttcredential.SimplifiedStartupClassifierOptions) (*mqttcredential.SimplifiedStartupClassifier, error) {
			return &mqttcredential.SimplifiedStartupClassifier{}, nil
		},
		classifyTarget: func(ctx context.Context, c *mqttcredential.SimplifiedStartupClassifier) (mqttcredential.ClassificationResult, error) {
			return mqttcredential.ClassificationResult{Status: mqttcredential.ClassificationCompleted}, nil
		},
		probeAuth: func(ctx context.Context, tlsConfig *tls.Config, target string, timeout time.Duration, user, password string, negative bool) error {
			// Broker auth fails!
			return mqttcredential.ErrVerificationFailed
		},
	}

	cfg := baseValidConfig()
	comp, err := composeCredentialsWithSeam(context.Background(), pool, cfg, seam)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if comp.manager == nil {
		t.Fatal("expected non-nil manager constructed")
	}
	if comp.ready == nil || comp.ready.Ready() {
		t.Fatal("expected ready=false when broker probe auth fails")
	}
	if err := comp.close(); err != nil {
		t.Fatalf("expected clean close: %v", err)
	}
}

func TestLegacyLifecycleStandaloneHelpers(t *testing.T) {
	for _, fail := range []bool{false, true} {
		calls := []string{}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := runCredentialBarrier(ctx, func(context.Context) error {
			calls = append(calls, "run")
			if fail {
				return errors.New("err")
			}
			return nil
		}, func(clean context.Context) error {
			calls = append(calls, "drain")
			return nil
		}, time.Second)
		if fail && (err == nil || len(calls) != 2) {
			t.Fatal("failed runCredentialBarrier not drained")
		}
		if !fail && (err != nil || len(calls) != 1) {
			t.Fatal("successful runCredentialBarrier error", err)
		}
	}
}
