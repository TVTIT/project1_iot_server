package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"iot-platform/internal/config"
	"iot-platform/internal/gateway"
	"iot-platform/internal/httpserver"
	"iot-platform/internal/mqttcredential"
)

type credentialComposition struct {
	manager        httpserver.CredentialManager
	ready          httpserver.CredentialStartupReadiness
	drain          func(context.Context) error
	cleanupTimeout time.Duration
}

func (c credentialComposition) close() error {
	ctx, cancel := context.WithTimeout(context.Background(), c.cleanupTimeout)
	defer cancel()
	return c.closeContext(ctx)
}

// closeContext drains credential work using the caller's deadline so a shared
// shutdown budget covers the credential close together with HTTP and ingress.
func (c credentialComposition) closeContext(ctx context.Context) error {
	if c.drain == nil {
		return nil
	}
	return c.drain(ctx)
}

// simplifiedStartupReadiness implements httpserver.CredentialStartupReadiness.
// Thread-safe and true only after complete classifier scan and verified management/backend
// fresh authentication checks.
// If the owner is lost, Ready() permanently becomes false (fails closed).
// Ready() indicates startup prerequisite health, while subsequent executor calls verify
// live ownership per operation.
type simplifiedStartupReadiness struct {
	mu        sync.RWMutex
	ready     bool
	owner     *credentialOwner
	ownerLoss bool
}

func (r *simplifiedStartupReadiness) Ready() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if !r.ready || r.ownerLoss {
		return false
	}
	if r.owner != nil {
		r.owner.mu.Lock()
		unavail := r.owner.unavailable || r.owner.closed
		r.owner.mu.Unlock()
		if unavail {
			return false
		}
	}
	return true
}

func (r *simplifiedStartupReadiness) markOwnerLost() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ownerLoss = true
	r.ready = false
}

// denyOnlyResetPrerequisite satisfies mqttcredential.ResetPrerequisite by failing closed with CodeServiceUnavailable.
// This is intentionally disabled production reset until a full operator-supervised reset workflow is built.
type denyOnlyResetPrerequisite struct{}

func (denyOnlyResetPrerequisite) SupervisedQuiescencePrerequisite(_ context.Context, _ string) error {
	return &mqttcredential.DomainError{Code: mqttcredential.CodeServiceUnavailable}
}

// ownershipGatedExecutor wraps a mqttcredential.SimplifiedExecutor and enforces dedicated PostgreSQL
// advisory lock ownership before and after every native execution and before revocation verification.
type ownershipGatedExecutor struct {
	mu           sync.Mutex
	inner        mqttcredential.SimplifiedExecutor
	owner        *credentialOwner
	readiness    *simplifiedStartupReadiness
	checkTimeout time.Duration
}

func (e *ownershipGatedExecutor) Execute(ctx context.Context, o mqttcredential.Operation, generate func(context.Context) (string, error)) (mqttcredential.SimplifiedExecutionResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	res := mqttcredential.SimplifiedExecutionResult{
		OperationID: o.OperationID,
		Outcome:     mqttcredential.ExecutionUnchangedFailure,
	}

	// 1. Gate BEFORE native execution
	if err := e.owner.CheckOwnership(ctx, e.checkTimeout); err != nil {
		if e.readiness != nil {
			e.readiness.markOwnerLost()
		}
		// Loss before execution => zero client creation, zero generator invocation, zero mutation.
		// Unchanged-failure matching operation ID.
		return res, &mqttcredential.DomainError{Code: mqttcredential.CodeServiceUnavailable}
	}

	// 2. Invoke inner executor
	innerRes, innerErr := e.inner.Execute(ctx, o, generate)

	// 3. Gate AFTER native execution
	if checkErr := e.owner.CheckOwnership(ctx, e.checkTimeout); checkErr != nil {
		if e.readiness != nil {
			e.readiness.markOwnerLost()
		}
		// Loss after attempted native execution => explicit recovery-required/unknown.
		// Preserve Mutated=true if mutation may have occurred (or if inner reported Mutated=true).
		// Zero secret disclosure.
		slog.Error("credential owner advisory lock lost during native operation; supervisor reset required",
			"operation_id", o.OperationID,
			"gateway_id", o.GatewayID,
			"mutated", innerRes.Mutated,
			"check_error", checkErr)
		return mqttcredential.SimplifiedExecutionResult{
			OperationID: o.OperationID,
			Outcome:     mqttcredential.ExecutionRecoveryRequired,
			Mutated:     innerRes.Mutated || true,
		}, &mqttcredential.DomainError{Code: mqttcredential.CodeRecoveryRequired}
	}

	return innerRes, innerErr
}

func (e *ownershipGatedExecutor) CheckRevoked(ctx context.Context, gatewayID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if err := e.owner.CheckOwnership(ctx, e.checkTimeout); err != nil {
		if e.readiness != nil {
			e.readiness.markOwnerLost()
		}
		return &mqttcredential.DomainError{Code: mqttcredential.CodeRecoveryRequired}
	}

	return e.inner.CheckRevoked(ctx, gatewayID)
}

// compositionSeam allows unit tests to inject mocks/fakes without real database/network.
type compositionSeam struct {
	acquireOwner   func(ctx context.Context, pool *pgxpool.Pool, timeout time.Duration) (*credentialOwner, error)
	newRepo        func(pool *pgxpool.Pool, timeout, finalizeTimeout time.Duration, scanLimit ...int) (*mqttcredential.PostgresRepository, error)
	newClassifier  func(r mqttcredential.SimplifiedStartupRepository, opts mqttcredential.SimplifiedStartupClassifierOptions) (*mqttcredential.SimplifiedStartupClassifier, error)
	classifyTarget func(ctx context.Context, c *mqttcredential.SimplifiedStartupClassifier) (mqttcredential.ClassificationResult, error)
	readCA         func(path string) ([]byte, error)
	newClient      func(ctx context.Context, cfg mqttcredential.DynSecConfig) (*mqttcredential.DynSecClient, error)
	probeAuth      func(ctx context.Context, tlsConfig *tls.Config, target string, timeout time.Duration, user, password string, negative bool) error
	newExecutor    func(cfg mqttcredential.SimplifiedDynSecConfig) (mqttcredential.SimplifiedExecutor, error)
}

var defaultCompositionSeam = compositionSeam{
	acquireOwner: acquireCredentialOwner,
	newRepo:      mqttcredential.NewPostgresRepositoryWithFinalizationTimeout,
	newClassifier: func(r mqttcredential.SimplifiedStartupRepository, opts mqttcredential.SimplifiedStartupClassifierOptions) (*mqttcredential.SimplifiedStartupClassifier, error) {
		return mqttcredential.NewSimplifiedStartupClassifier(r, opts)
	},
	classifyTarget: func(ctx context.Context, c *mqttcredential.SimplifiedStartupClassifier) (mqttcredential.ClassificationResult, error) {
		return c.Classify(ctx)
	},
	readCA: func(path string) ([]byte, error) {
		st, err := os.Lstat(path)
		if err != nil || !st.Mode().IsRegular() || st.Size() > 1<<20 {
			return nil, fmt.Errorf("credential CA unavailable")
		}
		ca, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("credential CA unavailable")
		}
		return ca, nil
	},
	newClient: mqttcredential.NewDynSecClient,
	probeAuth: probePrivateCredential,
	newExecutor: func(cfg mqttcredential.SimplifiedDynSecConfig) (mqttcredential.SimplifiedExecutor, error) {
		return mqttcredential.NewSimplifiedDynSecExecutor(cfg)
	},
}

func validateManagementURLSemantics(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u == nil {
		return fmt.Errorf("credential management URL invalid")
	}
	if u.Scheme != "ssl" {
		return fmt.Errorf("credential management URL invalid")
	}
	if u.Hostname() == "" || u.Port() == "" {
		return fmt.Errorf("credential management URL invalid")
	}
	if u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("credential management URL invalid")
	}
	return nil
}

// composeCredentials wires the active simplified credential subsystem.
func composeCredentials(ctx context.Context, pool *pgxpool.Pool, c config.CredentialConfig) (credentialComposition, error) {
	return composeCredentialsWithSeam(ctx, pool, c, defaultCompositionSeam)
}

func composeCredentialsWithSeam(ctx context.Context, pool *pgxpool.Pool, c config.CredentialConfig, seam compositionSeam) (out credentialComposition, err error) {
	if !c.Enabled {
		return out, nil
	}
	if pool == nil {
		return out, fmt.Errorf("credential database required")
	}

	// 0. Validate required qualification and persistence reader prerequisites BEFORE
	// owner acquisition, broker probes, client factories, or mutation.
	if c.Qualification.IsZero() || !c.Qualification.Valid() {
		return out, fmt.Errorf("credential qualification required and must match qualified identity")
	}
	if !filepath.IsAbs(c.SnapshotPath) || filepath.Clean(c.SnapshotPath) != c.SnapshotPath {
		return out, fmt.Errorf("credential snapshot path must be a clean absolute path")
	}
	if c.MaxSnapshotBytes <= 0 || c.MaxSnapshotBytes > 64<<20 {
		return out, fmt.Errorf("credential max snapshot bytes invalid")
	}

	validateTarget := func(u string) bool {
		return gateway.ValidateGatewayID(u) == nil && u != c.ManagerUsername && u != mqttcredential.BackendUsername
	}
	validateRole := func(u, r string) bool {
		return validateTarget(u) && r == "gateway_"+u
	}

	snapshotReader, err := mqttcredential.NewDynSecSnapshotReader(mqttcredential.DynSecSnapshotConfig{
		Path:           c.SnapshotPath,
		WriterUID:      c.WriterUID,
		MaxBytes:       int64(c.MaxSnapshotBytes),
		ValidateTarget: validateTarget,
	})
	if err != nil {
		return out, fmt.Errorf("create credential snapshot reader: %w", err)
	}

	// 1. Acquire dedicated credentialOwner BEFORE classification or any broker access.
	// No second owner / fallback.
	ownerTimeout := c.DBTimeout
	if ownerTimeout <= 0 {
		ownerTimeout = 5 * time.Second
	}
	owner, err := seam.acquireOwner(ctx, pool, ownerTimeout)
	if err != nil {
		return out, fmt.Errorf("acquire credential owner: %w", err)
	}

	// Clean up owner if composition fails
	defer func() {
		if err != nil && owner != nil {
			closeCtx, cancel := context.WithTimeout(context.Background(), c.RecoveryTimeout)
			defer cancel()
			_ = owner.Close(closeCtx, c.RecoveryTimeout)
		}
	}()

	// 2. Validate URL with strict ssl-only/no-userinfo/path/query/fragment semantics.
	if err := validateManagementURLSemantics(c.ManagementURL); err != nil {
		return out, err
	}

	// 3. Read CA file and build verified TLS config.
	caPEM, err := seam.readCA(c.CAFile)
	if err != nil {
		return out, fmt.Errorf("credential CA unavailable: %w", err)
	}
	tlsConfig, targetAddr, err := buildVerifiedTLSConfig(caPEM, c.ManagementURL)
	if err != nil {
		return out, fmt.Errorf("credential TLS config invalid: %w", err)
	}

	// 4. Create PostgresRepository
	repo, err := seam.newRepo(pool, c.DBTimeout, c.FinalizeTimeout, c.BatchSize)
	if err != nil {
		return out, fmt.Errorf("create credential repository: %w", err)
	}

	// 5. Initialize SimplifiedStartupClassifier
	classifierOpts := mqttcredential.SimplifiedStartupClassifierOptions{
		Timeout:  c.ReconcileTimeout,
		PageSize: c.BatchSize,
		MaxPages: c.MaxPages,
	}
	classifier, err := seam.newClassifier(repo, classifierOpts)
	if err != nil {
		return out, fmt.Errorf("create startup classifier: %w", err)
	}

	// 6. Run classification scan
	classifyRes, classifyErr := seam.classifyTarget(ctx, classifier)
	classificationComplete := classifyErr == nil && classifyRes.Status == mqttcredential.ClassificationCompleted
	if !classificationComplete {
		// Unavailable/incomplete classification must NOT report ready or perform broker mutation.
		// Construct safe unavailable manager/readiness with the SAME simplified ProvisionService,
		// keeping business API composition valid while all mutations fail closed.
		slog.Warn("credential startup classification incomplete; credential manager unavailable",
			"status", classifyRes.Status)
	}

	// 7. Verify management and backend fresh authentication checks only if classification completed.
	protectedUsernames := []string{mqttcredential.BackendUsername, c.ManagerUsername}

	var managerAuthErr error
	var backendAuthErr error
	if classificationComplete {
		// Probe Manager fresh authentication
		managerAuthErr = seam.probeAuth(ctx, tlsConfig, targetAddr, c.ClientTimeout, c.ManagerUsername, c.ManagerPassword, false)
		// Probe Backend fresh authentication
		backendAuthErr = seam.probeAuth(ctx, tlsConfig, targetAddr, c.ClientTimeout, mqttcredential.BackendUsername, c.BackendPassword, false)
		if managerAuthErr != nil || backendAuthErr != nil {
			slog.Warn("credential broker probe auth failed at startup; credential manager not ready",
				"manager_err", managerAuthErr, "backend_err", backendAuthErr)
		}
	}

	readiness := &simplifiedStartupReadiness{
		ready: classificationComplete && managerAuthErr == nil && backendAuthErr == nil,
		owner: owner,
	}

	// 8. Build SimplifiedDynSecConfig & Executor
	freshClient := func(clientCtx context.Context) (mqttcredential.SimplifiedDynSecClient, error) {
		// Fresh client per operation, bounded one connection attempt, no mutation retry.
		dynCfg := mqttcredential.DynSecConfig{
			BrokerURL:          c.ManagementURL,
			CAPEM:              caPEM,
			ManagerUsername:    c.ManagerUsername,
			ManagerPassword:    c.ManagerPassword,
			ProtectedUsernames: protectedUsernames,
			Timeout:            c.ClientTimeout,
			MaxPayloadBytes:    c.MaxPayloadBytes,
			MaxInflight:        c.MaxInflight,
			QueueSize:          c.QueueSize,
			ValidateTarget:     validateTarget,
			ValidateRole:       validateRole,
		}
		return seam.newClient(clientCtx, dynCfg)
	}

	loginFunc := func(probeCtx context.Context, username, password string) error {
		return seam.probeAuth(probeCtx, tlsConfig, targetAddr, c.ClientTimeout, username, password, false)
	}
	rejectedFunc := func(probeCtx context.Context, username, password string) error {
		return seam.probeAuth(probeCtx, tlsConfig, targetAddr, c.ClientTimeout, username, password, true)
	}

	dynSecCfg := mqttcredential.SimplifiedDynSecConfig{
		NewClient:          freshClient,
		Login:              loginFunc,
		Rejected:           rejectedFunc,
		ProtectedUsernames: protectedUsernames,
		Timeout:            c.OperationTimeout,
		VerifyRevoked:      snapshotReader.VerifyRevoked,
		Qualification:      c.Qualification,
	}

	newExec := seam.newExecutor
	if newExec == nil {
		newExec = func(cfg mqttcredential.SimplifiedDynSecConfig) (mqttcredential.SimplifiedExecutor, error) {
			return mqttcredential.NewSimplifiedDynSecExecutor(cfg)
		}
	}
	rawExecutor, err := newExec(dynSecCfg)
	if err != nil {
		return out, fmt.Errorf("create simplified executor: %w", err)
	}

	// Wrap executor with PostgreSQL advisory lock ownership checks
	gatedExecutor := &ownershipGatedExecutor{
		inner:        rawExecutor,
		owner:        owner,
		readiness:    readiness,
		checkTimeout: c.DBTimeout,
	}

	// 9. Construct SimplifiedProvisionService
	serviceOpts := mqttcredential.SimplifiedProvisionServiceOptions{
		RecoveryTimeout: c.RecoveryTimeout,
		StartupReady:    readiness.Ready,
	}
	service, err := mqttcredential.NewSimplifiedProvisionService(
		repo,
		gatedExecutor,
		denyOnlyResetPrerequisite{},
		serviceOpts,
	)
	if err != nil {
		return out, fmt.Errorf("create simplified provision service: %w", err)
	}

	out.manager = service
	out.ready = readiness
	out.cleanupTimeout = c.RecoveryTimeout
	out.drain = func(drainCtx context.Context) error {
		// Wait / synchronize native work if needed, then close owner
		gatedExecutor.mu.Lock()
		defer gatedExecutor.mu.Unlock()
		return owner.Close(drainCtx, c.RecoveryTimeout)
	}

	return out, nil
}

// Legacy helpers retained for historical standalone check if still referenced.
func runCredentialBarrier(ctx context.Context, run, drain func(context.Context) error, timeout time.Duration) error {
	if err := run(ctx); err != nil {
		cleanup, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		if drain(cleanup) != nil {
			return fmt.Errorf("credential startup uncertain; controller close unconfirmed")
		}
		return fmt.Errorf("credential startup reconciliation unavailable")
	}
	return nil
}
