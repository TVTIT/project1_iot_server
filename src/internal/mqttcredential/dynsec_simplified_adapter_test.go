package mqttcredential

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

// spyDynSecClient implements SimplifiedDynSecClient for tests.
type spyDynSecClient struct {
	mu            syncMutexSpy
	clients       map[string]DynSecClientMetadata
	roles         map[string][]adapterACL
	calls         []string
	failOnCmd     map[string]error
	closed        bool
	closeCount    int
	roleAbsentOn  string
	getCount      int
	failAfterNGet int
}

type syncMutexSpy struct{}

func (s *spyDynSecClient) Close() {
	s.closed = true
	s.closeCount++
	s.calls = append(s.calls, "close")
}

func (s *spyDynSecClient) CreateClient(ctx context.Context, u string) error {
	s.calls = append(s.calls, "createClient:"+u)
	if err, ok := s.failOnCmd["createClient"]; ok {
		return err
	}
	if _, exists := s.clients[u]; exists {
		return dynSecError("client_exists")
	}
	s.clients[u] = DynSecClientMetadata{
		Username: u,
		Disabled: false, // native default
		Roles:    []string{},
	}
	return nil
}

func (s *spyDynSecClient) DisableClient(ctx context.Context, u string) error {
	s.calls = append(s.calls, "disableClient:"+u)
	if err, ok := s.failOnCmd["disableClient"]; ok {
		return err
	}
	meta, exists := s.clients[u]
	if !exists {
		return errDynSecClientAbsent
	}
	meta.Disabled = true
	s.clients[u] = meta
	return nil
}

func (s *spyDynSecClient) EnableClient(ctx context.Context, u string) error {
	s.calls = append(s.calls, "enableClient:"+u)
	if err, ok := s.failOnCmd["enableClient"]; ok {
		return err
	}
	meta, exists := s.clients[u]
	if !exists {
		return errDynSecClientAbsent
	}
	meta.Disabled = false
	s.clients[u] = meta
	return nil
}

func (s *spyDynSecClient) SetClientPassword(ctx context.Context, u, password string) error {
	s.calls = append(s.calls, "setClientPassword:"+u)
	if err, ok := s.failOnCmd["setClientPassword"]; ok {
		return err
	}
	if _, exists := s.clients[u]; !exists {
		return errDynSecClientAbsent
	}
	return nil
}

func (s *spyDynSecClient) AddClientRole(ctx context.Context, u, role string) error {
	s.calls = append(s.calls, "addClientRole:"+u+":"+role)
	if err, ok := s.failOnCmd["addClientRole"]; ok {
		return err
	}
	meta, exists := s.clients[u]
	if !exists {
		return errDynSecClientAbsent
	}
	meta.Roles = append(meta.Roles, role)
	s.clients[u] = meta
	return nil
}

func (s *spyDynSecClient) GetClient(ctx context.Context, u string) (DynSecClientMetadata, error) {
	s.calls = append(s.calls, "getClient:"+u)
	s.getCount++
	if s.failAfterNGet > 0 && s.getCount > s.failAfterNGet {
		return DynSecClientMetadata{}, dynSecError("query_failed_after_mutation")
	}
	if err, ok := s.failOnCmd["getClient"]; ok {
		return DynSecClientMetadata{}, err
	}
	meta, exists := s.clients[u]
	if !exists {
		return DynSecClientMetadata{}, errDynSecClientAbsent
	}
	return meta, nil
}

func (s *spyDynSecClient) adapterRequest(ctx context.Context, command string, fields map[string]any) (json.RawMessage, error) {
	s.calls = append(s.calls, command)
	if err, ok := s.failOnCmd[command]; ok {
		return nil, err
	}
	switch command {
	case "getRole":
		roleName := fields["rolename"].(string)
		if s.roleAbsentOn == roleName {
			return nil, errDynSecRoleAbsent
		}
		acls, exists := s.roles[roleName]
		if !exists {
			return nil, errDynSecRoleAbsent
		}
		resp := map[string]any{
			"role": map[string]any{
				"rolename": roleName,
				"acls":     acls,
			},
		}
		return json.Marshal(resp)
	case "createRole":
		roleName := fields["rolename"].(string)
		var acls []adapterACL
		raw, _ := json.Marshal(fields["acls"])
		_ = json.Unmarshal(raw, &acls)
		s.roles[roleName] = acls
		if s.roleAbsentOn == roleName {
			s.roleAbsentOn = "" // created
		}
		return json.Marshal(map[string]any{"data": "ok"})
	default:
		return nil, dynSecError("unsupported")
	}
}

func newSpyClient() *spyDynSecClient {
	return &spyDynSecClient{
		clients:   make(map[string]DynSecClientMetadata),
		roles:     make(map[string][]adapterACL),
		calls:     make([]string, 0),
		failOnCmd: make(map[string]error),
	}
}

func defaultTestConfig(spy *spyDynSecClient) SimplifiedDynSecConfig {
	return SimplifiedDynSecConfig{
		NewClient: func(ctx context.Context) (SimplifiedDynSecClient, error) {
			return spy, nil
		},
		Login: func(ctx context.Context, username, password string) error {
			return nil
		},
		Rejected: func(ctx context.Context, username, password string) error {
			return nil
		},
		ProtectedUsernames: []string{BackendUsername, "manager_user"},
		Timeout:            time.Second,
	}
}

func TestSimplifiedDynSecExecutor_NoWorkAfterGenerationDeadline(t *testing.T) {
	spy := newSpyClient()
	cfg := defaultTestConfig(spy)
	cfg.Timeout = 10 * time.Millisecond
	factories := 0
	cfg.NewClient = func(context.Context) (SimplifiedDynSecClient, error) { factories++; return spy, nil }
	e, _ := NewSimplifiedDynSecExecutor(cfg)
	o := Operation{OperationID: uuid.New(), ActorUserID: uuid.New(), IdempotencyKey: uuid.New(), GatewayID: "test_gateway", Action: ActionProvision, CredentialVersion: 1, Status: OperationPending, Phase: PhaseIntent, DeliveryStatus: DeliveryUnknown, ExecutionContract: ContractSimplified}
	_, err := e.Execute(context.Background(), o, func(ctx context.Context) (string, error) {
		if _, ok := ctx.Deadline(); !ok {
			t.Fatal("unbounded generator")
		}
		<-ctx.Done()
		return "", ctx.Err()
	})
	if err == nil || factories != 0 || len(spy.calls) != 0 {
		t.Fatal("work after expired generation")
	}
}

func TestSimplifiedDynSecExecutor_NoRecoveryMutation(t *testing.T) {
	spy := newSpyClient()
	cfg := defaultTestConfig(spy)
	factories := 0
	cfg.NewClient = func(context.Context) (SimplifiedDynSecClient, error) { factories++; return spy, nil }
	spy.failOnCmd["createClient"] = errors.New("unknown")
	e, _ := NewSimplifiedDynSecExecutor(cfg)
	o := Operation{OperationID: uuid.New(), ActorUserID: uuid.New(), IdempotencyKey: uuid.New(), GatewayID: "test_gateway", Action: ActionProvision, CredentialVersion: 1, Status: OperationPending, Phase: PhaseIntent, DeliveryStatus: DeliveryUnknown, ExecutionContract: ContractSimplified}
	r, err := e.Execute(context.Background(), o, func(context.Context) (string, error) {
		return "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789", nil
	})
	if err == nil || !r.Mutated || r.Outcome != ExecutionRecoveryRequired || factories != 1 {
		t.Fatal("unknown recovery")
	}
	for _, call := range spy.calls {
		if call == "disableClient:test_gateway" {
			t.Fatal("untracked recovery mutation")
		}
	}
}

func TestSimplifiedDynSecExecutor_CancelledFactoryCannotMutate(t *testing.T) {
	spy := newSpyClient()
	cfg := defaultTestConfig(spy)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg.NewClient = func(context.Context) (SimplifiedDynSecClient, error) { cancel(); return spy, nil }
	e, _ := NewSimplifiedDynSecExecutor(cfg)
	o := Operation{OperationID: uuid.New(), ActorUserID: uuid.New(), IdempotencyKey: uuid.New(), GatewayID: "test_gateway", Action: ActionProvision, CredentialVersion: 1, Status: OperationPending, Phase: PhaseIntent, DeliveryStatus: DeliveryUnknown, ExecutionContract: ContractSimplified}
	r, err := e.Execute(ctx, o, func(context.Context) (string, error) {
		return "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789", nil
	})
	if err == nil || r.Mutated || r.Outcome != ExecutionUnchangedFailure {
		t.Fatal("mutation after cancellation")
	}
}

// 1. Config validation tests
func TestSimplifiedDynSecExecutor_ConfigValidation(t *testing.T) {
	spy := newSpyClient()
	validCfg := defaultTestConfig(spy)

	// Missing NewClient
	c := validCfg
	c.NewClient = nil
	if _, err := NewSimplifiedDynSecExecutor(c); err == nil {
		t.Fatal("expected error on missing NewClient")
	}

	// Missing Login
	c = validCfg
	c.Login = nil
	if _, err := NewSimplifiedDynSecExecutor(c); err == nil {
		t.Fatal("expected error on missing Login")
	}

	// Missing Rejected
	c = validCfg
	c.Rejected = nil
	if _, err := NewSimplifiedDynSecExecutor(c); err == nil {
		t.Fatal("expected error on missing Rejected")
	}

	// Insufficient ProtectedUsernames (< 2)
	c = validCfg
	c.ProtectedUsernames = []string{BackendUsername}
	if _, err := NewSimplifiedDynSecExecutor(c); err == nil {
		t.Fatal("expected error on < 2 protected usernames")
	}

	// Duplicate or empty protected usernames
	c = validCfg
	c.ProtectedUsernames = []string{BackendUsername, ""}
	if _, err := NewSimplifiedDynSecExecutor(c); err == nil {
		t.Fatal("expected error on empty protected username")
	}
	c = validCfg
	c.ProtectedUsernames = []string{BackendUsername, BackendUsername}
	if _, err := NewSimplifiedDynSecExecutor(c); err == nil {
		t.Fatal("expected error on duplicate protected username")
	}

	// Zero or excessive timeout
	c = validCfg
	c.Timeout = 0
	if _, err := NewSimplifiedDynSecExecutor(c); err == nil {
		t.Fatal("expected error on 0 timeout")
	}
	c = validCfg
	c.Timeout = 2 * time.Minute
	if _, err := NewSimplifiedDynSecExecutor(c); err == nil {
		t.Fatal("expected error on > 1m timeout")
	}
}

// 2. Target and contract guards
func TestSimplifiedDynSecExecutor_TargetGuards(t *testing.T) {
	spy := newSpyClient()
	exec, err := NewSimplifiedDynSecExecutor(defaultTestConfig(spy))
	if err != nil {
		t.Fatal(err)
	}

	baseOp := Operation{
		OperationID:       uuid.New(),
		ActorUserID:       uuid.New(),
		GatewayID:         "gateway_001",
		IdempotencyKey:    uuid.New(),
		Action:            ActionProvision,
		CredentialVersion: 1,
		Status:            OperationPending,
		Phase:             PhaseIntent,
		DeliveryStatus:    DeliveryUnknown,
		ExecutionContract: ContractSimplified,
		CreatedAt:         time.Now(),
		UpdatedAt:         time.Now(),
	}

	// Non-simplified contract rejected
	op := baseOp
	op.ExecutionContract = ContractLifecycle
	if _, err := exec.Execute(context.Background(), op, func(ctx context.Context) (string, error) { return "pw1234567890123456", nil }); err != ErrInvalidInput {
		t.Fatalf("expected ErrInvalidInput for non-simplified contract, got %v", err)
	}

	// Protected username as target rejected
	op = baseOp
	op.GatewayID = BackendUsername
	if _, err := exec.Execute(context.Background(), op, func(ctx context.Context) (string, error) { return "pw1234567890123456", nil }); err != ErrInvalidInput {
		t.Fatalf("expected ErrInvalidInput for protected target, got %v", err)
	}

	// Other protected username as target rejected
	op = baseOp
	op.GatewayID = "manager_user"
	if _, err := exec.Execute(context.Background(), op, func(ctx context.Context) (string, error) { return "pw1234567890123456", nil }); err != ErrInvalidInput {
		t.Fatalf("expected ErrInvalidInput for manager target, got %v", err)
	}

	// Non-pending status rejected
	op = baseOp
	op.Status = OperationSucceeded
	if _, err := exec.Execute(context.Background(), op, func(ctx context.Context) (string, error) { return "pw1234567890123456", nil }); err != ErrInvalidInput {
		t.Fatalf("expected ErrInvalidInput for non-pending status, got %v", err)
	}

	// Non-intent phase rejected
	op = baseOp
	op.Phase = PhaseFinalize
	if _, err := exec.Execute(context.Background(), op, func(ctx context.Context) (string, error) { return "pw1234567890123456", nil }); err != ErrInvalidInput {
		t.Fatalf("expected ErrInvalidInput for non-intent phase, got %v", err)
	}

	// Non-empty initial evidence rejected
	op = baseOp
	op.Evidence = VerificationEvidence{RAMApplied: true}
	if _, err := exec.Execute(context.Background(), op, func(ctx context.Context) (string, error) { return "pw1234567890123456", nil }); err != ErrInvalidInput {
		t.Fatalf("expected ErrInvalidInput for non-empty evidence, got %v", err)
	}

	// Rotate with non-monotonic version rejected
	op = baseOp
	op.Action = ActionRotate
	op.Previous = &CredentialSnapshot{Status: CredentialActive, CredentialVersion: 5}
	op.CredentialVersion = 5
	if _, err := exec.Execute(context.Background(), op, func(ctx context.Context) (string, error) { return "pw1234567890123456", nil }); err != ErrInvalidInput {
		t.Fatalf("expected ErrInvalidInput for non-monotonic rotate version, got %v", err)
	}

	// Revoke with mismatched version rejected
	op = baseOp
	op.Action = ActionRevoke
	op.Previous = &CredentialSnapshot{Status: CredentialActive, CredentialVersion: 5}
	op.CredentialVersion = 6
	if _, err := exec.Execute(context.Background(), op, nil); err != ErrInvalidInput {
		t.Fatalf("expected ErrInvalidInput for mismatched revoke version, got %v", err)
	}

	// Reset with non-monotonic version rejected
	predID := uuid.New()
	reason := ResetReasonRecoveryAfterUnknown
	op = baseOp
	op.Action = ActionReset
	op.ResetPredecessorID = &predID
	op.ResetReason = &reason
	op.Previous = &CredentialSnapshot{Status: CredentialRecoveryNeeded, CredentialVersion: 5}
	op.CredentialVersion = 5
	if _, err := exec.Execute(context.Background(), op, func(ctx context.Context) (string, error) { return "pw1234567890123456", nil }); err != ErrInvalidInput {
		t.Fatalf("expected ErrInvalidInput for non-monotonic reset version, got %v", err)
	}

	// Reset with invalid reason rejected
	badReason := ResetReason("invalid_reason")
	op.ResetReason = &badReason
	if _, err := exec.Execute(context.Background(), op, func(ctx context.Context) (string, error) { return "pw1234567890123456", nil }); err != ErrInvalidInput {
		t.Fatalf("expected ErrInvalidInput for invalid reset reason, got %v", err)
	}
}

// 3. Provisioning - Brand new provision success & client release
func TestSimplifiedDynSecExecutor_Provision_Success(t *testing.T) {
	spy := newSpyClient()
	spy.roleAbsentOn = "gateway_gateway_001" // role will be created

	loginCalled := false
	cfg := defaultTestConfig(spy)
	cfg.Login = func(ctx context.Context, u, pw string) error {
		loginCalled = true
		if u != "gateway_001" || pw != "secret123456789012" {
			t.Fatalf("unexpected login args: %s / %s", u, pw)
		}
		return nil
	}

	exec, err := NewSimplifiedDynSecExecutor(cfg)
	if err != nil {
		t.Fatal(err)
	}

	op := Operation{
		OperationID:       uuid.New(),
		ActorUserID:       uuid.New(),
		GatewayID:         "gateway_001",
		IdempotencyKey:    uuid.New(),
		Action:            ActionProvision,
		CredentialVersion: 1,
		Status:            OperationPending,
		Phase:             PhaseIntent,
		DeliveryStatus:    DeliveryUnknown,
		ExecutionContract: ContractSimplified,
		CreatedAt:         time.Now(),
		UpdatedAt:         time.Now(),
	}

	res, err := exec.Execute(context.Background(), op, func(ctx context.Context) (string, error) {
		return "secret123456789012", nil
	})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	if res.Outcome != ExecutionVerifiedSuccess {
		t.Fatalf("expected outcome verified_success, got %v", res.Outcome)
	}
	if !res.Evidence.RAMApplied || !res.Evidence.FreshPositiveVerified || res.Evidence.ProtocolDenialVerified {
		t.Fatalf("unexpected evidence flags: %+v", res.Evidence)
	}
	if !loginCalled {
		t.Fatal("login probe was not called")
	}
	if !spy.closed {
		t.Fatal("management client was not closed")
	}

	// Verify native client state in spy
	clientMeta, exists := spy.clients["gateway_001"]
	if !exists {
		t.Fatal("client was not created in spy")
	}
	if clientMeta.Disabled {
		t.Fatal("client should be enabled after provision")
	}
	if len(clientMeta.Roles) != 1 || clientMeta.Roles[0] != "gateway_gateway_001" {
		t.Fatalf("client roles mismatch: %v", clientMeta.Roles)
	}

	// Verify ACLs created in role
	roleAcls, exists := spy.roles["gateway_gateway_001"]
	if !exists {
		t.Fatal("role was not created in spy")
	}
	wanted := adapterGatewayACLs("gateway_001")
	if len(roleAcls) != len(wanted) {
		t.Fatalf("role ACL count mismatch: got %d, wanted %d", len(roleAcls), len(wanted))
	}
}

// 4. Provisioning - Unknown existing client must not be adopted
func TestSimplifiedDynSecExecutor_Provision_UnknownExistingClientRejected(t *testing.T) {
	spy := newSpyClient()
	// Pre-populate client in spy
	spy.clients["gateway_001"] = DynSecClientMetadata{
		Username: "gateway_001",
		Disabled: false,
		Roles:    []string{"gateway_gateway_001"},
	}

	exec, err := NewSimplifiedDynSecExecutor(defaultTestConfig(spy))
	if err != nil {
		t.Fatal(err)
	}

	op := Operation{
		OperationID:       uuid.New(),
		ActorUserID:       uuid.New(),
		GatewayID:         "gateway_001",
		IdempotencyKey:    uuid.New(),
		Action:            ActionProvision,
		CredentialVersion: 1,
		Status:            OperationPending,
		Phase:             PhaseIntent,
		DeliveryStatus:    DeliveryUnknown,
		ExecutionContract: ContractSimplified,
		Previous:          nil, // new provision
		CreatedAt:         time.Now(),
		UpdatedAt:         time.Now(),
	}

	res, err := exec.Execute(context.Background(), op, func(ctx context.Context) (string, error) {
		return "secret123456789012", nil
	})
	if err != ErrRecoveryRequired {
		t.Fatalf("expected ErrRecoveryRequired, got %v", err)
	}
	if res.Outcome != ExecutionRecoveryRequired {
		t.Fatalf("expected outcome recovery_required, got %v", res.Outcome)
	}
	if res.Mutated {
		t.Fatal("mutation should not have occurred")
	}
	if !spy.closed {
		t.Fatal("client should be closed")
	}
}

// 5. Reprovisioning of previously revoked credential
func TestSimplifiedDynSecExecutor_Reprovision_RevokedPredecessor(t *testing.T) {
	spy := newSpyClient()
	// Client exists and is disabled
	spy.clients["gateway_001"] = DynSecClientMetadata{
		Username: "gateway_001",
		Disabled: true,
		Roles:    []string{"gateway_gateway_001"},
	}
	spy.roles["gateway_gateway_001"] = adapterGatewayACLs("gateway_001")

	exec, err := NewSimplifiedDynSecExecutor(defaultTestConfig(spy))
	if err != nil {
		t.Fatal(err)
	}

	op := Operation{
		OperationID:       uuid.New(),
		ActorUserID:       uuid.New(),
		GatewayID:         "gateway_001",
		IdempotencyKey:    uuid.New(),
		Action:            ActionProvision,
		CredentialVersion: 2,
		Status:            OperationPending,
		Phase:             PhaseIntent,
		DeliveryStatus:    DeliveryUnknown,
		ExecutionContract: ContractSimplified,
		Previous: &CredentialSnapshot{
			Status:            CredentialRevoked,
			CredentialVersion: 1,
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	res, err := exec.Execute(context.Background(), op, func(ctx context.Context) (string, error) {
		return "secret123456789012", nil
	})
	if err != nil {
		t.Fatalf("reprovision failed: %v", err)
	}
	if res.Outcome != ExecutionVerifiedSuccess {
		t.Fatalf("expected verified_success, got %v", res.Outcome)
	}
	if !res.Evidence.RAMApplied || !res.Evidence.FreshPositiveVerified {
		t.Fatalf("unexpected evidence: %+v", res.Evidence)
	}

	// Case 5b: reprovision when native client was active instead of disabled -> must fail
	spy.clients["gateway_001"] = DynSecClientMetadata{
		Username: "gateway_001",
		Disabled: false, // invalid for revoked predecessor!
		Roles:    []string{"gateway_gateway_001"},
	}
	res, err = exec.Execute(context.Background(), op, func(ctx context.Context) (string, error) {
		return "secret123456789012", nil
	})
	if err != ErrRecoveryRequired {
		t.Fatalf("expected ErrRecoveryRequired, got %v", err)
	}
}

// 6. Rotate success & active baseline check
func TestSimplifiedDynSecExecutor_Rotate_Success(t *testing.T) {
	spy := newSpyClient()
	spy.clients["gateway_001"] = DynSecClientMetadata{
		Username: "gateway_001",
		Disabled: false,
		Roles:    []string{"gateway_gateway_001"},
	}
	spy.roles["gateway_gateway_001"] = adapterGatewayACLs("gateway_001")

	exec, err := NewSimplifiedDynSecExecutor(defaultTestConfig(spy))
	if err != nil {
		t.Fatal(err)
	}

	op := Operation{
		OperationID:       uuid.New(),
		ActorUserID:       uuid.New(),
		GatewayID:         "gateway_001",
		IdempotencyKey:    uuid.New(),
		Action:            ActionRotate,
		CredentialVersion: 2,
		Status:            OperationPending,
		Phase:             PhaseIntent,
		DeliveryStatus:    DeliveryUnknown,
		ExecutionContract: ContractSimplified,
		Previous: &CredentialSnapshot{
			Status:            CredentialActive,
			CredentialVersion: 1,
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	res, err := exec.Execute(context.Background(), op, func(ctx context.Context) (string, error) {
		return "newsecret1234567890", nil
	})
	if err != nil {
		t.Fatalf("rotate failed: %v", err)
	}
	if res.Outcome != ExecutionVerifiedSuccess {
		t.Fatalf("expected verified_success, got %v", res.Outcome)
	}
	if !res.Evidence.RAMApplied || !res.Evidence.FreshPositiveVerified {
		t.Fatalf("unexpected evidence: %+v", res.Evidence)
	}
}

// 7. Rotate without active baseline fails
func TestSimplifiedDynSecExecutor_Rotate_RequiresActiveBaseline(t *testing.T) {
	spy := newSpyClient()
	exec, err := NewSimplifiedDynSecExecutor(defaultTestConfig(spy))
	if err != nil {
		t.Fatal(err)
	}

	// 7a. Native client absent
	op := Operation{
		OperationID:       uuid.New(),
		ActorUserID:       uuid.New(),
		GatewayID:         "gateway_001",
		IdempotencyKey:    uuid.New(),
		Action:            ActionRotate,
		CredentialVersion: 2,
		Status:            OperationPending,
		Phase:             PhaseIntent,
		DeliveryStatus:    DeliveryUnknown,
		ExecutionContract: ContractSimplified,
		Previous: &CredentialSnapshot{
			Status:            CredentialActive,
			CredentialVersion: 1,
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	res, err := exec.Execute(context.Background(), op, func(ctx context.Context) (string, error) {
		return "newsecret1234567890", nil
	})
	if err != ErrRecoveryRequired {
		t.Fatalf("expected ErrRecoveryRequired on absent native client, got %v", err)
	}
	if res.Outcome != ExecutionRecoveryRequired {
		t.Fatalf("expected recovery_required, got %v", res.Outcome)
	}

	// 7b. Native client disabled
	spy.clients["gateway_001"] = DynSecClientMetadata{
		Username: "gateway_001",
		Disabled: true, // disabled in native!
		Roles:    []string{"gateway_gateway_001"},
	}
	spy.roles["gateway_gateway_001"] = adapterGatewayACLs("gateway_001")

	res, err = exec.Execute(context.Background(), op, func(ctx context.Context) (string, error) {
		return "newsecret1234567890", nil
	})
	if err != ErrRecoveryRequired {
		t.Fatalf("expected ErrRecoveryRequired on disabled native client, got %v", err)
	}

	// 7c. Role missing in native
	spy.clients["gateway_001"] = DynSecClientMetadata{
		Username: "gateway_001",
		Disabled: false,
		Roles:    []string{"gateway_gateway_001"},
	}
	delete(spy.roles, "gateway_gateway_001")
	res, err = exec.Execute(context.Background(), op, func(ctx context.Context) (string, error) {
		return "newsecret1234567890", nil
	})
	if err != ErrRecoveryRequired {
		t.Fatalf("expected ErrRecoveryRequired on missing role, got %v", err)
	}
}

// 8. Revoke success & zero password generator invocation
func TestSimplifiedDynSecExecutor_Revoke_UnsupportedSafetyGuard(t *testing.T) {
	spy := newSpyClient()
	spy.clients["gateway_001"] = DynSecClientMetadata{
		Username: "gateway_001",
		Disabled: false,
		Roles:    []string{"gateway_gateway_001"},
	}
	spy.roles["gateway_gateway_001"] = adapterGatewayACLs("gateway_001")

	newClientCalls := 0
	cfg := defaultTestConfig(spy)
	origNewClient := cfg.NewClient
	cfg.NewClient = func(ctx context.Context) (SimplifiedDynSecClient, error) {
		newClientCalls++
		return origNewClient(ctx)
	}

	rejectedCalls := 0
	cfg.Rejected = func(ctx context.Context, u, pw string) error {
		rejectedCalls++
		return nil
	}

	exec, err := NewSimplifiedDynSecExecutor(cfg)
	if err != nil {
		t.Fatal(err)
	}

	op := Operation{
		OperationID:       uuid.New(),
		ActorUserID:       uuid.New(),
		GatewayID:         "gateway_001",
		IdempotencyKey:    uuid.New(),
		Action:            ActionRevoke,
		CredentialVersion: 1,
		Status:            OperationPending,
		Phase:             PhaseIntent,
		DeliveryStatus:    DeliveryUnknown,
		ExecutionContract: ContractSimplified,
		Previous: &CredentialSnapshot{
			Status:            CredentialActive,
			CredentialVersion: 1,
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	generatorCalls := 0
	gen := func(ctx context.Context) (string, error) {
		generatorCalls++
		return "should_never_be_called", nil
	}

	res, err := exec.Execute(context.Background(), op, gen)

	if !errors.Is(err, ErrLifecycleUnavailable) {
		t.Fatalf("expected ErrLifecycleUnavailable, got %v", err)
	}
	if res.OperationID != op.OperationID {
		t.Fatalf("expected OperationID %v, got %v", op.OperationID, res.OperationID)
	}
	if res.Outcome != ExecutionUnchangedFailure {
		t.Fatalf("expected ExecutionUnchangedFailure, got %v", res.Outcome)
	}
	if res.Mutated {
		t.Fatalf("expected Mutated=false, got true")
	}
	if res.Evidence != (SimplifiedExecutionEvidence{}) {
		t.Fatalf("expected empty evidence, got %+v", res.Evidence)
	}
	if newClientCalls != 0 {
		t.Fatalf("expected NewClient calls == 0, got %d", newClientCalls)
	}
	if generatorCalls != 0 {
		t.Fatalf("expected generator calls == 0, got %d", generatorCalls)
	}
	if rejectedCalls != 0 {
		t.Fatalf("expected Rejected calls == 0, got %d", rejectedCalls)
	}
	for _, call := range spy.calls {
		if call == "disableClient:gateway_001" || call == "getClient:gateway_001" {
			t.Fatalf("spy should have zero broker calls, got call: %s", call)
		}
	}
}

// 8. Revoke is currently unsupported (safety containment guard)
func TestSimplifiedDynSecExecutor_Revoke_Unsupported(t *testing.T) {
	spy := newSpyClient()
	spy.clients["gateway_001"] = DynSecClientMetadata{
		Username: "gateway_001",
		Disabled: false,
		Roles:    []string{"gateway_gateway_001"},
	}
	spy.roles["gateway_gateway_001"] = adapterGatewayACLs("gateway_001")

	newClientCalled := 0
	rejectedCalled := false
	loginCalled := false
	cfg := defaultTestConfig(spy)
	origNewClient := cfg.NewClient
	cfg.NewClient = func(ctx context.Context) (SimplifiedDynSecClient, error) {
		newClientCalled++
		return origNewClient(ctx)
	}
	cfg.Rejected = func(ctx context.Context, u, pw string) error {
		rejectedCalled = true
		return nil
	}
	cfg.Login = func(ctx context.Context, u, pw string) error {
		loginCalled = true
		return nil
	}

	exec, err := NewSimplifiedDynSecExecutor(cfg)
	if err != nil {
		t.Fatal(err)
	}

	validOp := Operation{
		OperationID:       uuid.New(),
		ActorUserID:       uuid.New(),
		GatewayID:         "gateway_001",
		IdempotencyKey:    uuid.New(),
		Action:            ActionRevoke,
		CredentialVersion: 1,
		Status:            OperationPending,
		Phase:             PhaseIntent,
		DeliveryStatus:    DeliveryUnknown,
		ExecutionContract: ContractSimplified,
		Previous: &CredentialSnapshot{
			Status:            CredentialActive,
			CredentialVersion: 1,
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	generatorInvoked := false
	res, err := exec.Execute(context.Background(), validOp, func(ctx context.Context) (string, error) {
		generatorInvoked = true
		return "should_never_be_called", nil
	})
	if !errors.Is(err, ErrLifecycleUnavailable) {
		t.Fatalf("expected ErrLifecycleUnavailable on unsupported revoke, got %v", err)
	}
	if generatorInvoked {
		t.Fatal("password generator must NEVER be invoked on revoke")
	}
	if rejectedCalled {
		t.Fatal("rejection probe must NOT be called on unsupported revoke")
	}
	if loginCalled {
		t.Fatal("login probe must NOT be called on revoke")
	}
	if newClientCalled != 0 {
		t.Fatalf("NewClient must not be called, got %d", newClientCalled)
	}
	if res.OperationID != validOp.OperationID {
		t.Fatalf("expected OperationID %v, got %v", validOp.OperationID, res.OperationID)
	}
	if res.Outcome != ExecutionUnchangedFailure {
		t.Fatalf("expected ExecutionUnchangedFailure, got %v", res.Outcome)
	}
	if res.Mutated {
		t.Fatal("operation must NOT be mutated")
	}
	if res.Evidence != (SimplifiedExecutionEvidence{}) {
		t.Fatalf("expected empty evidence, got %+v", res.Evidence)
	}

	// Verify native client in spy was NOT mutated / disabled
	if spy.clients["gateway_001"].Disabled {
		t.Fatal("native client should NOT be modified")
	}

	// 8b: Invalid revoke target: missing Previous -> rejected by target validation with ErrInvalidInput
	invalidOpNoPrev := validOp
	invalidOpNoPrev.Previous = nil
	res, err = exec.Execute(context.Background(), invalidOpNoPrev, nil)
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput on missing Previous, got %v", err)
	}
	if res.Outcome != ExecutionUnchangedFailure || res.Mutated {
		t.Fatalf("expected unchanged unmutated failure on invalid target, got %+v", res)
	}

	// 8c: Invalid revoke target: mismatched version -> rejected by target validation with ErrInvalidInput
	invalidOpVer := validOp
	invalidOpVer.CredentialVersion = 2
	res, err = exec.Execute(context.Background(), invalidOpVer, nil)
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput on version mismatch, got %v", err)
	}
	if res.Outcome != ExecutionUnchangedFailure || res.Mutated {
		t.Fatalf("expected unchanged unmutated failure on invalid target, got %+v", res)
	}

	// 8d: Invalid revoke target: non-simplified contract -> rejected with ErrInvalidInput
	invalidOpContract := validOp
	invalidOpContract.ExecutionContract = ContractLifecycle
	res, err = exec.Execute(context.Background(), invalidOpContract, nil)
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput on non-simplified contract, got %v", err)
	}
	if res.Outcome != ExecutionUnchangedFailure || res.Mutated {
		t.Fatalf("expected unchanged unmutated failure on invalid target, got %+v", res)
	}

	// 8e: Expired context does NOT mutate
	expiredCtx, cancel := context.WithCancel(context.Background())
	cancel()
	res, err = exec.Execute(expiredCtx, validOp, nil)
	if res.Mutated {
		t.Fatal("expired context must NOT mutate")
	}
	if res.Outcome != ExecutionUnchangedFailure {
		t.Fatalf("expected ExecutionUnchangedFailure on expired context, got %v", res.Outcome)
	}
}

// 9. Reset flows
func TestSimplifiedDynSecExecutor_Reset_Success(t *testing.T) {
	// 9a. Lost secret recovery: client was already present and active
	t.Run("LostSecretRecovery", func(t *testing.T) {
		spy := newSpyClient()
		spy.clients["gateway_001"] = DynSecClientMetadata{
			Username: "gateway_001",
			Disabled: false,
			Roles:    []string{"gateway_gateway_001"},
		}
		spy.roles["gateway_gateway_001"] = adapterGatewayACLs("gateway_001")

		exec, err := NewSimplifiedDynSecExecutor(defaultTestConfig(spy))
		if err != nil {
			t.Fatal(err)
		}

		predID := uuid.New()
		reason := ResetReasonLostSecretRecovery
		op := Operation{
			OperationID:        uuid.New(),
			ActorUserID:        uuid.New(),
			GatewayID:          "gateway_001",
			IdempotencyKey:     uuid.New(),
			Action:             ActionReset,
			CredentialVersion:  2,
			Status:             OperationPending,
			Phase:              PhaseIntent,
			DeliveryStatus:     DeliveryUnknown,
			ExecutionContract:  ContractSimplified,
			ResetPredecessorID: &predID,
			ResetReason:        &reason,
			Previous: &CredentialSnapshot{
				Status:            CredentialActive,
				CredentialVersion: 1,
			},
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}

		res, err := exec.Execute(context.Background(), op, func(ctx context.Context) (string, error) {
			return "resetpw1234567890", nil
		})
		if err != nil {
			t.Fatalf("reset failed: %v", err)
		}
		if res.Outcome != ExecutionVerifiedSuccess {
			t.Fatalf("expected verified_success, got %v", res.Outcome)
		}
		if !res.Evidence.RAMApplied || !res.Evidence.FreshPositiveVerified {
			t.Fatalf("unexpected evidence: %+v", res.Evidence)
		}

		// LostSecretRecovery when client absent -> must fail
		delete(spy.clients, "gateway_001")
		res, err = exec.Execute(context.Background(), op, func(ctx context.Context) (string, error) {
			return "resetpw1234567890", nil
		})
		if err != ErrRecoveryRequired {
			t.Fatalf("expected ErrRecoveryRequired for lost secret on absent client, got %v", err)
		}
	})

	// 9b. Recovery after unknown: client was absent (e.g. failed during create)
	t.Run("RecoveryAfterUnknown_AbsentClient", func(t *testing.T) {
		spy := newSpyClient()
		spy.roleAbsentOn = "gateway_gateway_001"

		exec, err := NewSimplifiedDynSecExecutor(defaultTestConfig(spy))
		if err != nil {
			t.Fatal(err)
		}

		predID := uuid.New()
		reason := ResetReasonRecoveryAfterUnknown
		op := Operation{
			OperationID:        uuid.New(),
			ActorUserID:        uuid.New(),
			GatewayID:          "gateway_001",
			IdempotencyKey:     uuid.New(),
			Action:             ActionReset,
			CredentialVersion:  2,
			Status:             OperationPending,
			Phase:              PhaseIntent,
			DeliveryStatus:     DeliveryUnknown,
			ExecutionContract:  ContractSimplified,
			ResetPredecessorID: &predID,
			ResetReason:        &reason,
			Previous: &CredentialSnapshot{
				Status:            CredentialRecoveryNeeded,
				CredentialVersion: 1,
			},
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}

		res, err := exec.Execute(context.Background(), op, func(ctx context.Context) (string, error) {
			return "resetpw1234567890", nil
		})
		if err != nil {
			t.Fatalf("reset failed: %v", err)
		}
		if res.Outcome != ExecutionVerifiedSuccess {
			t.Fatalf("expected verified_success, got %v", res.Outcome)
		}
		if !res.Evidence.RAMApplied || !res.Evidence.FreshPositiveVerified {
			t.Fatalf("unexpected evidence: %+v", res.Evidence)
		}
		if spy.clients["gateway_001"].Disabled {
			t.Fatal("client should be enabled after reset")
		}

		// Foreign username mismatch on RecoveryAfterUnknown
		spy.clients["gateway_001"] = DynSecClientMetadata{
			Username: "foreign_user",
		}
		res, err = exec.Execute(context.Background(), op, func(ctx context.Context) (string, error) {
			return "resetpw1234567890", nil
		})
		if err != ErrRecoveryRequired {
			t.Fatalf("expected ErrRecoveryRequired on foreign client match, got %v", err)
		}
	})
}

// 10. Mutation uncertainty & rollback / recovery handling
func TestSimplifiedDynSecExecutor_PartialFailure_MarksMutatedAndRecoveryRequired(t *testing.T) {
	faults := []string{
		"ensureRoleCreate",
		"createClient",
		"disableClient",
		"setClientPassword",
		"enableClient",
		"getClientAfter",
		"ensureRoleVerify",
		"freshAuthLogin",
	}

	for _, fault := range faults {
		t.Run("fault_"+fault, func(t *testing.T) {
			spy := newSpyClient()
			spy.roleAbsentOn = "gateway_gateway_001"
			cfg := defaultTestConfig(spy)

			switch fault {
			case "ensureRoleCreate":
				spy.failOnCmd["createRole"] = dynSecError("transport_error")
			case "createClient":
				spy.failOnCmd["createClient"] = dynSecError("transport_error")
			case "disableClient":
				spy.failOnCmd["disableClient"] = dynSecError("transport_error")
			case "setClientPassword":
				spy.failOnCmd["setClientPassword"] = dynSecError("transport_error")
			case "enableClient":
				spy.failOnCmd["enableClient"] = dynSecError("transport_error")
			case "getClientAfter":
				spy.failAfterNGet = 1
			case "ensureRoleVerify":
				spy.failOnCmd["getRole"] = dynSecError("transport_error")
			case "freshAuthLogin":
				cfg.Login = func(ctx context.Context, u, pw string) error {
					return errors.New("auth_rejected")
				}
			}

			exec, err := NewSimplifiedDynSecExecutor(cfg)
			if err != nil {
				t.Fatal(err)
			}

			act := ActionProvision

			op := Operation{
				OperationID:       uuid.New(),
				ActorUserID:       uuid.New(),
				GatewayID:         "gateway_001",
				IdempotencyKey:    uuid.New(),
				Action:            act,
				CredentialVersion: 1,
				Status:            OperationPending,
				Phase:             PhaseIntent,
				DeliveryStatus:    DeliveryUnknown,
				ExecutionContract: ContractSimplified,
				Previous:          nil,
				CreatedAt:         time.Now(),
				UpdatedAt:         time.Now(),
			}

			gen := func(ctx context.Context) (string, error) {
				return "secret123456789012", nil
			}

			res, err := exec.Execute(context.Background(), op, gen)

			if err != ErrRecoveryRequired {
				t.Fatalf("expected ErrRecoveryRequired, got %v", err)
			}
			if res.Outcome != ExecutionRecoveryRequired {
				t.Fatalf("expected ExecutionRecoveryRequired, got %v", res.Outcome)
			}
			if !res.Mutated {
				t.Fatal("operation should be marked mutated")
			}
			if res.Evidence.FreshPositiveVerified {
				t.Fatal("fresh auth must not be marked verified on failure")
			}
		})
	}
}

// 11. Cancellation and client closure
func TestSimplifiedDynSecExecutor_ContextCancelled_ClosesClient(t *testing.T) {
	spy := newSpyClient()
	exec, err := NewSimplifiedDynSecExecutor(defaultTestConfig(spy))
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel context

	op := Operation{
		OperationID:       uuid.New(),
		ActorUserID:       uuid.New(),
		GatewayID:         "gateway_001",
		IdempotencyKey:    uuid.New(),
		Action:            ActionProvision,
		CredentialVersion: 1,
		Status:            OperationPending,
		Phase:             PhaseIntent,
		DeliveryStatus:    DeliveryUnknown,
		ExecutionContract: ContractSimplified,
		CreatedAt:         time.Now(),
		UpdatedAt:         time.Now(),
	}

	res, err := exec.Execute(ctx, op, func(ctx context.Context) (string, error) {
		return "secret123456789012", nil
	})
	if err == nil {
		t.Fatal("expected error on cancelled context")
	}
	if res.Outcome == ExecutionVerifiedSuccess {
		t.Fatal("cancelled execution must not be verified_success")
	}
}

// 12. Generator error and invalid secret validation
func TestSimplifiedDynSecExecutor_GeneratorErrors(t *testing.T) {
	spy := newSpyClient()
	exec, err := NewSimplifiedDynSecExecutor(defaultTestConfig(spy))
	if err != nil {
		t.Fatal(err)
	}

	op := Operation{
		OperationID:       uuid.New(),
		ActorUserID:       uuid.New(),
		GatewayID:         "gateway_001",
		IdempotencyKey:    uuid.New(),
		Action:            ActionProvision,
		CredentialVersion: 1,
		Status:            OperationPending,
		Phase:             PhaseIntent,
		DeliveryStatus:    DeliveryUnknown,
		ExecutionContract: ContractSimplified,
		CreatedAt:         time.Now(),
		UpdatedAt:         time.Now(),
	}

	// Nil generator
	res, err := exec.Execute(context.Background(), op, nil)
	if err != ErrVerificationFailed {
		t.Fatalf("expected ErrVerificationFailed on nil generator, got %v", err)
	}
	if res.Outcome != ExecutionUnchangedFailure {
		t.Fatalf("expected ExecutionUnchangedFailure, got %v", res.Outcome)
	}

	// Generator returns error
	res, err = exec.Execute(context.Background(), op, func(ctx context.Context) (string, error) {
		return "", errors.New("entropy_depleted")
	})
	if err != ErrVerificationFailed {
		t.Fatalf("expected ErrVerificationFailed on generator error, got %v", err)
	}

	// Generator returns invalid password (contains newline/null or exceeds 4096)
	res, err = exec.Execute(context.Background(), op, func(ctx context.Context) (string, error) {
		return "invalid\npassword", nil
	})
	if err != ErrVerificationFailed {
		t.Fatalf("expected ErrVerificationFailed on newline password, got %v", err)
	}
}

// 13. Client factory error
func TestSimplifiedDynSecExecutor_ClientFactoryError(t *testing.T) {
	cfg := defaultTestConfig(newSpyClient())
	cfg.NewClient = func(ctx context.Context) (SimplifiedDynSecClient, error) {
		return nil, errors.New("tls_handshake_failed")
	}

	exec, err := NewSimplifiedDynSecExecutor(cfg)
	if err != nil {
		t.Fatal(err)
	}

	op := Operation{
		OperationID:       uuid.New(),
		ActorUserID:       uuid.New(),
		GatewayID:         "gateway_001",
		IdempotencyKey:    uuid.New(),
		Action:            ActionProvision,
		CredentialVersion: 1,
		Status:            OperationPending,
		Phase:             PhaseIntent,
		DeliveryStatus:    DeliveryUnknown,
		ExecutionContract: ContractSimplified,
		CreatedAt:         time.Now(),
		UpdatedAt:         time.Now(),
	}

	res, err := exec.Execute(context.Background(), op, func(ctx context.Context) (string, error) {
		return "secret123456789012", nil
	})
	if err != ErrVerificationFailed {
		t.Fatalf("expected ErrVerificationFailed on client factory error, got %v", err)
	}
	if res.Outcome != ExecutionUnchangedFailure {
		t.Fatalf("expected ExecutionUnchangedFailure, got %v", res.Outcome)
	}
}

// 14. Initial native query error
func TestSimplifiedDynSecExecutor_InitialQueryError(t *testing.T) {
	spy := newSpyClient()
	spy.failOnCmd["getClient"] = errors.New("broker_timeout")

	exec, err := NewSimplifiedDynSecExecutor(defaultTestConfig(spy))
	if err != nil {
		t.Fatal(err)
	}

	op := Operation{
		OperationID:       uuid.New(),
		ActorUserID:       uuid.New(),
		GatewayID:         "gateway_001",
		IdempotencyKey:    uuid.New(),
		Action:            ActionProvision,
		CredentialVersion: 1,
		Status:            OperationPending,
		Phase:             PhaseIntent,
		DeliveryStatus:    DeliveryUnknown,
		ExecutionContract: ContractSimplified,
		CreatedAt:         time.Now(),
		UpdatedAt:         time.Now(),
	}

	res, err := exec.Execute(context.Background(), op, func(ctx context.Context) (string, error) {
		return "secret123456789012", nil
	})
	if err != ErrVerificationFailed {
		t.Fatalf("expected ErrVerificationFailed on initial query error, got %v", err)
	}
	if res.Outcome != ExecutionUnchangedFailure {
		t.Fatalf("expected ExecutionUnchangedFailure, got %v", res.Outcome)
	}
	if res.Mutated {
		t.Fatal("should not be mutated on initial query failure")
	}
}

// 15. EnsureRole edge cases
func TestSimplifiedDynSecExecutor_EnsureRoleEdgeCases(t *testing.T) {
	spy := newSpyClient()
	exec, err := NewSimplifiedDynSecExecutor(defaultTestConfig(spy))
	if err != nil {
		t.Fatal(err)
	}

	// 15a. Malformed gateway ID in ensureRole
	if err := exec.ensureRole(context.Background(), spy, "invalid/gateway", "gateway_invalid/gateway", false); err != ErrInvalidInput {
		t.Fatalf("expected ErrInvalidInput for invalid gateway ID, got %v", err)
	}

	// 15b. Protected gateway ID in ensureRole
	if err := exec.ensureRole(context.Background(), spy, BackendUsername, "gateway_"+BackendUsername, false); err != ErrInvalidInput {
		t.Fatalf("expected ErrInvalidInput for backend gateway ID, got %v", err)
	}

	// 15c. Mismatched role name
	if err := exec.ensureRole(context.Background(), spy, "gateway_001", "other_role", false); err != ErrInvalidInput {
		t.Fatalf("expected ErrInvalidInput for mismatched role name, got %v", err)
	}

	// 15d. Role absent when create=false
	spy.roleAbsentOn = "gateway_gateway_001"
	if err := exec.ensureRole(context.Background(), spy, "gateway_001", "gateway_gateway_001", false); err != ErrRecoveryRequired {
		t.Fatalf("expected ErrRecoveryRequired when role absent and create=false, got %v", err)
	}

	// 15e. Role with extra/corrupted ACLs
	spy.roleAbsentOn = ""
	corruptACLs := append(adapterGatewayACLs("gateway_001"), adapterACL{
		Type:  "publishClientSend",
		Topic: "unauthorized/#",
		Allow: true,
	})
	spy.roles["gateway_gateway_001"] = corruptACLs
	if err := exec.ensureRole(context.Background(), spy, "gateway_001", "gateway_gateway_001", false); err != ErrRecoveryRequired {
		t.Fatalf("expected ErrRecoveryRequired for corrupt ACLs, got %v", err)
	}

	// 15f. CheckRevoked unit tests
	spy.roles["gateway_gateway_001"] = adapterGatewayACLs("gateway_001")
	spy.clients["gateway_001"] = DynSecClientMetadata{
		Username: "gateway_001",
		Disabled: true,
		Roles:    []string{"gateway_gateway_001"},
	}
	if err := exec.CheckRevoked(context.Background(), "gateway_001"); err != nil {
		t.Fatalf("expected CheckRevoked success, got %v", err)
	}
	if err := exec.CheckRevoked(context.Background(), "invalid/id"); err != ErrInvalidInput {
		t.Fatalf("expected ErrInvalidInput on invalid gateway ID, got %v", err)
	}
	if err := exec.CheckRevoked(context.Background(), BackendUsername); err != ErrInvalidInput {
		t.Fatalf("expected ErrInvalidInput on protected username, got %v", err)
	}
	// Active client fails CheckRevoked
	spy.clients["gateway_001"] = DynSecClientMetadata{
		Username: "gateway_001",
		Disabled: false,
		Roles:    []string{"gateway_gateway_001"},
	}
	if err := exec.CheckRevoked(context.Background(), "gateway_001"); err != ErrRecoveryRequired {
		t.Fatalf("expected ErrRecoveryRequired on non-disabled client, got %v", err)
	}
}
