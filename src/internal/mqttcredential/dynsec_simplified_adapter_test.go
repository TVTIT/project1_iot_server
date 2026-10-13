package mqttcredential

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
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
	onDisableHook func(u string)
}

type syncMutexSpy struct{}

type spyDynSecClientWithAfterDisableRoleFailure struct {
	*spyDynSecClient
}

func (s *spyDynSecClientWithAfterDisableRoleFailure) DisableClient(ctx context.Context, u string) error {
	err := s.spyDynSecClient.DisableClient(ctx, u)
	if err == nil {
		delete(s.spyDynSecClient.roles, "gateway_"+u)
	}
	return err
}

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
	if s.onDisableHook != nil {
		s.onDisableHook(u)
	}
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
	if res.Evidence.NativeDisabledObserved || res.Evidence.NativePersistedRoleACLObserved {
		t.Fatalf("rotate must not claim revoke-specific evidence: %+v", res.Evidence)
	}

	// Verify exact ordered sequence of calls on the management client:
	// 1. getClient:gateway_001 (baseline check)
	// 2. getRole (ensureRole baseline)
	// 3. disableClient:gateway_001
	// 4. getClient:gateway_001 (disabled readback)
	// 5. getRole (ACL readback)
	// 6. setClientPassword:gateway_001
	// 7. enableClient:gateway_001
	// 8. getClient:gateway_001 (post-mutation check)
	// 9. getRole (post-mutation ACL check)
	expectedCalls := []string{
		"getClient:gateway_001",
		"getRole",
		"disableClient:gateway_001",
		"getClient:gateway_001",
		"getRole",
		"setClientPassword:gateway_001",
		"enableClient:gateway_001",
		"getClient:gateway_001",
		"getRole",
		"close",
	}
	if !reflect.DeepEqual(spy.calls, expectedCalls) {
		t.Fatalf("unexpected call order:\ngot:  %+v\nwant: %+v", spy.calls, expectedCalls)
	}
}

// Parameterized intermediate readback faults during Rotate (ROT1)
func TestSimplifiedDynSecExecutor_Rotate_IntermediateReadbackFaults(t *testing.T) {
	tests := []struct {
		name  string
		setup func(spy *spyDynSecClient, op *Operation) (context.Context, context.CancelFunc)
	}{
		{
			name: "disabled_get_error",
			setup: func(spy *spyDynSecClient, op *Operation) (context.Context, context.CancelFunc) {
				// baseline get is call 1; disabled get is call 2
				spy.failAfterNGet = 1
				return context.Background(), func() {}
			},
		},
		{
			name: "still_enabled_after_disable",
			setup: func(spy *spyDynSecClient, op *Operation) (context.Context, context.CancelFunc) {
				spy.onDisableHook = func(u string) {
					// Client remains enabled despite DisableClient call
					m := spy.clients[u]
					m.Disabled = false
					spy.clients[u] = m
				}
				return context.Background(), func() {}
			},
		},
		{
			name: "wrong_role_on_disabled_readback",
			setup: func(spy *spyDynSecClient, op *Operation) (context.Context, context.CancelFunc) {
				spy.onDisableHook = func(u string) {
					// Corrupt role on disabled readback
					m := spy.clients[u]
					m.Roles = []string{"gateway_wrong_role"}
					spy.clients[u] = m
				}
				return context.Background(), func() {}
			},
		},
		{
			name: "acl_readback_error",
			setup: func(spy *spyDynSecClient, op *Operation) (context.Context, context.CancelFunc) {
				spy.onDisableHook = func(u string) {
					// Corrupt ACL role so ensureRole(create=false) readback fails
					delete(spy.roles, "gateway_"+u)
				}
				return context.Background(), func() {}
			},
		},
		{
			name: "context_cancelled_before_password",
			setup: func(spy *spyDynSecClient, op *Operation) (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				spy.onDisableHook = func(u string) {
					// Cancel context after disable call before password mutation
					cancel()
				}
				return ctx, cancel
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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

			ctx, cancel := tt.setup(spy, &op)
			defer cancel()

			res, err := exec.Execute(ctx, op, func(ctx context.Context) (string, error) {
				return "newsecret1234567890", nil
			})

			if !errors.Is(err, ErrRecoveryRequired) {
				t.Fatalf("expected ErrRecoveryRequired, got %v", err)
			}
			if res.Outcome != ExecutionRecoveryRequired {
				t.Fatalf("expected ExecutionRecoveryRequired, got %v", res.Outcome)
			}
			if !res.Mutated {
				t.Fatalf("expected Mutated=true because DisableClient was executed, got false")
			}
			// Evidence must be empty/clean (no secret evidence claimed)
			if res.Evidence.RAMApplied || res.Evidence.FreshPositiveVerified || res.Evidence.NativeDisabledObserved || res.Evidence.NativePersistedRoleACLObserved {
				t.Fatalf("expected no secret/positive/revoke evidence on fault, got %+v", res.Evidence)
			}

			// Verify that neither setClientPassword nor enableClient was ever called:
			for _, call := range spy.calls {
				if call == "setClientPassword:gateway_001" {
					t.Fatalf("setClientPassword was called despite readback fault %s", tt.name)
				}
				if call == "enableClient:gateway_001" {
					t.Fatalf("enableClient was called despite readback fault %s", tt.name)
				}
			}
		})
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

// 8. Revoke missing prerequisites guard (safety containment guard)
func TestSimplifiedDynSecExecutor_Revoke_MissingPrerequisitesGuarded(t *testing.T) {
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
		t.Fatalf("expected ErrLifecycleUnavailable on unconfigured prerequisites revoke, got %v", err)
	}
	if generatorInvoked {
		t.Fatal("password generator must NEVER be invoked on revoke")
	}
	if rejectedCalled {
		t.Fatal("rejection probe must NOT be called on unconfigured revoke")
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

	// 8f: Protected gateway ID is denied by target validation before revoke guard
	protectedOp := validOp
	protectedOp.GatewayID = "manager_user"
	res, err = exec.Execute(context.Background(), protectedOp, nil)
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput on protected gateway ID, got %v", err)
	}
	if res.Outcome != ExecutionUnchangedFailure || res.Mutated {
		t.Fatalf("expected unchanged unmutated failure on protected gateway ID, got %+v", res)
	}

	// 8g: Qualified executor but missing verifier keeps revoke guarded with zero mutations/callbacks
	qual, qErr := NewNativeDisableQualification(
		QualifiedMosquittoImageRef,
		QualifiedMosquittoConfigID,
		QualifiedMosquittoPlatform,
	)
	if qErr != nil {
		t.Fatalf("failed to create valid qualification: %v", qErr)
	}
	cfgQualified := defaultTestConfig(spy)
	cfgQualified.Qualification = qual
	// cfgQualified.VerifyRevoked is deliberately nil here
	newClientCalledQual := 0
	origNewClientQual := cfgQualified.NewClient
	cfgQualified.NewClient = func(ctx context.Context) (SimplifiedDynSecClient, error) {
		newClientCalledQual++
		return origNewClientQual(ctx)
	}
	execQualified, err := NewSimplifiedDynSecExecutor(cfgQualified)
	if err != nil {
		t.Fatalf("unexpected error creating qualified executor: %v", err)
	}
	resQual, errQual := execQualified.Execute(context.Background(), validOp, nil)
	if !errors.Is(errQual, ErrLifecycleUnavailable) {
		t.Fatalf("expected ErrLifecycleUnavailable on qualified executor without verifier, got %v", errQual)
	}
	if newClientCalledQual != 0 {
		t.Fatalf("expected 0 NewClient calls on qualified executor without verifier, got %d", newClientCalledQual)
	}
	if resQual.Mutated {
		t.Fatal("qualified executor revoke must NOT mutate")
	}
	if resQual.Outcome != ExecutionUnchangedFailure {
		t.Fatalf("expected ExecutionUnchangedFailure on qualified executor without verifier, got %v", resQual.Outcome)
	}
	if resQual.Evidence != (SimplifiedExecutionEvidence{}) {
		t.Fatalf("expected zero evidence on qualified executor without verifier, got %+v", resQual.Evidence)
	}
}

// 8h: Public Execute(ActionRevoke) succeeds when qualified and verifier supplied, with NIL generator
func TestSimplifiedDynSecExecutor_PublicRevoke_QualifiedSuccess(t *testing.T) {
	qual, err := NewNativeDisableQualification(
		QualifiedMosquittoImageRef,
		QualifiedMosquittoConfigID,
		QualifiedMosquittoPlatform,
	)
	if err != nil {
		t.Fatalf("failed to create qualification: %v", err)
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

	t.Run("SuccessWithNilGeneratorAndExactEvidence", func(t *testing.T) {
		spy := newSpyClient()
		spy.clients["gateway_001"] = DynSecClientMetadata{
			Username: "gateway_001",
			Disabled: false,
			Roles:    []string{"gateway_gateway_001"},
		}
		spy.roles["gateway_gateway_001"] = adapterGatewayACLs("gateway_001")

		cfg := defaultTestConfig(spy)
		cfg.Qualification = qual
		verifierCalled := false
		cfg.VerifyRevoked = func(ctx context.Context, target string) error {
			if target != "gateway_001" {
				t.Fatalf("expected target gateway_001, got %s", target)
			}
			verifierCalled = true
			return nil
		}

		loginCalls := 0
		cfg.Login = func(ctx context.Context, u, pw string) error {
			loginCalls++
			return nil
		}
		rejectedCalls := 0
		cfg.Rejected = func(ctx context.Context, u, pw string) error {
			rejectedCalls++
			return nil
		}

		exec, err := NewSimplifiedDynSecExecutor(cfg)
		if err != nil {
			t.Fatalf("unexpected NewSimplifiedDynSecExecutor error: %v", err)
		}

		// Public Execute with NIL generator must succeed
		res, err := exec.Execute(context.Background(), validOp, nil)
		if err != nil {
			t.Fatalf("expected success with nil generator, got error: %v", err)
		}
		if res.Outcome != ExecutionVerifiedSuccess {
			t.Fatalf("expected ExecutionVerifiedSuccess, got %v", res.Outcome)
		}
		if !res.Mutated {
			t.Fatal("expected Mutated == true on successful revoke")
		}
		if !verifierCalled {
			t.Fatal("expected VerifyRevoked to be called")
		}
		if loginCalls != 0 {
			t.Fatalf("Login must never be called during revoke, got %d", loginCalls)
		}
		if rejectedCalls != 0 {
			t.Fatalf("Rejected must never be called during revoke, got %d", rejectedCalls)
		}

		// Exact evidence checks
		if !res.Evidence.RAMApplied {
			t.Fatal("expected RAMApplied == true")
		}
		if !res.Evidence.NativeDisabledObserved {
			t.Fatal("expected NativeDisabledObserved == true")
		}
		if !res.Evidence.NativePersistedRoleACLObserved {
			t.Fatal("expected NativePersistedRoleACLObserved == true")
		}
		if res.Evidence.Qualification != qual {
			t.Fatalf("expected qualification %+v, got %+v", qual, res.Evidence.Qualification)
		}
		if res.Evidence.FreshPositiveVerified || res.Evidence.ProtocolDenialVerified || res.Evidence.SessionHandlingVerified {
			t.Fatalf("expected legacy probe fields to be false, got %+v", res.Evidence)
		}

		// Exactly one disable in spy calls
		disableCalls := 0
		for _, call := range spy.calls {
			if call == "disableClient:gateway_001" {
				disableCalls++
			}
			if call == "setClientPassword" || call == "createClient" || call == "deleteClient" {
				t.Fatalf("forbidden broker mutation called: %s", call)
			}
		}
		if disableCalls != 1 {
			t.Fatalf("expected exactly 1 disableClient call, got %d", disableCalls)
		}
	})

	t.Run("SuppliedGeneratorSpyNeverInvoked", func(t *testing.T) {
		spy := newSpyClient()
		spy.clients["gateway_001"] = DynSecClientMetadata{
			Username: "gateway_001",
			Disabled: false,
			Roles:    []string{"gateway_gateway_001"},
		}
		spy.roles["gateway_gateway_001"] = adapterGatewayACLs("gateway_001")

		cfg := defaultTestConfig(spy)
		cfg.Qualification = qual
		cfg.VerifyRevoked = func(ctx context.Context, target string) error { return nil }

		exec, err := NewSimplifiedDynSecExecutor(cfg)
		if err != nil {
			t.Fatalf("unexpected NewSimplifiedDynSecExecutor error: %v", err)
		}

		generatorCalls := 0
		gen := func(ctx context.Context) (string, error) {
			generatorCalls++
			return "nonnil_generator_secret", nil
		}

		res, err := exec.Execute(context.Background(), validOp, gen)
		if err != nil {
			t.Fatalf("expected success with nonnil generator, got error: %v", err)
		}
		if res.Outcome != ExecutionVerifiedSuccess {
			t.Fatalf("expected ExecutionVerifiedSuccess, got %v", res.Outcome)
		}
		if generatorCalls != 0 {
			t.Fatalf("supplied generator MUST NEVER be invoked during revoke, got %d", generatorCalls)
		}
	})
}

func TestSimplifiedDynSecExecutor_PrivateRevoke(t *testing.T) {
	// RED test stub for E2 private revoke
	qual, err := NewNativeDisableQualification(
		QualifiedMosquittoImageRef,
		QualifiedMosquittoConfigID,
		QualifiedMosquittoPlatform,
	)
	if err != nil {
		t.Fatalf("failed to create qualification: %v", err)
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

	t.Run("SuccessCallOrderAndExactEvidence", func(t *testing.T) {
		spy := newSpyClient()
		spy.clients["gateway_001"] = DynSecClientMetadata{
			Username: "gateway_001",
			Disabled: false,
			Roles:    []string{"gateway_gateway_001"},
		}
		spy.roles["gateway_gateway_001"] = adapterGatewayACLs("gateway_001")

		cfg := defaultTestConfig(spy)
		cfg.Qualification = qual
		verifierCalled := false
		cfg.VerifyRevoked = func(ctx context.Context, target string) error {
			if target != "gateway_001" {
				t.Fatalf("expected target gateway_001, got %s", target)
			}
			verifierCalled = true
			return nil
		}

		exec, err := NewSimplifiedDynSecExecutor(cfg)
		if err != nil {
			t.Fatalf("unexpected NewSimplifiedDynSecExecutor error: %v", err)
		}

		res, err := exec.executeRevoke(context.Background(), validOp)
		if err != nil {
			t.Fatalf("expected success, got error: %v", err)
		}
		if res.Outcome != ExecutionVerifiedSuccess {
			t.Fatalf("expected ExecutionVerifiedSuccess, got %v", res.Outcome)
		}
		if !res.Mutated {
			t.Fatal("expected Mutated == true on successful revoke")
		}
		if !verifierCalled {
			t.Fatal("expected VerifyRevoked to be called")
		}

		// Exact evidence checks
		if !res.Evidence.RAMApplied {
			t.Fatal("expected RAMApplied == true")
		}
		if !res.Evidence.NativeDisabledObserved {
			t.Fatal("expected NativeDisabledObserved == true")
		}
		if !res.Evidence.NativePersistedRoleACLObserved {
			t.Fatal("expected NativePersistedRoleACLObserved == true")
		}
		if res.Evidence.Qualification != qual {
			t.Fatalf("expected qualification %+v, got %+v", qual, res.Evidence.Qualification)
		}
		if res.Evidence.FreshPositiveVerified || res.Evidence.ProtocolDenialVerified || res.Evidence.SessionHandlingVerified {
			t.Fatalf("expected legacy probe fields to be false, got %+v", res.Evidence)
		}

		// Calls trace verification
		expectedCalls := []string{
			"getClient:gateway_001",
			"getRole",
			"disableClient:gateway_001",
			"getClient:gateway_001",
			"getRole",
			"close",
		}
		if len(spy.calls) != len(expectedCalls) {
			t.Fatalf("expected %d calls %v, got %d %v", len(expectedCalls), expectedCalls, len(spy.calls), spy.calls)
		}
		for i, call := range expectedCalls {
			if spy.calls[i] != call {
				t.Fatalf("call[%d]: expected %s, got %s", i, call, spy.calls[i])
			}
		}
	})

	t.Run("MissingCapabilityOrVerifierOrInvalidTarget_ZeroCallbackAndZeroMutation", func(t *testing.T) {
		spy := newSpyClient()
		spy.clients["gateway_001"] = DynSecClientMetadata{
			Username: "gateway_001",
			Disabled: false,
			Roles:    []string{"gateway_gateway_001"},
		}
		spy.roles["gateway_gateway_001"] = adapterGatewayACLs("gateway_001")

		// 1. Missing qualification
		cfgNoQual := defaultTestConfig(spy)
		cfgNoQual.VerifyRevoked = func(ctx context.Context, target string) error { return nil }
		execNoQual, err := NewSimplifiedDynSecExecutor(cfgNoQual)
		if err != nil {
			t.Fatal(err)
		}
		res, err := execNoQual.executeRevoke(context.Background(), validOp)
		if !errors.Is(err, ErrLifecycleUnavailable) {
			t.Fatalf("expected ErrLifecycleUnavailable on missing qualification, got %v", err)
		}
		if res.Mutated || res.Outcome != ExecutionUnchangedFailure {
			t.Fatalf("expected unchanged unmutated result, got %+v", res)
		}
		if len(spy.calls) != 0 {
			t.Fatalf("expected 0 spy calls on missing qualification, got %v", spy.calls)
		}

		// 2. Missing verifier
		cfgNoVer := defaultTestConfig(spy)
		cfgNoVer.Qualification = qual
		execNoVer, err := NewSimplifiedDynSecExecutor(cfgNoVer)
		if err != nil {
			t.Fatal(err)
		}
		res, err = execNoVer.executeRevoke(context.Background(), validOp)
		if !errors.Is(err, ErrLifecycleUnavailable) {
			t.Fatalf("expected ErrLifecycleUnavailable on missing verifier, got %v", err)
		}
		if res.Mutated || res.Outcome != ExecutionUnchangedFailure {
			t.Fatalf("expected unchanged unmutated result, got %+v", res)
		}
		if len(spy.calls) != 0 {
			t.Fatalf("expected 0 spy calls on missing verifier, got %v", spy.calls)
		}

		// 3. Invalid target: protected user
		cfgValid := defaultTestConfig(spy)
		cfgValid.Qualification = qual
		cfgValid.VerifyRevoked = func(ctx context.Context, target string) error { return nil }
		execValid, err := NewSimplifiedDynSecExecutor(cfgValid)
		if err != nil {
			t.Fatal(err)
		}
		protectedOp := validOp
		protectedOp.GatewayID = "manager_user"
		res, err = execValid.executeRevoke(context.Background(), protectedOp)
		if !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("expected ErrInvalidInput on protected target, got %v", err)
		}
		if res.Mutated || res.Outcome != ExecutionUnchangedFailure {
			t.Fatalf("expected unchanged unmutated result, got %+v", res)
		}
		if len(spy.calls) != 0 {
			t.Fatalf("expected 0 spy calls on protected target, got %v", spy.calls)
		}

		// 4. Invalid target: non-revoke action
		nonRevokeOp := validOp
		nonRevokeOp.Action = ActionProvision
		res, err = execValid.executeRevoke(context.Background(), nonRevokeOp)
		if !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("expected ErrInvalidInput on non-revoke action, got %v", err)
		}
		if res.Mutated || res.Outcome != ExecutionUnchangedFailure {
			t.Fatalf("expected unchanged unmutated result, got %+v", res)
		}
		if len(spy.calls) != 0 {
			t.Fatalf("expected 0 spy calls on non-revoke action, got %v", spy.calls)
		}

		// 5. Invalid target: missing Previous
		noPrevOp := validOp
		noPrevOp.Previous = nil
		res, err = execValid.executeRevoke(context.Background(), noPrevOp)
		if !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("expected ErrInvalidInput on missing Previous, got %v", err)
		}
		if res.Mutated || res.Outcome != ExecutionUnchangedFailure {
			t.Fatalf("expected unchanged unmutated result, got %+v", res)
		}
		if len(spy.calls) != 0 {
			t.Fatalf("expected 0 spy calls on missing Previous, got %v", spy.calls)
		}
	})

	t.Run("BaselineDrift_NoMutationAndRecoveryRequired", func(t *testing.T) {
		// Case A: Client absent in baseline
		spyA := newSpyClient()
		cfgA := defaultTestConfig(spyA)
		cfgA.Qualification = qual
		cfgA.VerifyRevoked = func(ctx context.Context, target string) error { return nil }
		execA, _ := NewSimplifiedDynSecExecutor(cfgA)
		resA, errA := execA.executeRevoke(context.Background(), validOp)
		if !errors.Is(errA, ErrRecoveryRequired) {
			t.Fatalf("expected ErrRecoveryRequired on absent baseline, got %v", errA)
		}
		if resA.Outcome != ExecutionRecoveryRequired || resA.Mutated {
			t.Fatalf("expected recovery required and unmutated on absent baseline, got %+v", resA)
		}

		// Case B: Client already disabled in baseline
		spyB := newSpyClient()
		spyB.clients["gateway_001"] = DynSecClientMetadata{
			Username: "gateway_001",
			Disabled: true,
			Roles:    []string{"gateway_gateway_001"},
		}
		spyB.roles["gateway_gateway_001"] = adapterGatewayACLs("gateway_001")
		cfgB := defaultTestConfig(spyB)
		cfgB.Qualification = qual
		cfgB.VerifyRevoked = func(ctx context.Context, target string) error { return nil }
		execB, _ := NewSimplifiedDynSecExecutor(cfgB)
		resB, errB := execB.executeRevoke(context.Background(), validOp)
		if !errors.Is(errB, ErrRecoveryRequired) {
			t.Fatalf("expected ErrRecoveryRequired on already disabled baseline, got %v", errB)
		}
		if resB.Outcome != ExecutionRecoveryRequired || resB.Mutated {
			t.Fatalf("expected recovery required and unmutated on already disabled baseline, got %+v", resB)
		}

		// Case C: Role ACL drift in baseline
		spyC := newSpyClient()
		spyC.clients["gateway_001"] = DynSecClientMetadata{
			Username: "gateway_001",
			Disabled: false,
			Roles:    []string{"gateway_gateway_001"},
		}
		// missing or corrupted role
		cfgC := defaultTestConfig(spyC)
		cfgC.Qualification = qual
		cfgC.VerifyRevoked = func(ctx context.Context, target string) error { return nil }
		execC, _ := NewSimplifiedDynSecExecutor(cfgC)
		resC, errC := execC.executeRevoke(context.Background(), validOp)
		if !errors.Is(errC, ErrRecoveryRequired) {
			t.Fatalf("expected ErrRecoveryRequired on baseline ACL drift, got %v", errC)
		}
		if resC.Outcome != ExecutionRecoveryRequired || resC.Mutated {
			t.Fatalf("expected recovery required and unmutated on baseline ACL drift, got %+v", resC)
		}
	})

	t.Run("PostMutationFailures_MarkMutatedAndRecoveryRequired", func(t *testing.T) {
		// 1. DisableClient failure
		spy1 := newSpyClient()
		spy1.clients["gateway_001"] = DynSecClientMetadata{
			Username: "gateway_001",
			Disabled: false,
			Roles:    []string{"gateway_gateway_001"},
		}
		spy1.roles["gateway_gateway_001"] = adapterGatewayACLs("gateway_001")
		spy1.failOnCmd["disableClient"] = errors.New("network drop during disable")

		cfg1 := defaultTestConfig(spy1)
		cfg1.Qualification = qual
		cfg1.VerifyRevoked = func(ctx context.Context, target string) error { return nil }
		exec1, _ := NewSimplifiedDynSecExecutor(cfg1)

		res1, err1 := exec1.executeRevoke(context.Background(), validOp)
		if !errors.Is(err1, ErrRecoveryRequired) {
			t.Fatalf("expected ErrRecoveryRequired on disableClient failure, got %v", err1)
		}
		if !res1.Mutated {
			t.Fatal("expected Mutated == true on disableClient failure")
		}
		if res1.Outcome != ExecutionRecoveryRequired {
			t.Fatalf("expected ExecutionRecoveryRequired, got %v", res1.Outcome)
		}
		if res1.Evidence != (SimplifiedExecutionEvidence{}) {
			t.Fatalf("expected zero evidence on disable failure, got %+v", res1.Evidence)
		}

		// 2. Disabled readback mismatch (fail query after mutation)
		spy2 := newSpyClient()
		spy2.clients["gateway_001"] = DynSecClientMetadata{
			Username: "gateway_001",
			Disabled: false,
			Roles:    []string{"gateway_gateway_001"},
		}
		spy2.roles["gateway_gateway_001"] = adapterGatewayACLs("gateway_001")
		spy2.failAfterNGet = 1 // 1st get succeeds (baseline), 2nd get fails

		cfg2 := defaultTestConfig(spy2)
		cfg2.Qualification = qual
		cfg2.VerifyRevoked = func(ctx context.Context, target string) error { return nil }
		exec2, _ := NewSimplifiedDynSecExecutor(cfg2)

		res2, err2 := exec2.executeRevoke(context.Background(), validOp)
		if !errors.Is(err2, ErrRecoveryRequired) {
			t.Fatalf("expected ErrRecoveryRequired on readback query error, got %v", err2)
		}
		if !res2.Mutated || res2.Outcome != ExecutionRecoveryRequired {
			t.Fatalf("expected mutated recovery required, got %+v", res2)
		}
		if res2.Evidence != (SimplifiedExecutionEvidence{}) {
			t.Fatalf("expected zero evidence, got %+v", res2.Evidence)
		}

		// 3. Role ACL check error after disable
		spy3 := newSpyClient()
		spy3.clients["gateway_001"] = DynSecClientMetadata{
			Username: "gateway_001",
			Disabled: false,
			Roles:    []string{"gateway_gateway_001"},
		}
		spy3.roles["gateway_gateway_001"] = adapterGatewayACLs("gateway_001")

		cfg3 := defaultTestConfig(spy3)
		cfg3.Qualification = qual
		cfg3.VerifyRevoked = func(ctx context.Context, target string) error { return nil }
		exec3, _ := NewSimplifiedDynSecExecutor(cfg3)

		// Cause getRole to fail on the second call (after disableClient)
		origDisable := spy3.DisableClient
		_ = origDisable
		// When disableClient is called, we delete the role so second getRole fails
		spy3.roleAbsentOn = "" // baseline ok
		// We can intercept by wrapping NewClient or by setting roleAbsentOn inside test:
		// Better: custom client wrapper or simply custom failOnCmd set during disable
		// Let's test role ACL failure after disable:
		clientWithRoleFailure := &spyDynSecClientWithAfterDisableRoleFailure{
			spyDynSecClient: spy3,
		}
		cfg3.NewClient = func(ctx context.Context) (SimplifiedDynSecClient, error) {
			return clientWithRoleFailure, nil
		}
		exec3, _ = NewSimplifiedDynSecExecutor(cfg3)
		res3, err3 := exec3.executeRevoke(context.Background(), validOp)
		if !errors.Is(err3, ErrRecoveryRequired) {
			t.Fatalf("expected ErrRecoveryRequired on role failure after disable, got %v", err3)
		}
		if !res3.Mutated || res3.Outcome != ExecutionRecoveryRequired {
			t.Fatalf("expected mutated recovery required on role failure, got %+v", res3)
		}
		if res3.Evidence.NativePersistedRoleACLObserved {
			t.Fatal("NativePersistedRoleACLObserved must not be true on role failure")
		}

		// 4. Persistence verifier error
		spy4 := newSpyClient()
		spy4.clients["gateway_001"] = DynSecClientMetadata{
			Username: "gateway_001",
			Disabled: false,
			Roles:    []string{"gateway_gateway_001"},
		}
		spy4.roles["gateway_gateway_001"] = adapterGatewayACLs("gateway_001")

		cfg4 := defaultTestConfig(spy4)
		cfg4.Qualification = qual
		cfg4.VerifyRevoked = func(ctx context.Context, target string) error {
			return errors.New("snapshot verification failed")
		}
		exec4, _ := NewSimplifiedDynSecExecutor(cfg4)

		res4, err4 := exec4.executeRevoke(context.Background(), validOp)
		if !errors.Is(err4, ErrRecoveryRequired) {
			t.Fatalf("expected ErrRecoveryRequired on persistence verifier failure, got %v", err4)
		}
		if !res4.Mutated || res4.Outcome != ExecutionRecoveryRequired {
			t.Fatalf("expected mutated recovery required, got %+v", res4)
		}
		// RAMApplied and NativeDisabledObserved were recorded before verifier, but NativePersistedRoleACLObserved must NOT be set
		if res4.Evidence.NativePersistedRoleACLObserved {
			t.Fatal("NativePersistedRoleACLObserved must be false when persistence verifier fails")
		}
		if res4.Evidence.Qualification != (NativeDisableQualification{}) {
			t.Fatal("Qualification must be zero when persistence verifier fails")
		}

		// 5. Context cancelled after mutation / verifier
		spy5 := newSpyClient()
		spy5.clients["gateway_001"] = DynSecClientMetadata{
			Username: "gateway_001",
			Disabled: false,
			Roles:    []string{"gateway_gateway_001"},
		}
		spy5.roles["gateway_gateway_001"] = adapterGatewayACLs("gateway_001")

		cfg5 := defaultTestConfig(spy5)
		cfg5.Qualification = qual
		cancelCtx, cancelFn := context.WithCancel(context.Background())
		cfg5.VerifyRevoked = func(ctx context.Context, target string) error {
			cancelFn()
			return nil
		}
		exec5, _ := NewSimplifiedDynSecExecutor(cfg5)

		res5, err5 := exec5.executeRevoke(cancelCtx, validOp)
		if !errors.Is(err5, ErrRecoveryRequired) {
			t.Fatalf("expected ErrRecoveryRequired on context cancellation after mutation, got %v", err5)
		}
		if !res5.Mutated || res5.Outcome != ExecutionRecoveryRequired {
			t.Fatalf("expected mutated recovery required on cancel, got %+v", res5)
		}
	})
}

// TestNativeDisableQualification tests the narrow qualification descriptor constructor,
// validation methods, and config validation rules for Task 2.8a E1.
func TestNativeDisableQualification(t *testing.T) {
	// Exact qualified identity succeeds
	qual, err := NewNativeDisableQualification(
		QualifiedMosquittoImageRef,
		QualifiedMosquittoConfigID,
		QualifiedMosquittoPlatform,
	)
	if err != nil {
		t.Fatalf("expected success for exact qualified identity, got %v", err)
	}
	if qual.IsZero() {
		t.Fatal("expected non-zero qualification descriptor")
	}
	if !qual.Valid() {
		t.Fatal("expected Valid() == true for exact qualified descriptor")
	}
	if qual.ImageRef() != QualifiedMosquittoImageRef {
		t.Fatalf("unexpected ImageRef: %s", qual.ImageRef())
	}
	if qual.ConfigID() != QualifiedMosquittoConfigID {
		t.Fatalf("unexpected ConfigID: %s", qual.ConfigID())
	}
	if qual.Platform() != QualifiedMosquittoPlatform {
		t.Fatalf("unexpected Platform: %s", qual.Platform())
	}
	if qual.Version() != QualifiedMosquittoVersion {
		t.Fatalf("unexpected Version: %s", qual.Version())
	}

	// Rejections for missing/mismatched/unpinned/latest/unsupported platform
	cases := []struct {
		name     string
		imageRef string
		configID string
		platform string
	}{
		{"empty imageRef", "", QualifiedMosquittoConfigID, QualifiedMosquittoPlatform},
		{"empty configID", QualifiedMosquittoImageRef, "", QualifiedMosquittoPlatform},
		{"empty platform", QualifiedMosquittoImageRef, QualifiedMosquittoConfigID, ""},
		{"latest tag", "eclipse-mosquitto:latest", QualifiedMosquittoConfigID, QualifiedMosquittoPlatform},
		{"unpinned image", "eclipse-mosquitto:2.0.18", QualifiedMosquittoConfigID, QualifiedMosquittoPlatform},
		{"mismatched image hash", "eclipse-mosquitto@sha256:0000000000000000000000000000000000000000000000000000000000000000", QualifiedMosquittoConfigID, QualifiedMosquittoPlatform},
		{"mismatched configID", QualifiedMosquittoImageRef, "sha256:1111111111111111111111111111111111111111111111111111111111111111", QualifiedMosquittoPlatform},
		{"unsupported platform darwin/arm64", QualifiedMosquittoImageRef, QualifiedMosquittoConfigID, "darwin/arm64"},
		{"unsupported platform linux/arm64", QualifiedMosquittoImageRef, QualifiedMosquittoConfigID, "linux/arm64"},
		{"all mismatched", "unknown", "unknown", "unknown"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			badQual, bErr := NewNativeDisableQualification(tc.imageRef, tc.configID, tc.platform)
			if !errors.Is(bErr, ErrInvalidInput) {
				t.Fatalf("expected ErrInvalidInput, got %v", bErr)
			}
			if !badQual.IsZero() {
				t.Fatalf("expected zero value on error, got %+v", badQual)
			}
			if badQual.Valid() {
				t.Fatal("expected Valid() == false on bad qualification")
			}
		})
	}

	// Zero descriptor
	var zeroQual NativeDisableQualification
	if !zeroQual.IsZero() {
		t.Fatal("expected IsZero() == true for zero value")
	}
	if zeroQual.Valid() {
		t.Fatal("expected Valid() == false for zero value")
	}

	// Config validation: zero descriptor preserves constructor for non-revoke calls
	spy := newSpyClient()
	cfg := defaultTestConfig(spy)
	cfg.Qualification = NativeDisableQualification{}
	execZero, err := NewSimplifiedDynSecExecutor(cfg)
	if err != nil {
		t.Fatalf("expected zero qualification to allow executor creation, got %v", err)
	}
	if execZero == nil {
		t.Fatal("expected non-nil executor")
	}

	// Config validation: malformed nonzero qualification descriptor rejected before callbacks
	cfgMalformed := defaultTestConfig(spy)
	newClientCalled := false
	cfgMalformed.NewClient = func(ctx context.Context) (SimplifiedDynSecClient, error) {
		newClientCalled = true
		return spy, nil
	}
	// Synthetic nonzero qualification with tampered/empty fields
	cfgMalformed.Qualification = NativeDisableQualification{
		imageRef: "tampered",
	}
	execMalformed, mErr := NewSimplifiedDynSecExecutor(cfgMalformed)
	if !errors.Is(mErr, ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput on malformed qualification, got %v", mErr)
	}
	if execMalformed != nil {
		t.Fatalf("expected nil executor on malformed qualification, got %v", execMalformed)
	}
	if newClientCalled {
		t.Fatal("NewClient must NOT be called when qualification is malformed")
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
			if fault == "setClientPassword" || fault == "enableClient" {
				// Also test on ActionRotate to ensure Rotate's password/enable faults
				// behave identically: Mutated=true, ExecutionRecoveryRequired, no retry/compensation.
				act = ActionRotate
			}

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
			if act == ActionRotate {
				op.CredentialVersion = 2
				op.Previous = &CredentialSnapshot{
					Status:            CredentialActive,
					CredentialVersion: 1,
				}
				spy.clients["gateway_001"] = DynSecClientMetadata{
					Username: "gateway_001",
					Disabled: false,
					Roles:    []string{"gateway_gateway_001"},
				}
				spy.roles["gateway_gateway_001"] = adapterGatewayACLs("gateway_001")
				spy.roleAbsentOn = ""
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

	// Subtest 1: Missing prerequisites fail before client allocation
	// exec was created with defaultTestConfig which has zero Qualification and nil VerifyRevoked
	if err := exec.CheckRevoked(context.Background(), "gateway_001"); !errors.Is(err, ErrLifecycleUnavailable) {
		t.Fatalf("expected ErrLifecycleUnavailable on missing prerequisites, got %v", err)
	}

	// Create qualified executor with verifier for CheckRevoked tests
	qualCheck, qErr := NewNativeDisableQualification(
		QualifiedMosquittoImageRef,
		QualifiedMosquittoConfigID,
		QualifiedMosquittoPlatform,
	)
	if qErr != nil {
		t.Fatalf("failed to create qualification: %v", qErr)
	}
	cfgCheck := defaultTestConfig(spy)
	cfgCheck.Qualification = qualCheck
	verifierCheckCalls := 0
	var verifierCheckErr error
	cfgCheck.VerifyRevoked = func(ctx context.Context, target string) error {
		verifierCheckCalls++
		return verifierCheckErr
	}
	execCheck, err := NewSimplifiedDynSecExecutor(cfgCheck)
	if err != nil {
		t.Fatalf("unexpected NewSimplifiedDynSecExecutor error: %v", err)
	}

	// Missing dependency zero client allocation test:
	// Verify that if qualification is zero, NewClient is never called.
	cfgZeroQual := defaultTestConfig(spy)
	cfgZeroQual.VerifyRevoked = func(ctx context.Context, target string) error { return nil }
	newClientCallsZero := 0
	origNewClientZero := cfgZeroQual.NewClient
	cfgZeroQual.NewClient = func(ctx context.Context) (SimplifiedDynSecClient, error) {
		newClientCallsZero++
		return origNewClientZero(ctx)
	}
	execZeroQual, _ := NewSimplifiedDynSecExecutor(cfgZeroQual)
	if err := execZeroQual.CheckRevoked(context.Background(), "gateway_001"); !errors.Is(err, ErrLifecycleUnavailable) {
		t.Fatalf("expected ErrLifecycleUnavailable when qualification zero, got %v", err)
	}
	if newClientCallsZero != 0 {
		t.Fatalf("expected 0 NewClient calls when qualification zero, got %d", newClientCallsZero)
	}

	// Success case: live readback + snapshot verifier succeed
	if err := execCheck.CheckRevoked(context.Background(), "gateway_001"); err != nil {
		t.Fatalf("expected CheckRevoked success, got %v", err)
	}
	if verifierCheckCalls != 1 {
		t.Fatalf("expected VerifyRevoked to be called once, got %d", verifierCheckCalls)
	}

	if err := execCheck.CheckRevoked(context.Background(), "invalid/id"); err != ErrInvalidInput {
		t.Fatalf("expected ErrInvalidInput on invalid gateway ID, got %v", err)
	}
	if err := execCheck.CheckRevoked(context.Background(), BackendUsername); err != ErrInvalidInput {
		t.Fatalf("expected ErrInvalidInput on protected username, got %v", err)
	}
	// Active client fails CheckRevoked
	spy.clients["gateway_001"] = DynSecClientMetadata{
		Username: "gateway_001",
		Disabled: false,
		Roles:    []string{"gateway_gateway_001"},
	}
	if err := execCheck.CheckRevoked(context.Background(), "gateway_001"); err != ErrRecoveryRequired {
		t.Fatalf("expected ErrRecoveryRequired on non-disabled client, got %v", err)
	}

	// Persisted mismatch returns recovery
	spy.clients["gateway_001"] = DynSecClientMetadata{
		Username: "gateway_001",
		Disabled: true,
		Roles:    []string{"gateway_gateway_001"},
	}
	verifierCheckErr = errors.New("snapshot mismatch")
	if err := execCheck.CheckRevoked(context.Background(), "gateway_001"); err != ErrRecoveryRequired {
		t.Fatalf("expected ErrRecoveryRequired on persistence mismatch, got %v", err)
	}
}
