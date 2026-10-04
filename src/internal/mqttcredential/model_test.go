package mqttcredential

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestModelMutationInput(t *testing.T) {
	actor := uuid.New()
	valid := MutationInput{GatewayID: "fixture_gateway", IdempotencyKey: uuid.New(), Action: ActionProvision}
	if err := ValidateMutationInput(actor, valid); err != nil {
		t.Fatal(err)
	}
	for _, action := range []Action{ActionProvision, ActionRotate, ActionRevoke} {
		input := valid
		input.Action = action
		if err := ValidateMutationInput(actor, input); err != nil {
			t.Fatal(err)
		}
	}
	cases := []struct {
		name  string
		actor uuid.UUID
		input MutationInput
	}{
		{"nil actor", uuid.Nil, valid},
		{"nil key", actor, MutationInput{GatewayID: valid.GatewayID, Action: ActionRotate}},
		{"empty action", actor, MutationInput{GatewayID: valid.GatewayID, IdempotencyKey: valid.IdempotencyKey}},
		{"unknown action", actor, MutationInput{GatewayID: valid.GatewayID, IdempotencyKey: valid.IdempotencyKey, Action: "recover"}},
	}
	for _, id := range []string{"", "backend_service", "../fixture", "a/b", " a", "a+", strings.Repeat("a", 65)} {
		input := valid
		input.GatewayID = id
		cases = append(cases, struct {
			name  string
			actor uuid.UUID
			input MutationInput
		}{"gateway " + id, actor, input})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateMutationInput(tc.actor, tc.input)
			var domainErr *DomainError
			if !errors.Is(err, ErrInvalidInput) || !errors.As(err, &domainErr) || domainErr.Code != CodeInvalidRequest {
				t.Fatalf("expected typed invalid input, got %v", err)
			}
		})
	}
	// Input is a use-case contract, not a JSON body accepted from clients.
	typ := reflect.TypeOf(valid)
	for _, forbidden := range []string{"Password", "Username", "Actor", "CredentialVersion", "OperationID"} {
		if _, ok := typ.FieldByName(forbidden); ok {
			t.Fatalf("client input contains %s", forbidden)
		}
	}
}

func TestModelEnumValidation(t *testing.T) {
	groups := []struct {
		name     string
		validate func(string) error
		valid    []string
	}{
		{"action", func(s string) error { return Action(s).Validate() }, []string{"provision", "rotate", "revoke"}},
		{"credential", func(s string) error { return CredentialStatus(s).Validate() }, []string{"provisioning", "active", "rotating", "revoking", "revoked", "failed", "recovery_needed"}},
		{"operation", func(s string) error { return OperationStatus(s).Validate() }, []string{"pending", "succeeded", "failed", "recovery_needed"}},
		{"phase", func(s string) error { return OperationPhase(s).Validate() }, []string{"intent", "dynsec_mutation", "snapshot_readback", "finalize", "recovery"}},
		{"delivery", func(s string) error { return DeliveryStatus(s).Validate() }, []string{"unknown", "device_updated"}},
		{"execution", func(s string) error { return ExecutionOutcome(s).Validate() }, []string{"verified_success", "unchanged_failure", "recovery_required", "maintenance_closed"}},
	}
	for _, group := range groups {
		t.Run(group.name, func(t *testing.T) {
			for _, v := range group.valid {
				if err := group.validate(v); err != nil {
					t.Fatalf("valid %s: %v", v, err)
				}
				b, err := json.Marshal(v)
				if err != nil || string(b) != `"`+v+`"` {
					t.Fatal("enum wire spelling changed")
				}
			}
			for _, v := range []string{"", "ACTIVE", "snapshot_observed", "password_fixture"} {
				if !errors.Is(group.validate(v), ErrInvalidInput) {
					t.Fatalf("accepted invalid %s", v)
				}
			}
		})
	}
	if OperationStatus("device_updated").Validate() == nil || CredentialStatus("pending").Validate() == nil {
		t.Fatal("semantic groups conflated")
	}
	for _, value := range []any{ActionRotate, CredentialActive, OperationPending, PhaseSnapshotReadback, DeliveryDeviceUpdated, ExecutionVerifiedSuccess} {
		b, err := json.Marshal(value)
		if err != nil || string(b) != `"`+fmt.Sprint(value)+`"` {
			t.Fatal("typed enum JSON contract changed")
		}
	}
}

func TestModelOperationIdentity(t *testing.T) {
	op := Operation{OperationID: uuid.New(), ActorUserID: uuid.New(), GatewayID: "fixture_gateway", IdempotencyKey: uuid.New(), Action: ActionRotate, CredentialVersion: 2, Status: OperationPending, Phase: PhaseIntent, DeliveryStatus: DeliveryUnknown}
	if err := op.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*Operation){
		func(o *Operation) { o.OperationID = uuid.Nil },
		func(o *Operation) { o.ActorUserID = uuid.Nil },
		func(o *Operation) { o.IdempotencyKey = uuid.Nil },
		func(o *Operation) { o.GatewayID = "backend_service" },
		func(o *Operation) { o.Action = "unknown" },
		func(o *Operation) { o.Status = "active" },
		func(o *Operation) { o.Phase = "snapshot_observed" },
		func(o *Operation) { o.DeliveryStatus = "succeeded" },
	} {
		bad := op
		mutate(&bad)
		if !errors.Is(bad.Validate(), ErrInvalidInput) {
			t.Fatal("invalid operation accepted")
		}
	}
}

func fixtureOperation(action Action) Operation {
	now := time.Now().UTC()
	return Operation{OperationID: uuid.New(), ActorUserID: uuid.New(), GatewayID: "fixture_gateway", IdempotencyKey: uuid.New(), Action: action, CredentialVersion: 3, Status: OperationPending, Phase: PhaseIntent, DeliveryStatus: DeliveryUnknown, CreatedAt: now, UpdatedAt: now}
}

func TestModelGenerationAndAdmission(t *testing.T) {
	for _, tc := range []struct{ current, history, want int64 }{{0, 0, 1}, {2, 5, 6}, {7, 3, 8}} {
		got, err := AllocateVersion(tc.current, tc.history)
		if err != nil || got != tc.want {
			t.Fatalf("allocation %v: %d %v", tc, got, err)
		}
	}
	for _, pair := range [][2]int64{{-1, 0}, {0, -1}, {math.MaxInt64, 1}, {1, math.MaxInt64}} {
		if _, err := AllocateVersion(pair[0], pair[1]); !errors.Is(err, ErrInvalidInput) {
			t.Fatal("invalid/overflow allocation accepted")
		}
	}
	for _, tc := range []struct {
		from   CredentialStatus
		action Action
		want   CredentialStatus
	}{
		{"", ActionProvision, CredentialProvisioning}, {CredentialRevoked, ActionProvision, CredentialProvisioning},
		{CredentialFailed, ActionProvision, CredentialProvisioning}, {CredentialActive, ActionRotate, CredentialRotating},
		{CredentialActive, ActionRevoke, CredentialRevoking},
	} {
		got, err := BeginCredentialStatus(tc.from, tc.action)
		if err != nil || got != tc.want {
			t.Fatalf("admission %v: %s %v", tc, got, err)
		}
	}
	for _, from := range []CredentialStatus{CredentialProvisioning, CredentialRotating, CredentialRevoking, CredentialRecoveryNeeded} {
		if _, err := BeginCredentialStatus(from, ActionRotate); err == nil {
			t.Fatal("unresolved allowed new operation")
		}
	}
	for _, tc := range []struct {
		from   CredentialStatus
		action Action
	}{{CredentialActive, ActionProvision}, {"", ActionRotate}, {CredentialRevoked, ActionRotate}, {"bogus", ActionProvision}, {CredentialActive, "bogus"}, {CredentialRevoked, ActionRevoke}} {
		if _, err := BeginCredentialStatus(tc.from, tc.action); err == nil {
			t.Fatal("invalid admission")
		}
	}
}

func TestModelCompletionProof(t *testing.T) {
	proof := VerificationEvidence{RAMApplied: true, SnapshotObserved: true, FreshPositiveVerified: true}
	for _, action := range []Action{ActionProvision, ActionRotate, ActionRevoke} {
		op := fixtureOperation(action)
		if action != ActionProvision {
			op.Previous = &CredentialSnapshot{Status: CredentialActive, CredentialVersion: 1}
		}
		if action == ActionRevoke {
			op.CredentialVersion = 1
		}
		at := op.CreatedAt.Add(time.Second)
		for _, evidence := range []VerificationEvidence{{}, {SnapshotObserved: true}, {RAMApplied: true}, {RAMApplied: true, SnapshotObserved: true}} {
			if action == ActionRevoke && evidence.RAMApplied && evidence.SnapshotObserved {
				continue
			}
			if _, err := CompleteOperation(op, ExecutionVerifiedSuccess, evidence, at); err == nil {
				t.Fatal("checkpoint/incomplete proof became success")
			}
		}
		done, err := CompleteOperation(op, ExecutionVerifiedSuccess, proof, at)
		if err != nil || done.Operation.Status != OperationSucceeded || done.Operation.CompletedAt == nil || done.Operation.DeliveryStatus != DeliveryUnknown {
			t.Fatalf("completion: %+v %v", done, err)
		}
		want := CredentialActive
		if action == ActionRevoke {
			want = CredentialRevoked
		}
		if done.Credential.Status != want {
			t.Fatal("wrong verified state")
		}
		if _, err := CompleteOperation(done.Operation, ExecutionVerifiedSuccess, proof, at); err == nil {
			t.Fatal("terminal event mutable")
		}
		uncertain, err := CompleteOperation(op, ExecutionRecoveryRequired, VerificationEvidence{RAMApplied: true}, at)
		if err != nil || uncertain.Operation.CompletedAt != nil || uncertain.Credential.Status != CredentialRecoveryNeeded {
			t.Fatal("uncertainty finalized")
		}
		if _, err := CompleteOperation(uncertain.Operation, ExecutionUnchangedFailure, VerificationEvidence{}, at); err == nil {
			t.Fatal("fake old-password rollback")
		}
		if _, err := CompleteOperation(uncertain.Operation, ExecutionVerifiedSuccess, proof, at); err != nil {
			t.Fatal("verified recovery disallowed", err)
		}
		failed, err := CompleteOperation(op, ExecutionUnchangedFailure, VerificationEvidence{}, at)
		if err != nil || failed.Operation.Status != OperationFailed {
			t.Fatal("known failure not terminal")
		}
		if action == ActionProvision {
			if failed.Credential.Status != CredentialFailed {
				t.Fatal("failed initial provision active")
			}
		} else if failed.Credential.Status != CredentialActive || failed.Credential.CredentialVersion != 1 {
			t.Fatal("unchanged broker lost previous state")
		}
		if _, err := CompleteOperation(op, ExecutionUnchangedFailure, proof, at); err == nil {
			t.Fatal("changed broker called unchanged")
		}
		if _, err := CompleteOperation(op, ExecutionMaintenanceClosed, proof, at); err == nil {
			t.Fatal("maintenance checkpoint is success")
		}
	}
}

func TestModelSucceededOperationRequiresVerification(t *testing.T) {
	for _, action := range []Action{ActionProvision, ActionRotate, ActionRevoke} {
		op := fixtureOperation(action)
		op.Status, op.Phase, op.CompletedAt = OperationSucceeded, PhaseFinalize, &op.CreatedAt
		input := MutationInput{GatewayID: op.GatewayID, IdempotencyKey: op.IdempotencyKey, Action: action}
		meta := Metadata{GatewayID: op.GatewayID}
		for _, evidence := range []VerificationEvidence{{}, {RAMApplied: true}, {SnapshotObserved: true}, {RAMApplied: true, SnapshotObserved: true}} {
			if action == ActionRevoke && evidence.RAMApplied && evidence.SnapshotObserved {
				continue
			}
			op.Evidence = evidence
			if op.ValidateHistorical() == nil {
				t.Fatalf("%s succeeded without verification: %+v", action, evidence)
			}
			if _, err := ReplayOperation(op, op.ActorUserID, input, meta); err == nil {
				t.Fatal("unverified success replayed")
			}
		}
		op.Evidence = VerificationEvidence{RAMApplied: true, SnapshotObserved: true, FreshPositiveVerified: action != ActionRevoke}
		if err := op.ValidateHistorical(); err != nil {
			t.Fatal("verified success rejected", err)
		}
	}
}

func TestModelUnchangedFailurePreservesPreviousSnapshot(t *testing.T) {
	op := fixtureOperation(ActionRotate)
	activated := op.CreatedAt.Add(-time.Hour)
	previous := CredentialSnapshot{Status: CredentialActive, CredentialVersion: 1, ActivatedAt: &activated}
	op.Previous = &previous
	done, err := CompleteOperation(op, ExecutionUnchangedFailure, VerificationEvidence{}, op.CreatedAt)
	if err != nil || !reflect.DeepEqual(done.Credential, previous) || !reflect.DeepEqual(*done.Operation.Previous, previous) {
		t.Fatalf("unchanged failure altered previous metadata: %+v %v", done, err)
	}
	if _, err := CompleteOperation(done.Operation, ExecutionRecoveryRequired, VerificationEvidence{}, op.CreatedAt); err == nil {
		t.Fatal("terminal failure reopened")
	}
	op.Evidence = VerificationEvidence{RAMApplied: true, SnapshotObserved: true}
	recovery, err := CompleteOperation(op, ExecutionRecoveryRequired, VerificationEvidence{}, op.CreatedAt)
	if err != nil || recovery.Operation.Evidence != op.Evidence || recovery.Operation.CompletedAt != nil {
		t.Fatal("recovery discarded evidence or became terminal", err)
	}
}

func TestModelOperationInvariantsAndReplay(t *testing.T) {
	op := fixtureOperation(ActionRotate)
	for _, mutate := range []func(*Operation){
		func(o *Operation) { o.CredentialVersion = 0 }, func(o *Operation) { o.CredentialVersion = -1 },
		func(o *Operation) { o.Status = OperationSucceeded }, func(o *Operation) { o.CompletedAt = &o.CreatedAt },
		func(o *Operation) { o.Status = OperationRecoveryNeeded; o.CompletedAt = &o.CreatedAt },
	} {
		bad := op
		mutate(&bad)
		if bad.Validate() == nil {
			t.Fatal("invalid completion/version accepted")
		}
	}
	input := MutationInput{GatewayID: op.GatewayID, IdempotencyKey: op.IdempotencyKey, Action: op.Action}
	meta := Metadata{GatewayID: op.GatewayID, CredentialVersion: 4, Status: CredentialActive}
	for _, tc := range []struct {
		status OperationStatus
		code   ErrorCode
	}{{OperationPending, CodeOperationInProgress}, {OperationRecoveryNeeded, CodeRecoveryRequired}} {
		o := op
		o.Status = tc.status
		_, err := ReplayOperation(o, op.ActorUserID, input, meta)
		var de *DomainError
		if !errors.As(err, &de) || de.Code != tc.code {
			t.Fatalf("replay: %v", err)
		}
	}
	for _, status := range []OperationStatus{OperationSucceeded, OperationFailed} {
		o := op
		o.Status = status
		o.CompletedAt = &o.CreatedAt
		if status == OperationSucceeded {
			o.Evidence = VerificationEvidence{RAMApplied: true, SnapshotObserved: true, FreshPositiveVerified: true}
		}
		result, err := ReplayOperation(o, op.ActorUserID, input, meta)
		if err != nil || result.SecretReturned || result.OperationID != op.OperationID || result.Metadata != meta {
			t.Fatal("replay not safe metadata-only", err)
		}
	}
	for _, mismatch := range []MutationInput{{GatewayID: "other_fixture", IdempotencyKey: input.IdempotencyKey, Action: input.Action}, {GatewayID: input.GatewayID, IdempotencyKey: uuid.New(), Action: input.Action}, {GatewayID: input.GatewayID, IdempotencyKey: input.IdempotencyKey, Action: ActionRevoke}} {
		if _, err := ReplayOperation(op, op.ActorUserID, mismatch, meta); err == nil {
			t.Fatal("semantic mismatch replay")
		}
	}
	if _, err := ReplayOperation(op, uuid.New(), input, meta); err == nil {
		t.Fatal("cross actor replay")
	}
	historical := op
	historical.ActorUserID = uuid.Nil
	if historical.ValidateHistorical() != nil || historical.Validate() == nil {
		t.Fatal("historical null actor policy")
	}
	if _, err := ReplayOperation(historical, op.ActorUserID, input, meta); err == nil {
		t.Fatal("historical null actor replay")
	}
}

func TestModelCompletionRejectsInvalidGenerationAndTime(t *testing.T) {
	proof := VerificationEvidence{RAMApplied: true, SnapshotObserved: true, FreshPositiveVerified: true}
	op := fixtureOperation(ActionRotate)
	op.Previous = &CredentialSnapshot{Status: CredentialActive, CredentialVersion: 1}
	for _, mutate := range []func(*Operation){
		func(o *Operation) { o.Previous = nil },
		func(o *Operation) { o.Previous = &CredentialSnapshot{Status: CredentialRevoked, CredentialVersion: 1} },
		func(o *Operation) { o.CredentialVersion = 1 },
		func(o *Operation) { o.Action = ActionRevoke },    // revoke cannot allocate 3
		func(o *Operation) { o.Action = ActionProvision }, // active reprovision forbidden
		func(o *Operation) { o.Previous = &CredentialSnapshot{Status: CredentialActive, CredentialVersion: 0} },
		func(o *Operation) { o.UpdatedAt = o.CreatedAt.Add(time.Hour) },
	} {
		bad := op
		mutate(&bad)
		if _, err := CompleteOperation(bad, ExecutionVerifiedSuccess, proof, op.CreatedAt.Add(time.Second)); err == nil {
			t.Fatal("bad operation completed")
		}
	}
	for _, at := range []time.Time{{}, op.CreatedAt.Add(-time.Second)} {
		if _, err := CompleteOperation(op, ExecutionVerifiedSuccess, proof, at); err == nil {
			t.Fatal("invalid time completed")
		}
	}
	for _, previous := range []CredentialStatus{CredentialRevoked, CredentialFailed} {
		reprovision := fixtureOperation(ActionProvision)
		reprovision.Previous = &CredentialSnapshot{Status: previous, CredentialVersion: 1}
		if _, err := CompleteOperation(reprovision, ExecutionVerifiedSuccess, proof, reprovision.CreatedAt); err != nil {
			t.Fatal("reprovision denied", err)
		}
	}
	bad := op
	bad.Status = OperationFailed
	before := op.CreatedAt.Add(-time.Second)
	bad.CompletedAt = &before
	if bad.Validate() == nil {
		t.Fatal("completion before creation")
	}
	if _, err := ReplayOperation(op, uuid.Nil, MutationInput{}, Metadata{}); err == nil {
		t.Fatal("invalid replay input")
	}
	op.Status, op.CompletedAt = OperationSucceeded, &op.CreatedAt
	op.Evidence = VerificationEvidence{RAMApplied: true, SnapshotObserved: true, FreshPositiveVerified: true}
	input := MutationInput{GatewayID: op.GatewayID, IdempotencyKey: op.IdempotencyKey, Action: op.Action}
	if _, err := ReplayOperation(op, op.ActorUserID, input, Metadata{}); err == nil {
		t.Fatal("wrong metadata replay")
	}
	op.Phase = "bogus"
	if _, err := ReplayOperation(op, op.ActorUserID, input, Metadata{}); err == nil {
		t.Fatal("invalid stored operation replay")
	}
}

func TestModelSafeSerializationAndSecret(t *testing.T) {
	m := Metadata{GatewayID: "fixture_gateway", Username: "fixture_gateway", CredentialVersion: 1, Status: CredentialProvisioning, LastOperationID: uuid.New(), OperationStatus: OperationPending}
	metaJSON, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(metaJSON, &fields); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"gateway_id", "username", "credential_version", "status", "last_operation_id", "operation_status", "activated_at", "revoked_at", "last_error_code"} {
		if _, ok := fields[key]; !ok {
			t.Fatalf("missing metadata key %s", key)
		}
	}
	for _, forbidden := range []string{"password", "hash", "snapshot", "path", "broker_url"} {
		if strings.Contains(string(metaJSON), forbidden) {
			t.Fatalf("unsafe metadata field %s", forbidden)
		}
	}
	if fields["status"] != "provisioning" || fields["activated_at"] != nil {
		t.Fatal("version inferred active state")
	}
	metadataResult := MutationResult{Metadata: m, OperationID: uuid.New(), NextAction: "rotate_with_new_idempotency_key"}
	safeJSON, err := json.Marshal(metadataResult)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(safeJSON), "password") || !strings.Contains(string(safeJSON), `"secret_returned":false`) {
		t.Fatal("metadata-only output contains a secret response")
	}
	secret := "fixture_one_time_secret_DO_NOT_LOG"
	result := NewSecretResult(m, uuid.New(), secret)
	for _, value := range []any{result, &result, []SecretResult{result}} {
		for _, format := range []string{"%v", "%+v", "%#v", "%s"} {
			if strings.Contains(fmt.Sprintf(format, value), secret) {
				t.Fatalf("secret leaked through %s", format)
			}
		}
	}
	b, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &fields); err != nil {
		t.Fatal(err)
	}
	if fields["password"] != secret || fields["secret_returned"] != true || fields["operation_id"] == nil {
		t.Fatal("explicit one-time response incomplete")
	}
	result.ClearSecret()
	b, err = json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), secret) || strings.Contains(string(b), `"password"`) || !strings.Contains(string(b), `"secret_returned":false`) {
		t.Fatal("cleared result retained secret response")
	}
	op := Operation{Evidence: VerificationEvidence{RAMApplied: true, SnapshotObserved: true}, DeliveryStatus: DeliveryUnknown}
	b, err = json.Marshal(op)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"snapshot_observed":true`) || strings.Contains(string(b), "device_updated") {
		t.Fatal("snapshot evidence implies device delivery")
	}
}

func TestModelDomainErrorsSafeAndCompatible(t *testing.T) {
	for _, code := range []ErrorCode{CodeInvalidRequest, CodeForbidden, CodeNotFound, CodeCredentialConflict, CodeOperationInProgress, CodeIdempotencyConflict, CodeRuntimeDisabled, CodeRuntimeBusy, CodeVerificationUnavailable, CodeRecoveryRequired, CodeFinalizationPending, CodeServiceUnavailable, CodeInternalError} {
		err := &DomainError{Code: code}
		if err.Error() != string(code) {
			t.Fatal("error includes raw diagnostics")
		}
	}
	if !errors.Is(&DomainError{Code: CodeRecoveryRequired}, ErrRecoveryRequired) || !errors.Is(&DomainError{Code: CodeRuntimeBusy}, ErrRuntimeBusy) {
		t.Fatal("legacy sentinel compatibility lost")
	}
	if errors.Is(&DomainError{Code: CodeFinalizationPending}, ErrRecoveryRequired) {
		t.Fatal("finalization ambiguity collapsed into recovery")
	}
}
