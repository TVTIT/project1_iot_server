package mqttcredential

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestRevokePreservesOpenLifetime(t *testing.T) {
	for _, fault := range []string{"", "snapshot", "denial", "missing_denial", "epoch", "finalization_epoch", "closed"} {
		t.Run(fault, func(t *testing.T) { testRevokeOpenLifetime(t, fault) })
	}
}

func testRevokeOpenLifetime(t *testing.T, fault string) {
	ctrl := &adapterControllerFixture{d: LifecycleDescription{Epoch: "epoch", Open: true, Alive: true}}
	if fault == "closed" {
		ctrl.d.Open = false
		ctrl.d.Nonce = "" // Invalid CLOSED description: no challenge.
	}
	ctrl.shiftOnDescribe = fault == "epoch"
	disabled := false
	cfg := DynSecAdapterConfig{Controller: ctrl, Timeout: time.Second, RecoveryTimeout: time.Second, ProtectedUsernames: []string{"manager", "backend"}}
	cfg.Login = func(context.Context, string, string) error { t.Fatal("revoke generated a secret"); return nil }
	cfg.Rejected = func(context.Context, string, string) error {
		if !disabled {
			t.Fatal("probe before disable")
		}
		if fault == "denial" {
			return ErrVerificationFailed
		}
		return nil
	}
	if fault == "missing_denial" {
		cfg.Rejected = nil
	}
	cfg.Observe = func(context.Context, string) (DynSecClientMetadata, error) {
		if fault == "snapshot" {
			return DynSecClientMetadata{}, ErrVerificationFailed
		}
		return DynSecClientMetadata{Username: "A", Disabled: disabled, Roles: []string{"gateway_A"}}, nil
	}
	cfg.NewClient = func(ctx context.Context) (*DynSecClient, error) {
		f := &dynsecFake{}
		c, e := newDynSecClient(ctx, dynsecTestConfig(), f)
		f.publish = func(b []byte) {
			var req struct {
				Commands []map[string]any `json:"commands"`
			}
			_ = json.Unmarshal(b, &req)
			command := req.Commands[0]["command"].(string)
			fields := map[string]any{}
			if command == "disableClient" {
				disabled = true
			}
			if command == "getClient" {
				fields["data"] = map[string]any{"client": map[string]any{"username": "A", "disabled": disabled, "roles": []any{map[string]any{"rolename": "gateway_A"}}}}
			}
			if command == "getRole" {
				fields["data"] = map[string]any{"role": map[string]any{"rolename": "gateway_A", "acls": adapterGatewayACLs("A")}}
			}
			f.receive(dynSecResponseTopic, dynsecReply(b, "", "", fields), false)
		}
		return c, e
	}
	a, e := NewDynSecAdapter(cfg)
	if e != nil {
		t.Fatal(e)
	}
	op := Operation{OperationID: uuid.New(), ActorUserID: uuid.New(), IdempotencyKey: uuid.New(), GatewayID: "A", Action: ActionRevoke, CredentialVersion: 1, Status: OperationPending, Phase: PhaseIntent, DeliveryStatus: DeliveryUnknown, Previous: &CredentialSnapshot{Status: CredentialActive, CredentialVersion: 1}}
	r, e := a.Execute(context.Background(), op, nil)
	if fault == "snapshot" || fault == "denial" || fault == "missing_denial" || fault == "epoch" || fault == "closed" {
		if e == nil || r.Outcome == ExecutionVerifiedSuccess || ctrl.d.Open {
			t.Fatal("uncertainty left gate OPEN")
		}
		if a.ReleaseClosed(context.Background(), op.OperationID) != nil {
			t.Fatal("recovery release")
		}
		return
	}
	if e != nil {
		t.Fatal(e)
	}
	if !ctrl.d.Open || len(ctrl.calls) != 0 {
		t.Fatalf("successful revoke drained unrelated sessions: %v", ctrl.calls)
	}
	if fault == "finalization_epoch" {
		ctrl.d.Epoch = "other"
		if a.OpenAfterFinalization(context.Background(), r, op.OperationID) == nil {
			t.Fatal("stale OPEN lifetime accepted")
		}
		if a.CloseDrain(context.Background()) != nil || a.ReleaseClosed(context.Background(), op.OperationID) != nil || ctrl.d.Open {
			t.Fatal("stale finalize recovery")
		}
		return
	}
	if a.VerifyRevocations(context.Background(), r, nil) != nil || a.OpenAfterFinalization(context.Background(), r, op.OperationID) != nil || a.VerifyOpen(context.Background(), r) != nil {
		t.Fatal("finalization lost OPEN lifetime")
	}
	if len(ctrl.calls) != 0 {
		t.Fatalf("finalization performed global lifecycle mutation: %v", ctrl.calls)
	}
}
