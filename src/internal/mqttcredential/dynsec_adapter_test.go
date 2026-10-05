package mqttcredential

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// Injected transport unit fault: only the exact correlated pinned native
// getClient error proves absence; other errors must remain uncertainty.
func TestDynSecAdapterNativeAbsence(t *testing.T) {
	for _, message := range []string{"Client not found", "Internal error", "SECRET diagnostic"} {
		f := &dynsecFake{}
		c, err := newDynSecClient(context.Background(), dynsecTestConfig(), f)
		if err != nil {
			t.Fatal(err)
		}
		f.publish = func(b []byte) {
			f.receive(dynSecResponseTopic, dynsecReply(b, "", "", map[string]any{"error": message}), false)
		}
		_, err = c.GetClient(context.Background(), "A")
		if errors.Is(err, errDynSecClientAbsent) != (message == "Client not found") {
			t.Fatal("absence classification")
		}
		c.Close()
	}
}

func TestVerifyRevocationsCorrelatedAbsence(t *testing.T) {
	for _, mode := range []string{"absent", "query-error", "snapshot-error", "snapshot-present", "epoch-shift"} {
		t.Run(mode, func(t *testing.T) {
			ctrl := &adapterControllerFixture{d: LifecycleDescription{Epoch: "epoch", Nonce: "closed", Alive: true}, shiftOnDescribe: mode == "epoch-shift"}
			cfg := DynSecAdapterConfig{Controller: ctrl, Timeout: time.Second, RecoveryTimeout: time.Second, ProtectedUsernames: []string{"manager", "backend"}, Login: func(context.Context, string, string) error { t.Fatal("absence is not a password witness"); return nil }}
			cfg.NewClient = func(ctx context.Context) (*DynSecClient, error) {
				f := &dynsecFake{}
				c, err := newDynSecClient(ctx, dynsecTestConfig(), f)
				f.publish = func(b []byte) {
					var req struct {
						Commands []map[string]any `json:"commands"`
					}
					_ = json.Unmarshal(b, &req)
					if req.Commands[0]["command"] != "getClient" {
						t.Fatal("absent principal must not be created or altered")
					}
					message := "Client not found"
					if mode == "query-error" {
						message = "Internal error"
					}
					f.receive(dynSecResponseTopic, dynsecReply(b, "", "", map[string]any{"error": message}), false)
				}
				return c, err
			}
			cfg.Observe = func(context.Context, string) (DynSecClientMetadata, error) {
				if mode == "snapshot-error" {
					return DynSecClientMetadata{}, ErrVerificationFailed
				}
				if mode == "snapshot-present" {
					return DynSecClientMetadata{Username: "B"}, nil
				}
				return DynSecClientMetadata{}, errDynSecSnapshotAbsent
			}
			a, err := NewDynSecAdapter(cfg)
			if err != nil {
				t.Fatal(err)
			}
			r := DynSecAdapterResult{OperationID: uuid.New(), Outcome: ExecutionVerifiedSuccess, receipt: VerificationReceipt{Epoch: "epoch", Nonce: "closed"}}
			a.busy, a.pending = true, r
			err = a.VerifyRevocations(context.Background(), r, []RevocationDecision{{GatewayID: "B", OperationID: uuid.New(), CredentialVersion: 1}})
			if (err == nil) != (mode == "absent") {
				t.Fatalf("mode %s: %v", mode, err)
			}
			if ctrl.d.Open {
				t.Fatal("verification opened ingress")
			}
		})
	}
}

func TestDynSecAdapterRejectHiddenGroupAndClientID(t *testing.T) {
	for _, extra := range []map[string]any{{"groups": []any{map[string]any{"groupname": "privileged"}}}, {"clientid": "restricted"}} {
		v := map[string]any{"username": "A", "roles": []any{map[string]any{"rolename": "gateway_A"}}}
		for k, x := range extra {
			v[k] = x
		}
		b, _ := json.Marshal(v)
		got, e := projectDynSecClient(b, "A")
		if e != nil {
			t.Fatal(e)
		}
		if adapterClientMatches(got, "A", "gateway_A", false) {
			t.Fatal("hidden policy accepted")
		}
	}
}

type adapterControllerFixture struct {
	calls           []string
	d               LifecycleDescription
	shiftOnDescribe bool
	descriptions    int
}

func (f *adapterControllerFixture) CloseDrain(context.Context) error {
	f.calls = append(f.calls, "close")
	f.d = LifecycleDescription{Epoch: "epoch", Nonce: "closed", Alive: true}
	return nil
}
func (f *adapterControllerFixture) Describe(context.Context) (LifecycleDescription, error) {
	f.descriptions++
	if f.shiftOnDescribe && f.descriptions == 2 {
		f.d.Epoch = "unverified-lifetime"
	}
	return f.d, nil
}

func TestDynSecAdapterFaultsStayClosed(t *testing.T) {
	for _, mode := range []string{"generation", "query", "disableClient", "setClientPassword", "enableClient", "snapshot", "login", "freshclient", "role", "partial_role", "unknown_native", "revoke_snapshot", "protected", "stale"} {
		t.Run(mode, func(t *testing.T) {
			ctrl := &adapterControllerFixture{}
			clients := 0
			roleCreated := false
			disabled := false
			cfg := DynSecAdapterConfig{Controller: ctrl, Timeout: time.Second, RecoveryTimeout: time.Second, ProtectedUsernames: []string{"manager", "backend"}}
			cfg.NewClient = func(ctx context.Context) (*DynSecClient, error) {
				clients++
				if mode == "freshclient" && clients == 2 {
					return nil, ErrVerificationFailed
				}
				f := &dynsecFake{}
				c, e := newDynSecClient(ctx, dynsecTestConfig(), f)
				f.publish = func(b []byte) {
					var req struct {
						Commands []map[string]any `json:"commands"`
					}
					_ = json.Unmarshal(b, &req)
					cmd := req.Commands[0]["command"].(string)
					fields := map[string]any{}
					if cmd == "disableClient" {
						disabled = true
					}
					if cmd == "enableClient" {
						disabled = false
					}
					if cmd == "getClient" {
						fields["data"] = map[string]any{"client": map[string]any{"username": "A", "disabled": disabled, "roles": []any{map[string]any{"rolename": "gateway_A"}}}}
					}
					if cmd == "getRole" {
						fields["data"] = map[string]any{"role": map[string]any{"rolename": "gateway_A", "acls": adapterGatewayACLs("A")}}
					}
					if mode == "partial_role" && cmd == "getClient" {
						fields = map[string]any{"error": "Client not found"}
					}
					if mode == "partial_role" && cmd == "getRole" && !roleCreated {
						fields = map[string]any{"error": "Role not found"}
					}
					if cmd == "createRole" {
						roleCreated = true
					}
					if cmd == mode || mode == "query" && cmd == "getClient" || mode == "role" && cmd == "getRole" || mode == "partial_role" && cmd == "createClient" {
						fields = map[string]any{"error": "safe injected failure"}
					}
					f.receive(dynSecResponseTopic, dynsecReply(b, "", "", fields), false)
				}
				return c, e
			}
			cfg.Observe = func(context.Context, string) (DynSecClientMetadata, error) {
				if mode == "snapshot" || mode == "revoke_snapshot" {
					return DynSecClientMetadata{}, ErrVerificationFailed
				}
				return DynSecClientMetadata{Username: "A", Disabled: disabled, Roles: []string{"gateway_A"}}, nil
			}
			cfg.Login = func(context.Context, string, string) error {
				if mode == "login" {
					return ErrVerificationFailed
				}
				return nil
			}
			a, e := NewDynSecAdapter(cfg)
			if e != nil {
				t.Fatal(e)
			}
			op := Operation{OperationID: uuid.New(), ActorUserID: uuid.New(), IdempotencyKey: uuid.New(), GatewayID: "A", Action: ActionRotate, CredentialVersion: 2, Status: OperationPending, Phase: PhaseIntent, DeliveryStatus: DeliveryUnknown, Previous: &CredentialSnapshot{Status: CredentialActive, CredentialVersion: 1}}
			if mode == "partial_role" || mode == "unknown_native" {
				op.Action = ActionProvision
				op.Previous = nil
			}
			if mode == "revoke_snapshot" {
				op.Action = ActionRevoke
				op.CredentialVersion = 1
			}
			if mode == "protected" {
				op.GatewayID = "manager"
			}
			r, e := a.Execute(context.Background(), op, func(context.Context) (string, error) {
				if mode == "generation" {
					return "", ErrVerificationFailed
				}
				return "new-fixture-password", nil
			})
			if mode == "stale" {
				if e != nil {
					t.Fatal(e)
				}
				ctrl.d.Epoch = "other"
				if a.OpenAfterFinalization(context.Background(), r, op.OperationID) == nil || ctrl.d.Open {
					t.Fatal("stale receipt")
				}
			} else if e == nil || r.Outcome == ExecutionVerifiedSuccess || ctrl.d.Open {
				t.Fatal("fault became success", mode, e)
			}
			if mode == "partial_role" && (r.Outcome != ExecutionRecoveryRequired || !roleCreated) {
				t.Fatal("partial role false unchanged")
			}
			if mode == "protected" {
				if len(ctrl.calls) != 0 {
					t.Fatal("protected target side effect")
				}
				return
			}
			if e = a.ReleaseClosed(context.Background(), op.OperationID); e != nil || ctrl.d.Open {
				t.Fatal("maintenance release", e)
			}
		})
	}
}

func TestDynSecAdapterInvalidConfiguration(t *testing.T) {
	base := DynSecAdapterConfig{Controller: &adapterControllerFixture{}, NewClient: func(context.Context) (*DynSecClient, error) { panic("unexpected client") }, Observe: func(context.Context, string) (DynSecClientMetadata, error) { panic("unexpected observation") }, Login: func(context.Context, string, string) error { panic("unexpected login") }, ProtectedUsernames: []string{"manager", "backend"}, Timeout: time.Second, RecoveryTimeout: time.Second}
	for _, mutate := range []func(*DynSecAdapterConfig){
		func(c *DynSecAdapterConfig) { c.Controller = nil },
		func(c *DynSecAdapterConfig) { var p *adapterControllerFixture; c.Controller = p },
		func(c *DynSecAdapterConfig) { c.NewClient = nil },
		func(c *DynSecAdapterConfig) { c.Observe = nil },
		func(c *DynSecAdapterConfig) { c.Login = nil },
		func(c *DynSecAdapterConfig) { c.ProtectedUsernames = []string{"", "backend"} },
		func(c *DynSecAdapterConfig) { c.ProtectedUsernames = []string{"manager", "manager"} },
		func(c *DynSecAdapterConfig) { c.Timeout = 0 },
		func(c *DynSecAdapterConfig) { c.Timeout = time.Minute + 1 },
		func(c *DynSecAdapterConfig) { c.RecoveryTimeout = 0 },
		func(c *DynSecAdapterConfig) { c.RecoveryTimeout = time.Minute + 1 },
	} {
		cfg := base
		mutate(&cfg)
		if a, e := NewDynSecAdapter(cfg); a != nil || e != ErrInvalidInput {
			t.Fatal("invalid configuration accepted")
		}
	}
}

func TestDynSecAdapterRejectsMalformedRole(t *testing.T) {
	for _, data := range []any{nil, map[string]any{"role": nil}, map[string]any{"role": map[string]any{"rolename": "gateway_A", "acls": "invalid"}}, map[string]any{"role": map[string]any{"rolename": "gateway_A", "acls": append(adapterGatewayACLs("A"), adapterACL{"publishClientSend", "#", 0, true})}}, map[string]any{"role": map[string]any{"rolename": "gateway_A", "acls": adapterGatewayACLs("B")}}} {
		f := &dynsecFake{}
		c, e := newDynSecClient(context.Background(), dynsecTestConfig(), f)
		if e != nil {
			t.Fatal(e)
		}
		f.publish = func(b []byte) {
			f.receive(dynSecResponseTopic, dynsecReply(b, "", "", map[string]any{"data": data}), false)
		}
		if (&DynSecAdapter{}).ensureRole(context.Background(), c, "A", "gateway_A", false) == nil {
			t.Fatal("malformed/broad role accepted")
		}
		c.Close()
	}
}

func TestDynSecAdapterGeneratorErrorIsRedacted(t *testing.T) {
	ctrl := &adapterControllerFixture{}
	cfg := DynSecAdapterConfig{Controller: ctrl, ProtectedUsernames: []string{"manager", "backend"}, Timeout: time.Second, RecoveryTimeout: time.Second, Observe: func(context.Context, string) (DynSecClientMetadata, error) { panic("unexpected observation") }, Login: func(context.Context, string, string) error { panic("unexpected login") }}
	cfg.NewClient = func(ctx context.Context) (*DynSecClient, error) {
		f := &dynsecFake{}
		c, e := newDynSecClient(ctx, dynsecTestConfig(), f)
		f.publish = func(b []byte) {
			f.receive(dynSecResponseTopic, dynsecReply(b, "", "", map[string]any{"error": "Client not found"}), false)
		}
		return c, e
	}
	a, e := NewDynSecAdapter(cfg)
	if e != nil {
		t.Fatal(e)
	}
	op := Operation{OperationID: uuid.New(), ActorUserID: uuid.New(), IdempotencyKey: uuid.New(), GatewayID: "A", Action: ActionProvision, CredentialVersion: 1, Status: OperationPending, Phase: PhaseIntent, DeliveryStatus: DeliveryUnknown}
	r, e := a.Execute(context.Background(), op, func(context.Context) (string, error) { return "SECRET_CALLBACK", errors.New("SECRET_CALLBACK") })
	if e == nil || strings.Contains(fmt.Sprintf("%v %+v %#v", e, r, r), "SECRET_CALLBACK") || r.Outcome != ExecutionMaintenanceClosed {
		t.Fatal("secret leaked or false proof")
	}
}
func (f *adapterControllerFixture) RestartClosed(context.Context) (LifecycleDescription, error) {
	f.calls = append(f.calls, "restart")
	f.d.Epoch = "fresh"
	return f.d, nil
}
func (f *adapterControllerFixture) OpenVerified(_ context.Context, r VerificationReceipt) error {
	if r.Epoch != f.d.Epoch || r.Nonce != f.d.Nonce {
		return ErrLifecycleUnavailable
	}
	f.calls = append(f.calls, "open")
	f.d.Open = true
	return nil
}

func TestDynSecAdapterRotateClosedUntilAcknowledgement(t *testing.T) {
	for _, shifted := range []bool{false, true} {
		t.Run(fmt.Sprint(shifted), func(t *testing.T) {
			ctrl := &adapterControllerFixture{}
			ctrl.shiftOnDescribe = shifted
			cfg := DynSecAdapterConfig{Controller: ctrl, Timeout: time.Second, RecoveryTimeout: time.Second, ProtectedUsernames: []string{"manager", "backend"}}
			cfg.NewClient = func(ctx context.Context) (*DynSecClient, error) {
				ctrl.calls = append(ctrl.calls, "client")
				f := &dynsecFake{}
				c, e := newDynSecClient(ctx, dynsecTestConfig(), f)
				f.publish = func(b []byte) {
					var req struct {
						Commands []map[string]any `json:"commands"`
					}
					_ = json.Unmarshal(b, &req)
					command := req.Commands[0]["command"].(string)
					ctrl.calls = append(ctrl.calls, command)
					fields := map[string]any{}
					if command == "getRole" {
						fields["data"] = map[string]any{"role": map[string]any{"rolename": "gateway_A", "acls": adapterGatewayACLs("A")}}
					}
					if command == "getClient" {
						fields["data"] = map[string]any{"client": map[string]any{"username": "A", "roles": []any{map[string]any{"rolename": "gateway_A"}}}}
					}
					f.receive(dynSecResponseTopic, dynsecReply(b, "", "", fields), false)
				}
				return c, e
			}
			cfg.Observe = func(context.Context, string) (DynSecClientMetadata, error) {
				ctrl.calls = append(ctrl.calls, "snapshot")
				return DynSecClientMetadata{Username: "A", Roles: []string{"gateway_A"}}, nil
			}
			cfg.Login = func(context.Context, string, string) error { ctrl.calls = append(ctrl.calls, "login"); return nil }
			a, e := NewDynSecAdapter(cfg)
			if e != nil {
				t.Fatal(e)
			}
			op := Operation{OperationID: uuid.New(), ActorUserID: uuid.New(), IdempotencyKey: uuid.New(), GatewayID: "A", Action: ActionRotate, CredentialVersion: 2, Status: OperationPending, Phase: PhaseIntent, DeliveryStatus: DeliveryUnknown, Previous: &CredentialSnapshot{Status: CredentialActive, CredentialVersion: 1}}
			r, e := a.Execute(context.Background(), op, func(context.Context) (string, error) {
				ctrl.calls = append(ctrl.calls, "secret")
				return "new-test-secret", nil
			})
			if shifted {
				if e == nil || r.Outcome == ExecutionVerifiedSuccess || ctrl.d.Open {
					t.Fatal("evidence from prior lifetime credited to new epoch")
				}
				return
			}
			if e != nil || r.Outcome != ExecutionVerifiedSuccess || ctrl.d.Open {
				t.Fatalf("verification: %+v %v", r, e)
			}
			expected := []string{"close", "client", "getClient", "getRole", "secret", "disableClient", "setClientPassword", "enableClient", "getClient", "snapshot", "restart", "client", "getClient", "getRole", "login", "snapshot"}
			if !reflect.DeepEqual(ctrl.calls, expected) {
				t.Fatalf("sequence %v", ctrl.calls)
			}
			if _, e = a.Execute(context.Background(), op, nil); e == nil {
				t.Fatal("admission escaped DB finalization")
			}
			if a.OpenAfterFinalization(context.Background(), r, uuid.New()) == nil || ctrl.d.Open {
				t.Fatal("wrong operation opened")
			}
			if e = a.OpenAfterFinalization(context.Background(), r, op.OperationID); e != nil || !ctrl.d.Open {
				t.Fatal("trusted finalization seam", e)
			}
			if a.OpenAfterFinalization(context.Background(), r, op.OperationID) == nil {
				t.Fatal("receipt replay")
			}
		})
	}
}
