package mqttcredential

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

type startupProofController struct{ adapterControllerFixture }

func (f *startupProofController) CloseDrain(context.Context) error {
	ep, _ := freshIdentity()
	nonce, _ := freshIdentity()
	f.d = LifecycleDescription{Epoch: ep, Nonce: nonce, Alive: true}
	return nil
}
func (f *startupProofController) RestartClosed(context.Context) (LifecycleDescription, error) {
	f.d.Epoch, _ = freshIdentity()
	return f.d, nil
}

func TestDynSecStartupProofBoundaries(t *testing.T) {
	for _, mode := range []string{"absent", "present", "query-error", "snapshot-error", "snapshot-present", "role-error", "disable-error", "post-disable-error", "protected", "backend", "invalid", "epoch-shift", "active-absent", "unknown-status", "inventory-present", "inventory-error"} {
		t.Run(mode, func(t *testing.T) {
			ctrl := &startupProofController{}
			disabled := false
			cfg := DynSecAdapterConfig{Controller: ctrl, Timeout: time.Second, RecoveryTimeout: time.Second, ProtectedUsernames: []string{"manager", "backend"}, Login: func(context.Context, string, string) error {
				t.Fatal("startup cannot authenticate lost password")
				return nil
			}}
			cfg.NewClient = func(ctx context.Context) (*DynSecClient, error) {
				f := &dynsecFake{}
				c, err := newDynSecClient(ctx, dynsecTestConfig(), f)
				f.publish = func(b []byte) {
					var req struct {
						Commands []map[string]any `json:"commands"`
					}
					_ = json.Unmarshal(b, &req)
					cmd := req.Commands[0]["command"]
					fields := map[string]any{}
					switch cmd {
					case "getClient":
						fields["error"] = "Client not found"
						if mode == "present" || mode == "role-error" || mode == "disable-error" || mode == "post-disable-error" || mode == "inventory-present" || mode == "inventory-error" {
							fields = map[string]any{"data": map[string]any{"client": map[string]any{"username": "A", "disabled": disabled, "roles": []any{map[string]any{"rolename": "gateway_A"}}}}}
						}
						if mode == "query-error" || mode == "post-disable-error" && disabled {
							fields = map[string]any{"error": "Internal error"}
						}
					case "getRole":
						fields["data"] = map[string]any{"role": map[string]any{"rolename": "gateway_A", "acls": adapterGatewayACLs("A")}}
						if mode == "role-error" {
							fields = map[string]any{"error": "Internal error"}
						}
					case "disableClient":
						disabled = true
						if mode == "disable-error" {
							fields["error"] = "Internal error"
						}
					default:
						t.Fatal("unexpected mutation")
					}
					f.receive(dynSecResponseTopic, dynsecReply(b, "", "", fields), false)
				}
				return c, err
			}
			cfg.Observe = func(context.Context, string) (DynSecClientMetadata, error) {
				if mode == "snapshot-error" || mode == "inventory-error" {
					return DynSecClientMetadata{}, ErrVerificationFailed
				}
				if mode == "present" || mode == "inventory-present" {
					return DynSecClientMetadata{Username: "A", Disabled: disabled, Roles: []string{"gateway_A"}}, nil
				}
				if mode == "snapshot-present" {
					return DynSecClientMetadata{Username: "A"}, nil
				}
				return DynSecClientMetadata{}, errDynSecSnapshotAbsent
			}
			a, err := NewDynSecAdapter(cfg)
			if err != nil {
				t.Fatal(err)
			}
			r, err := a.BeginStartup(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if mode == "epoch-shift" {
				ctrl.shiftOnDescribe = true
			}
			u := "A"
			if mode == "protected" {
				u = "manager"
			}
			if mode == "backend" {
				u = BackendUsername
			}
			if mode == "invalid" {
				u = "../A"
			}
			var proof RecoveryDisableEvidence
			if mode == "active-absent" || mode == "unknown-status" || mode == "inventory-present" || mode == "inventory-error" {
				status := CredentialActive
				if mode == "unknown-status" {
					status = "unknown"
				}
				err = a.VerifyStartupInventory(context.Background(), r, []Metadata{{GatewayID: "A", Status: status}})
			} else {
				proof, err = a.DisableStartup(context.Background(), r, u)
			}
			ok := mode == "absent" || mode == "present" || mode == "inventory-present"
			if (err == nil) != ok {
				t.Fatalf("mode %s: %v", mode, err)
			}
			if ok && mode != "inventory-present" && proof.Validate(r.Epoch) != nil {
				t.Fatal("invalid proof")
			}
			if !ok && proof != (RecoveryDisableEvidence{}) {
				t.Fatal("failed observation returned proof")
			}
			if ok {
				if err = a.OpenStartup(context.Background(), r); err == nil {
					t.Fatal("fixture retains challenge; OPEN description must reject it")
				}
			}
		})
	}
}
