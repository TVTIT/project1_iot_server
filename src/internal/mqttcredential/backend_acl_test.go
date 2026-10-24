package mqttcredential

import (
	"context"
	"encoding/json"
	"testing"
)

func TestBackendACLReadOnlyExact(t *testing.T) {
	for _, mode := range []string{"valid", "missing", "extra", "group", "role", "default", "disabled", "clientid", "missing_allow", "priority"} {
		t.Run(mode, func(t *testing.T) {
			f := &dynsecFake{}
			c, err := newDynSecClient(context.Background(), dynsecTestConfig(), f)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			calls := 0
			f.publish = func(b []byte) {
				var req struct {
					Commands []map[string]any `json:"commands"`
				}
				_ = json.Unmarshal(b, &req)
				command := req.Commands[0]["command"].(string)
				var data any
				switch command {
				case "getClient":
					client := map[string]any{"username": "backend", "roles": []any{map[string]any{"rolename": "bootstrap_backend"}}, "groups": []any{}, "disabled": false}
					if mode == "group" {
						client["groups"] = []any{map[string]any{"groupname": "broad"}}
					}
					if mode == "role" {
						client["roles"] = []any{map[string]any{"rolename": "admin"}}
					}
					if mode == "disabled" {
						client["disabled"] = true
					}
					if mode == "clientid" {
						client["clientid"] = "foreign"
					}
					data = map[string]any{"client": client}
				case "getRole":
					acls := []map[string]any{}
					for _, suffix := range []string{"telemetry", "responses"} {
						for _, kind := range []string{"publishClientReceive", "subscribeLiteral", "unsubscribeLiteral"} {
							acls = append(acls, map[string]any{"acltype": kind, "topic": "gateways/+/" + suffix + "/#", "allow": true, "priority": 0})
						}
					}
					for _, suffix := range []string{"acks", "commands"} {
						acls = append(acls, map[string]any{"acltype": "publishClientSend", "topic": "gateways/+/" + suffix + "/#", "allow": true, "priority": 0})
					}
					if mode == "missing" {
						acls = acls[1:]
					}
					if mode == "extra" {
						acls = append(acls, map[string]any{"acltype": "publishClientSend", "topic": "#", "allow": true, "priority": 0})
					}
					if mode == "missing_allow" {
						delete(acls[0], "allow")
					}
					if mode == "priority" {
						acls[0]["priority"] = 1
					}
					data = map[string]any{"role": map[string]any{"rolename": "bootstrap_backend", "acls": acls}}
				case "getDefaultACLAccess":
					acls := []any{}
					for _, kind := range []string{"publishClientSend", "publishClientReceive", "subscribe", "unsubscribe"} {
						acls = append(acls, map[string]any{"acltype": kind, "allow": mode == "default"})
					}
					data = map[string]any{"acls": acls}
				default:
					t.Errorf("mutation/unsupported inspection %s", command)
				}
				calls++
				f.receive(dynSecResponseTopic, dynsecReply(b, "", "", map[string]any{"data": data}), false)
			}
			err = c.VerifyBackendACL(context.Background(), "backend", "data-client")
			if (err == nil) != (mode == "valid") {
				t.Fatalf("mode %s: %v", mode, err)
			}
			if mode == "valid" && calls < 3 {
				t.Fatalf("missing readback: %d", calls)
			}
		})
	}
}
