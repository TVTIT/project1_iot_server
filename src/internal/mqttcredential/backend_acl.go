package mqttcredential

import (
	"context"
	"encoding/json"
)

// VerifyBackendACL reads native policy; it never mutates broker configuration.
func (c *DynSecClient) VerifyBackendACL(ctx context.Context, username, clientID string) error {
	bad := dynSecError("backend_acl_mismatch")
	if ctx == nil || username == "" || username == c.cfg.ManagerUsername || clientID == "" {
		return bad
	}
	// This bypasses mutation target guards only for these three fixed read APIs.
	b, err := c.adapterRequest(ctx, "getClient", map[string]any{"username": username})
	if err != nil {
		return err
	}
	var client struct {
		Client json.RawMessage `json:"client"`
	}
	if json.Unmarshal(b, &client) != nil {
		return bad
	}
	m, err := projectDynSecClient(client.Client, username)
	if err != nil || m.Disabled || len(m.Groups) != 0 || len(m.Roles) != 1 || m.Roles[0] != "bootstrap_backend" || (m.ClientID != "" && m.ClientID != clientID) {
		return bad
	}
	b, err = c.adapterRequest(ctx, "getRole", map[string]any{"rolename": "bootstrap_backend"})
	if err != nil {
		return err
	}
	var role struct {
		Role struct {
			Name string `json:"rolename"`
			ACLs []struct {
				Kind     string `json:"acltype"`
				Topic    string `json:"topic"`
				Allow    *bool  `json:"allow"`
				Priority *int   `json:"priority"`
			} `json:"acls"`
		} `json:"role"`
	}
	if json.Unmarshal(b, &role) != nil || role.Role.Name != "bootstrap_backend" {
		return bad
	}
	expected := map[string]bool{}
	for _, suffix := range []string{"telemetry", "responses"} {
		for _, kind := range []string{"publishClientReceive", "subscribeLiteral", "unsubscribeLiteral"} {
			expected[kind+"|gateways/+/"+suffix+"/#"] = true
		}
	}
	for _, suffix := range []string{"acks", "commands"} {
		expected["publishClientSend|gateways/+/"+suffix+"/#"] = true
	}
	for _, acl := range role.Role.ACLs {
		key := acl.Kind + "|" + acl.Topic
		if !expected[key] || acl.Allow == nil || !*acl.Allow || acl.Priority == nil || *acl.Priority != 0 {
			return bad
		}
		delete(expected, key)
	}
	if len(expected) != 0 {
		return bad
	}
	b, err = c.adapterRequest(ctx, "getDefaultACLAccess", map[string]any{})
	if err != nil {
		return err
	}
	var defaults struct {
		ACLs []struct {
			Kind  string `json:"acltype"`
			Allow *bool  `json:"allow"`
		} `json:"acls"`
	}
	if json.Unmarshal(b, &defaults) != nil {
		return bad
	}
	denied := map[string]bool{"publishClientSend": true, "publishClientReceive": true, "subscribe": true, "unsubscribe": true}
	for _, acl := range defaults.ACLs {
		if !denied[acl.Kind] || acl.Allow == nil || *acl.Allow {
			return bad
		}
		delete(denied, acl.Kind)
	}
	if len(denied) != 0 {
		return bad
	}
	return nil
}
