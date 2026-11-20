package mqttcredential

import (
	"context"
	"os"
	"testing"
	"time"
)

// TestStage8LiveBackendACLProbe is an opt-in fixture precondition. It exercises
// the same manager TLS connection and exact native readback as the backend's
// VerifyConnection callback, without exposing credentials or ACL payloads.
func TestStage8LiveBackendACLProbe(t *testing.T) {
	if os.Getenv("STAGE8_DYNSEC_PROBE") != "1" {
		t.Skip("fixture-only probe")
	}
	brokerURL := os.Getenv("STAGE8_DYNSEC_BROKER_URL")
	managerUsername := os.Getenv("STAGE8_DYNSEC_MANAGER_USERNAME")
	managerPassword := os.Getenv("STAGE8_DYNSEC_MANAGER_PASSWORD")
	username := os.Getenv("STAGE8_DYNSEC_BACKEND_USERNAME")
	clientID := os.Getenv("STAGE8_DYNSEC_BACKEND_CLIENT_ID")
	caPath := os.Getenv("STAGE8_DYNSEC_CA_FILE")
	if brokerURL == "" || managerUsername == "" || managerPassword == "" ||
		username == "" || clientID == "" || caPath == "" {
		t.Fatal("fixture_probe_configuration_missing")
	}
	ca, err := os.ReadFile(caPath)
	if err != nil {
		t.Fatal("fixture_probe_ca_unavailable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := NewDynSecClient(ctx, DynSecConfig{
		BrokerURL: brokerURL, CAPEM: ca, ManagerUsername: managerUsername,
		ManagerPassword:    managerPassword,
		ProtectedUsernames: []string{managerUsername, username},
		Timeout:            5 * time.Second, MaxPayloadBytes: 65536, MaxInflight: 1, QueueSize: 4,
		ValidateTarget: func(string) bool { return false },
		ValidateRole:   func(string, string) bool { return false },
	})
	if err != nil {
		t.Fatalf("fixture_manager_tls_connect:%v", err)
	}
	defer client.Close()
	if err := client.VerifyBackendACL(ctx, username, clientID, true); err != nil {
		t.Fatalf("fixture_backend_acl_readback:%v", err)
	}
	t.Log("fixture manager TLS and exact backend DynSec readback: PASS")
}
