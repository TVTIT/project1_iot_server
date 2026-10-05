package config

import (
	"fmt"
	"net/url"
	"path/filepath"
	"strconv"
	"time"

	"iot-platform/internal/mqttcredential"
)

// CredentialConfig is opt-in. No secrets are interpolated into child argv.
type CredentialConfig struct {
	Enabled                                                                                                        bool
	ManagerUsername, ManagerPassword, BackendPassword                                                              string
	ManagementURL, CAFile, SnapshotPath, ControlDir                                                                string
	DBTimeout, FinalizeTimeout, ReconcileTimeout, OperationTimeout, RequestTimeout, RecoveryTimeout, ClientTimeout time.Duration
	BatchSize, MaxPages, QueueSize, MaxInflight, MaxPayloadBytes, MaxSnapshotBytes                                 int
}

func (CredentialConfig) String() string { return "CredentialConfig{redacted}" }

// GoString prevents accidental secret disclosure by diagnostic formatting.
func (c CredentialConfig) GoString() string { return c.String() }

func loadCredential(lookup LookupFunc, cfg Config) (CredentialConfig, error) {
	c := CredentialConfig{}
	raw := valueOrDefault(lookup, "MQTT_CREDENTIAL_API_ENABLED", "false")
	var err error
	if c.Enabled, err = strconv.ParseBool(raw); err != nil {
		return c, fmt.Errorf("MQTT_CREDENTIAL_API_ENABLED must be boolean")
	}
	if !c.Enabled {
		return c, nil
	}
	gate, err := strconv.ParseBool(valueOrDefault(lookup, "INGRESS_GATE_ENABLED", "true"))
	if err != nil || !gate || cfg.MQTTCredentialRuntime.Enabled {
		return c, fmt.Errorf("credential API requires ingress gate and disabled legacy runtime")
	}
	c.ManagerUsername = valueOrDefault(lookup, "MQTT_DYNSEC_MANAGER_USERNAME", "")
	if c.ManagerUsername == "" || c.ManagerUsername == mqttcredential.BackendUsername {
		return c, fmt.Errorf("distinct MQTT_DYNSEC_MANAGER_USERNAME required")
	}
	c.ManagerPassword, _ = lookup("MQTT_DYNSEC_MANAGER_PASSWORD")
	c.BackendPassword, _ = lookup("MQTT_PASSWORD")
	if mqttcredential.ValidateBackendPassword(c.ManagerPassword) != nil || mqttcredential.ValidateBackendPassword(c.BackendPassword) != nil || c.ManagerPassword == c.BackendPassword {
		return c, fmt.Errorf("distinct valid manager and backend passwords required")
	}
	c.ManagementURL = valueOrDefault(lookup, "MQTT_INTERNAL_MANAGEMENT_URL", "")
	u, e := url.Parse(c.ManagementURL)
	if e != nil || u.Scheme != "ssl" || u.Hostname() == "" || u.Port() == "" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
		return c, fmt.Errorf("MQTT_INTERNAL_MANAGEMENT_URL must be ssl URL with hostname and port")
	}
	for key, dest := range map[string]*string{"MQTT_TLS_CA_FILE": &c.CAFile, "MQTT_DYNSEC_JSON_PATH": &c.SnapshotPath, "MQTT_CONTROLLER_DIR": &c.ControlDir} {
		*dest = valueOrDefault(lookup, key, "")
		if !filepath.IsAbs(*dest) || filepath.Clean(*dest) != *dest {
			return c, fmt.Errorf("%s must be a clean absolute path", key)
		}
	}
	if len(c.ControlDir) > 75 {
		return c, fmt.Errorf("MQTT_CONTROLLER_DIR too long")
	}
	for _, p := range []struct {
		key           string
		dest          *time.Duration
		fallback, max time.Duration
	}{
		{"MQTT_CREDENTIAL_DB_TIMEOUT", &c.DBTimeout, 2 * time.Second, time.Minute},
		{"MQTT_CREDENTIAL_FINALIZE_TIMEOUT", &c.FinalizeTimeout, 5 * time.Second, time.Minute},
		{"MQTT_CREDENTIAL_RECONCILE_TIMEOUT", &c.ReconcileTimeout, 2 * time.Minute, 10 * time.Minute},
		{"MQTT_CREDENTIAL_OPERATION_TIMEOUT", &c.OperationTimeout, 10 * time.Second, time.Minute},
		{"MQTT_CREDENTIAL_REQUEST_TIMEOUT", &c.RequestTimeout, 40 * time.Second, 5 * time.Minute},
		{"MQTT_CREDENTIAL_RECOVERY_TIMEOUT", &c.RecoveryTimeout, 5 * time.Second, time.Minute},
		{"MQTT_DYNSEC_TIMEOUT", &c.ClientTimeout, 3 * time.Second, time.Minute},
	} {
		if *p.dest, err = boundedDuration(lookup, p.key, p.fallback, time.Millisecond, p.max); err != nil {
			return c, err
		}
	}
	for _, p := range []struct {
		key           string
		dest          *int
		fallback, max int
	}{
		{"MQTT_CREDENTIAL_RECONCILE_BATCH_SIZE", &c.BatchSize, 64, 1000},
		{"MQTT_CREDENTIAL_RECONCILE_MAX_PAGES", &c.MaxPages, 16, 1024},
		{"MQTT_DYNSEC_QUEUE_SIZE", &c.QueueSize, 16, 4096},
		{"MQTT_DYNSEC_MAX_INFLIGHT", &c.MaxInflight, 1, 1024},
		{"MQTT_DYNSEC_MAX_PAYLOAD_BYTES", &c.MaxPayloadBytes, 65536, 1 << 20},
		{"MQTT_DYNSEC_MAX_SNAPSHOT_BYTES", &c.MaxSnapshotBytes, 1 << 20, 64 << 20},
	} {
		if *p.dest, err = integer(lookup, p.key, p.fallback, 1, p.max); err != nil {
			return c, err
		}
	}
	// Whole-request context also bounds scan/finalization work. Detached service
	// recovery is additional; authorization precedes the credential handler.
	if c.ClientTimeout > c.OperationTimeout || c.DBTimeout > c.FinalizeTimeout || c.RequestTimeout <= c.OperationTimeout+c.FinalizeTimeout || cfg.HTTPWriteTimeout <= cfg.AuthorizationTimeout+c.RequestTimeout+c.RecoveryTimeout || cfg.ShutdownTimeout <= c.RequestTimeout+c.RecoveryTimeout {
		return c, fmt.Errorf("credential HTTP/shutdown budgets too small or inconsistent")
	}
	return c, nil
}
