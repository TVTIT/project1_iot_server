package config

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"iot-platform/internal/digitaltwin"
	"iot-platform/internal/mqttcontrol"
	"iot-platform/internal/mqttcredential"
	"iot-platform/internal/mqttingress"
)

// Stage5Config keeps data traffic separate from bounded ACL inspection credentials.
type Stage5Config struct {
	Enabled    bool
	Control    digitaltwin.ControlConfig
	Dispatch   mqttcontrol.Config
	Data       mqttingress.TransportOptions
	Management mqttcredential.DynSecConfig
}

// String redacts data and privileged management credentials.
func (Stage5Config) String() string { return "Stage5Config{redacted}" }

// GoString prevents diagnostic secret exposure.
func (c Stage5Config) GoString() string { return c.String() }

func loadStage5(lookup LookupFunc) (Stage5Config, error) {
	cfg := Stage5Config{Control: digitaltwin.DefaultControlConfig(), Dispatch: mqttcontrol.DefaultConfig()}
	var err error
	if cfg.Enabled, err = boolValue(lookup, "STAGE5_ENABLED", false); err != nil {
		return cfg, err
	}
	if cfg.Control.DBTimeout, err = boundedDuration(lookup, "STAGE5_REQUEST_TIMEOUT", cfg.Control.DBTimeout, time.Millisecond, time.Minute); err != nil {
		return cfg, err
	}
	if cfg.Control.TTL, err = boundedDuration(lookup, "STAGE5_COMMAND_TTL", cfg.Control.TTL, time.Second, 24*time.Hour); err != nil {
		return cfg, err
	}
	if cfg.Control.TTL%time.Microsecond != 0 {
		return cfg, fmt.Errorf("STAGE5_COMMAND_TTL must have microsecond precision")
	}
	if cfg.Control.MaxPendingOutbox, err = integer(lookup, "STAGE5_MAX_PENDING_OUTBOX", cfg.Control.MaxPendingOutbox, 1, 1000000); err != nil {
		return cfg, err
	}
	if cfg.Control.MaxReceipts, err = integer(lookup, "STAGE5_MAX_RECEIPTS", cfg.Control.MaxReceipts, 1, 1000000); err != nil {
		return cfg, err
	}
	for _, p := range []struct {
		key           string
		dest          *int
		def, min, max int
	}{
		{"STAGE5_PUBLISH_WORKERS", &cfg.Dispatch.Workers, 1, 1, 16},
		{"STAGE5_SCAN_BATCH", &cfg.Dispatch.BatchSize, 32, 1, 1024},
		{"STAGE5_RESULT_QUEUE", &cfg.Dispatch.ResultQueueCapacity, 128, 1, 4096},
		{"STAGE5_PAYLOAD_BYTES", &cfg.Dispatch.MaxPayloadBytes, 8192, 8192, 8192},
	} {
		if *p.dest, err = integer(lookup, p.key, p.def, p.min, p.max); err != nil {
			return cfg, err
		}
	}
	for _, p := range []struct {
		key  string
		dest *time.Duration
		def  time.Duration
	}{
		{"STAGE5_SCAN_INTERVAL", &cfg.Dispatch.ScanInterval, time.Second},
		{"STAGE5_EXPIRE_INTERVAL", &cfg.Dispatch.ExpireInterval, time.Second},
		{"STAGE5_RETRY_BACKOFF", &cfg.Dispatch.RetryBackoff, time.Second},
		{"STAGE5_PUBLISH_TIMEOUT", &cfg.Dispatch.PublishTimeout, 3 * time.Second},
		{"STAGE5_ACL_TIMEOUT", &cfg.Management.Timeout, 5 * time.Second},
		{"STAGE5_CONNECT_TIMEOUT", &cfg.Data.ConnectTimeout, 5 * time.Second},
		{"STAGE5_SUBSCRIBE_TIMEOUT", &cfg.Data.SubscribeTimeout, 3 * time.Second},
		{"STAGE5_RECONNECT_MAX_INTERVAL", &cfg.Data.ReconnectMaxInterval, 30 * time.Second},
	} {
		if *p.dest, err = boundedDuration(lookup, p.key, p.def, time.Millisecond, time.Minute); err != nil {
			return cfg, err
		}
	}
	if !cfg.Enabled {
		return cfg, nil
	}
	for _, p := range []struct {
		key  string
		dest *string
		max  int
	}{
		{"MQTT_BROKER_URL", &cfg.Data.BrokerURL, 256}, {"STAGE5_MQTT_CLIENT_ID", &cfg.Data.ClientID, 128},
		{"MQTT_USERNAME", &cfg.Data.Username, 128}, {"MQTT_PASSWORD", &cfg.Data.Password, 512},
		{"MQTT_TLS_CA_FILE", &cfg.Data.CAFile, 1024}, {"MQTT_INTERNAL_MANAGEMENT_URL", &cfg.Management.BrokerURL, 256},
		{"MQTT_DYNSEC_MANAGER_USERNAME", &cfg.Management.ManagerUsername, 128}, {"MQTT_DYNSEC_MANAGER_PASSWORD", &cfg.Management.ManagerPassword, 512},
	} {
		if *p.dest, err = boundedIngressString(lookup, p.key, p.max); err != nil {
			return cfg, fmt.Errorf("%s required and bounded when STAGE5_ENABLED", p.key)
		}
	}
	if err = validateTLSBrokerURL(cfg.Data.BrokerURL); err != nil {
		return cfg, err
	}
	if err = validateTLSBrokerURL(cfg.Management.BrokerURL); err != nil {
		return cfg, err
	}
	if !filepath.IsAbs(cfg.Data.CAFile) || filepath.Clean(cfg.Data.CAFile) != cfg.Data.CAFile || strings.ContainsAny(cfg.Data.CAFile, "\x00\r\n") {
		return cfg, fmt.Errorf("stage 5 CA must be clean absolute path")
	}
	if cfg.Data.Username == cfg.Management.ManagerUsername || cfg.Data.Password == cfg.Management.ManagerPassword {
		return cfg, fmt.Errorf("stage 5 requires distinct data and management identities and secrets")
	}
	cfg.Data.PublishTimeout = cfg.Dispatch.PublishTimeout
	cfg.Data.CloseWaitTimeout = time.Second
	cfg.Management.MaxPayloadBytes = 65536
	cfg.Management.QueueSize = 8
	cfg.Management.MaxInflight = 1
	cfg.Management.ProtectedUsernames = []string{cfg.Data.Username, cfg.Management.ManagerUsername}
	cfg.Management.ValidateTarget = func(string) bool { return false }
	cfg.Management.ValidateRole = func(string, string) bool { return false }
	return cfg, nil
}
