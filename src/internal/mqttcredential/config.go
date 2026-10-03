//nolint:misspell // MOSQUITTO_PASSWD_PATH is a stable configuration contract.
package mqttcredential

import (
	"fmt"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// StartupConfig is inert unless Enabled. Recovery verification is deliberately
// not supplied by startup: backend health cannot prove a target generation.
type StartupConfig struct {
	Enabled                                           bool
	BrokerURL, Username, Password, CAFile, SocketPath string
	Runtime                                           RuntimeConfig
}

func cleanPath(s string) bool {
	return filepath.IsAbs(s) && filepath.Clean(s) == s && s != "/" && !strings.ContainsAny(s, "\x00\r\n")
}

// LoadConfig parses and validates the opt-in credential runtime configuration.
func LoadConfig(get func(string) (string, bool)) (StartupConfig, error) {
	var c StartupConfig
	raw, ok := get("MQTT_CREDENTIAL_RUNTIME_ENABLED")
	if ok {
		v, e := strconv.ParseBool(raw)
		if e != nil {
			return c, fmt.Errorf("invalid MQTT_CREDENTIAL_RUNTIME_ENABLED")
		}
		c.Enabled = v
	}
	if !c.Enabled {
		return c, nil
	}
	read := func(k string) string { v, _ := get(k); return v }
	c.BrokerURL = read("MQTT_BROKER_URL")
	c.Username = read("MQTT_USERNAME")
	c.Password = read("MQTT_PASSWORD")
	u, e := url.Parse(c.BrokerURL)
	if e != nil || u.Scheme != "ssl" || u.Hostname() == "" || u.Port() == "" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
		return c, fmt.Errorf("MQTT_BROKER_URL requires ssl host and port")
	}
	if c.Username != BackendUsername || !validPassword(c.Password) || strings.TrimSpace(c.Password) == "" || strings.Contains(strings.ToLower(c.Password), "replace_with") || strings.Contains(strings.ToLower(c.Password), "change_me") {
		return c, fmt.Errorf("invalid MQTT backend credentials")
	}
	c.CAFile = read("MQTT_TLS_CA_FILE")
	c.SocketPath = read("MQTT_RELOAD_SOCKET")
	c.Runtime.AuthDir = read("MQTT_AUTH_DIR")
	c.Runtime.Tool.Path = read("MOSQUITTO_PASSWD_PATH")
	for _, p := range []string{c.CAFile, c.SocketPath, c.Runtime.AuthDir, c.Runtime.Tool.Path} {
		if !cleanPath(p) {
			return c, fmt.Errorf("invalid MQTT runtime path")
		}
	}
	if len(c.SocketPath) > 100 {
		return c, fmt.Errorf("MQTT reload socket path too long")
	}
	for _, item := range []struct {
		k   string
		dst *time.Duration
		d   time.Duration
	}{
		{"MQTT_CREDENTIAL_OPERATION_TIMEOUT", &c.Runtime.OperationTimeout, 10 * time.Second}, {"MQTT_PASSWORD_TOOL_TIMEOUT", &c.Runtime.Tool.Timeout, 3 * time.Second}, {"MQTT_RELOAD_TIMEOUT", &c.Runtime.ReloadTimeout, 2 * time.Second}, {"MQTT_CREDENTIAL_PROBE_TIMEOUT", &c.Runtime.ProbeTimeout, 3 * time.Second}, {"MQTT_CREDENTIAL_RECOVERY_TIMEOUT", &c.Runtime.RecoveryTimeout, 5 * time.Second},
	} {
		*item.dst = item.d
		if v, ok := get(item.k); ok {
			d, e := time.ParseDuration(v)
			if e != nil || d <= 0 || d > time.Minute {
				return c, fmt.Errorf("invalid %s", item.k)
			}
			*item.dst = d
		}
	}
	c.Runtime.MaxFileBytes = DefaultMaxFileBytes
	c.Runtime.MaxPending = 8
	if v, ok := get("MQTT_CREDENTIAL_MAX_FILE_BYTES"); ok {
		n, e := strconv.ParseInt(v, 10, 64)
		if e != nil || n < 1 || n > 64<<20 {
			return c, fmt.Errorf("invalid MQTT_CREDENTIAL_MAX_FILE_BYTES")
		}
		c.Runtime.MaxFileBytes = n
	}
	if v, ok := get("MQTT_CREDENTIAL_MAX_PENDING"); ok {
		n, e := strconv.Atoi(v)
		if e != nil || n < 1 || n > 1024 {
			return c, fmt.Errorf("invalid MQTT_CREDENTIAL_MAX_PENDING")
		}
		c.Runtime.MaxPending = n
	}
	return c, nil
}
