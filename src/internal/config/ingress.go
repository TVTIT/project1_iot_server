package config

import (
	"fmt"
	"math"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// MQTT telemetry ingress defaults and supported bounds. These are starting
// values for the Task 3.4 configuration slice, not measured capacity numbers.
const (
	// MQTT_INGRESS_ENABLED is opt-in and independent of
	// MQTT_CREDENTIAL_API_ENABLED and the legacy MQTT_CREDENTIAL_RUNTIME_ENABLED.
	// It must not be confused with the credential lifecycle INGRESS_GATE_ENABLED.
	defaultMQTTIngressEnabled = false

	defaultMQTTQueueCapacity = 256
	maxMQTTQueueCapacity     = 1_000_000
	defaultMQTTWorkerCount   = 4
	maxMQTTWorkerCount       = 10_000

	// MQTT_MAX_PAYLOAD_BYTES is the application-level per-message payload limit.
	// The demo/default profile is 64 KiB; the supported maximum is 1 MiB, which
	// is also the value the broker packet bound is derived from.
	DefaultMQTTMaxPayloadBytes = 65_536
	MaxMQTTMaxPayloadBytes     = 1 << 20

	// The queue memory budget is the dominant in-RAM copy buffer:
	// MQTT_QUEUE_CAPACITY x MQTT_MAX_PAYLOAD_BYTES. The default profile
	// (256 x 64 KiB) is 16 MiB. The supported maximum is 256 MiB, excluding
	// workers, decoded sample slices and Paho internals.
	DefaultIngressQueueMemoryBytes int64 = 16 << 20
	MaxIngressQueueMemoryBytes     int64 = 256 << 20

	maxMQTTClientIDLength = 128
	maxMQTTUsernameLength = 128
	maxMQTTPasswordLength = 512

	// ingressReservedConnections are shared-pool connections kept outside the
	// ingestion workers for HTTP and readiness. One extra is reserved when the
	// credential API holds a dedicated owner connection in the same pool.
	ingressReservedConnections int32 = 2
)

// IngressConfig configures the MQTT telemetry ingress data client. It is inert
// unless Enabled, in which case a distinct backend data client identity and CA
// are required. Values are validated for bounds here; loading the CA and
// creating the connection happen at server startup, never in this package.
type IngressConfig struct {
	Enabled bool

	BrokerURL string
	ClientID  string
	Username  string
	Password  string
	CAFile    string

	QueueCapacity   int
	WorkerCount     int
	MaxPayloadBytes int

	ConnectTimeout       time.Duration
	SubscribeTimeout     time.Duration
	PublishTimeout       time.Duration
	ReconnectMaxInterval time.Duration

	MaxSamplesPerMessage int
	MaxBatchDuration     time.Duration
	MaxFutureSkew        time.Duration
	DBTimeout            time.Duration
}

// String redacts every value so diagnostics never reveal credentials or paths.
func (IngressConfig) String() string { return "IngressConfig{redacted}" }

// GoString prevents accidental secret disclosure by diagnostic formatting.
func (c IngressConfig) GoString() string { return c.String() }

// loadIngress parses and validates the ingress configuration. Queue, worker,
// payload, timeout and telemetry limits are parsed unconditionally so disabled
// deployments still fail fast on nonsensical values. Identity and CA
// requirements, pool reservation and the shutdown budget are enforced only
// when ingress is enabled, so HTTP-only fixtures need no data credential or CA.
func loadIngress(lookup LookupFunc, databaseMaxConns int32, credentialEnabled bool, shutdownTimeout time.Duration) (IngressConfig, error) {
	var c IngressConfig
	enabled, err := boolValue(lookup, "MQTT_INGRESS_ENABLED", defaultMQTTIngressEnabled)
	if err != nil {
		return c, err
	}
	c.Enabled = enabled

	if c.QueueCapacity, err = integer(lookup, "MQTT_QUEUE_CAPACITY", defaultMQTTQueueCapacity, 1, maxMQTTQueueCapacity); err != nil {
		return c, err
	}
	if c.WorkerCount, err = integer(lookup, "MQTT_WORKER_COUNT", defaultMQTTWorkerCount, 1, maxMQTTWorkerCount); err != nil {
		return c, err
	}
	if c.MaxPayloadBytes, err = integer(lookup, "MQTT_MAX_PAYLOAD_BYTES", DefaultMQTTMaxPayloadBytes, 1, MaxMQTTMaxPayloadBytes); err != nil {
		return c, err
	}
	if err = checkQueueMemoryBound(c.QueueCapacity, c.MaxPayloadBytes); err != nil {
		return c, err
	}

	if c.ConnectTimeout, err = boundedDuration(lookup, "MQTT_CONNECT_TIMEOUT", 5*time.Second, time.Millisecond, time.Minute); err != nil {
		return c, err
	}
	if c.SubscribeTimeout, err = boundedDuration(lookup, "MQTT_SUBSCRIBE_TIMEOUT", 3*time.Second, time.Millisecond, time.Minute); err != nil {
		return c, err
	}
	if c.PublishTimeout, err = boundedDuration(lookup, "MQTT_PUBLISH_TIMEOUT", 3*time.Second, time.Millisecond, time.Minute); err != nil {
		return c, err
	}
	if c.ReconnectMaxInterval, err = boundedDuration(lookup, "MQTT_RECONNECT_MAX_INTERVAL", 30*time.Second, time.Second, 10*time.Minute); err != nil {
		return c, err
	}

	if c.MaxSamplesPerMessage, err = integer(lookup, "TELEMETRY_MAX_SAMPLES_PER_MESSAGE", 1000, 1, math.MaxInt32); err != nil {
		return c, err
	}
	if c.MaxBatchDuration, err = boundedDuration(lookup, "TELEMETRY_MAX_BATCH_DURATION", time.Minute, time.Second, time.Hour); err != nil {
		return c, err
	}
	if c.MaxFutureSkew, err = boundedDuration(lookup, "TELEMETRY_MAX_FUTURE_SKEW", 2*time.Minute, 0, time.Hour); err != nil {
		return c, err
	}
	if c.DBTimeout, err = boundedDuration(lookup, "TELEMETRY_DB_TIMEOUT", 5*time.Second, time.Millisecond, time.Minute); err != nil {
		return c, err
	}

	if !c.Enabled {
		return c, nil
	}

	brokerURL, ok := nonEmpty(lookup, "MQTT_BROKER_URL")
	if !ok {
		return c, fmt.Errorf("MQTT_BROKER_URL is required when MQTT_INGRESS_ENABLED is true")
	}
	if err = validateTLSBrokerURL(brokerURL); err != nil {
		return c, err
	}
	c.BrokerURL = brokerURL

	if c.ClientID, err = boundedIngressString(lookup, "MQTT_CLIENT_ID", maxMQTTClientIDLength); err != nil {
		return c, err
	}
	if c.Username, err = boundedIngressString(lookup, "MQTT_USERNAME", maxMQTTUsernameLength); err != nil {
		return c, err
	}
	if c.Password, err = boundedIngressString(lookup, "MQTT_PASSWORD", maxMQTTPasswordLength); err != nil {
		return c, err
	}

	caFile, ok := nonEmpty(lookup, "MQTT_TLS_CA_FILE")
	if !ok {
		return c, fmt.Errorf("MQTT_TLS_CA_FILE is required when MQTT_INGRESS_ENABLED is true")
	}
	if !filepath.IsAbs(caFile) || filepath.Clean(caFile) != caFile || strings.ContainsAny(caFile, "\x00\r\n") {
		return c, fmt.Errorf("MQTT_TLS_CA_FILE must be a clean absolute path")
	}
	c.CAFile = caFile

	if err = checkIngressPoolReservation(c.WorkerCount, databaseMaxConns, credentialEnabled); err != nil {
		return c, err
	}
	if shutdownTimeout < c.DBTimeout+c.PublishTimeout {
		return c, fmt.Errorf("SHUTDOWN_TIMEOUT must cover TELEMETRY_DB_TIMEOUT + MQTT_PUBLISH_TIMEOUT when MQTT_INGRESS_ENABLED is true")
	}
	return c, nil
}

// validateTLSBrokerURL accepts only an ssl:// URL with an explicit host and
// port and no userinfo, path, query or fragment. The runtime still performs
// CA and hostname/SAN verification.
func validateTLSBrokerURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "ssl" || u.Hostname() == "" || u.Port() == "" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("MQTT_BROKER_URL must be an ssl:// URL with host and port and no userinfo, path, query or fragment")
	}
	return nil
}

// boundedIngressString reads a required non-blank, length-bounded identity
// value and rejects embedded NUL/newline characters before they reach a
// credential or broker API. The raw value is preserved, never trimmed.
func boundedIngressString(lookup LookupFunc, key string, max int) (string, error) {
	raw, _ := lookup(key)
	if strings.TrimSpace(raw) == "" {
		return "", fmt.Errorf("%s is required when MQTT_INGRESS_ENABLED is true", key)
	}
	if len(raw) > max || strings.ContainsAny(raw, "\x00\r\n") {
		return "", fmt.Errorf("%s is invalid or too long", key)
	}
	return raw, nil
}

// checkQueueMemoryBound keeps MQTT_QUEUE_CAPACITY x MQTT_MAX_PAYLOAD_BYTES
// overflow-safe and within the supported in-memory queue budget.
func checkQueueMemoryBound(capacity, payload int) error {
	if capacity <= 0 || payload <= 0 {
		return fmt.Errorf("MQTT_QUEUE_CAPACITY and MQTT_MAX_PAYLOAD_BYTES must be positive")
	}
	if int64(capacity) > math.MaxInt64/int64(payload) {
		return fmt.Errorf("MQTT_QUEUE_CAPACITY x MQTT_MAX_PAYLOAD_BYTES overflows")
	}
	budget := int64(capacity) * int64(payload)
	if budget > MaxIngressQueueMemoryBytes {
		return fmt.Errorf("MQTT_QUEUE_CAPACITY x MQTT_MAX_PAYLOAD_BYTES (%d bytes) exceeds the supported %d bytes", budget, MaxIngressQueueMemoryBytes)
	}
	return nil
}

// checkIngressPoolReservation keeps the ingestion workers from consuming the
// whole shared pool. At least two connections stay for HTTP and readiness, and
// one more when the credential API holds a dedicated owner connection in the
// same pool.
func checkIngressPoolReservation(workers int, databaseMaxConns int32, credentialEnabled bool) error {
	reserved := ingressReservedConnections
	if credentialEnabled {
		reserved++
	}
	if int32(workers) > databaseMaxConns-reserved {
		return fmt.Errorf("MQTT_WORKER_COUNT (%d) must leave at least %d shared database connections for HTTP/readiness", workers, reserved)
	}
	return nil
}

// boolValue parses a strict boolean flag, falling back when unset.
func boolValue(lookup LookupFunc, key string, fallback bool) (bool, error) {
	raw, ok := nonEmpty(lookup, key)
	if !ok {
		return fallback, nil
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("%s must be boolean", key)
	}
	return value, nil
}
