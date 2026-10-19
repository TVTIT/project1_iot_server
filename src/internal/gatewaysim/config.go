// Package gatewaysim implements the development Gateway simulator used for
// Stage 3 telemetry integration. This slice provides an independent
// configuration loader and a pure synthetic telemetry batch builder. It
// deliberately does not reuse the server configuration loader, which requires
// DATABASE_URL and Supabase JWT settings a Gateway does not have, and it does
// not own durable state such as the SQLite outbox, transport, scheduler or CLI.
package gatewaysim

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"iot-platform/internal/gateway"
)

// LookupFunc returns an environment value and whether it is set.
type LookupFunc func(string) (string, bool)

// Defaults and hard bounds for the Gateway simulator configuration. These are
// starting values for the Task 3.5 slice, not measured capacity numbers. The
// sample profile is additionally checked against the backend telemetry limits
// by validateSampleProfile so the simulator never emits a batch the backend
// would reject.
const (
	// Default sample profile: 100 Hz, one 100-sample batch covering one second.
	defaultSampleRateHz    = 100
	defaultSamplesPerBatch = 100
	maxSampleRateHz        = 100_000
	maxSamplesPerBatch     = 1000

	defaultAckTimeout = 5 * time.Second
	defaultRetryMin   = time.Second
	defaultRetryMax   = 30 * time.Second
	maxRetryDelay     = 10 * time.Minute

	defaultMaxInflight = 8
	maxMaxInflight     = 4096

	defaultOutboxMaxRows  = 5000
	maxOutboxMaxRows      = 1_000_000
	defaultOutboxMaxBytes = int64(64 << 20) // 67_108_864
	maxOutboxMaxBytes     = int64(8) << 30

	// Operation timeouts are finite and configurable with the GATEWAY_ prefix.
	// They are recorded here so the defaults are documented next to the loader
	// even though the transport, SQLite and CLI that consume them land in later
	// slices.
	defaultConnectTimeout   = 10 * time.Second
	defaultSubscribeTimeout = 5 * time.Second
	defaultPublishTimeout   = 5 * time.Second
	defaultSQLiteTimeout    = 5 * time.Second
	defaultShutdownTimeout  = 10 * time.Second
	minTimeout              = time.Millisecond
	maxTimeout              = 10 * time.Minute

	maxClientIDLength = 128
	maxUsernameLength = 128
	maxPasswordLength = 512
)

// Config is the validated Gateway simulator configuration. It contains only
// Gateway-side values; it never carries database or Supabase settings.
type Config struct {
	GatewayID string
	SensorID  string

	BrokerURL string
	ClientID  string
	Username  string
	Password  string
	CAFile    string

	OutboxPath string

	SampleRateHz     int
	SamplesPerBatch  int
	SampleIntervalUS int64
	BatchSpan        time.Duration
	MaxPayloadBytes  int

	AckTimeout    time.Duration
	RetryMinDelay time.Duration
	RetryMaxDelay time.Duration

	MaxInflight    int
	OutboxMaxRows  int
	OutboxMaxBytes int64

	ConnectTimeout   time.Duration
	SubscribeTimeout time.Duration
	PublishTimeout   time.Duration
	SQLiteTimeout    time.Duration
	ShutdownTimeout  time.Duration
}

// String redacts every field so diagnostics never reveal credentials or paths.
func (Config) String() string { return "gatewaysim.Config{redacted}" }

// GoString prevents accidental secret disclosure by diagnostic formatting.
func (c Config) GoString() string { return c.String() }

// LoadFromEnvironment loads configuration from the process environment.
func LoadFromEnvironment() (Config, error) {
	return Load(os.LookupEnv)
}

// Load parses and validates the Gateway simulator configuration using lookup.
// It reads only GATEWAY_* keys and never requires DATABASE_URL, Supabase JWT
// settings or any server credential.
func Load(lookup LookupFunc) (Config, error) {
	var c Config
	var ok bool
	var err error

	gatewayID, ok := nonEmpty(lookup, "GATEWAY_ID")
	if !ok {
		return c, fmt.Errorf("GATEWAY_ID is required")
	}
	if err := gateway.ValidateGatewayID(gatewayID); err != nil {
		return c, fmt.Errorf("GATEWAY_ID is invalid")
	}
	c.GatewayID = gatewayID

	sensorID, ok := nonEmpty(lookup, "SENSOR_ID")
	if !ok {
		return c, fmt.Errorf("SENSOR_ID is required")
	}
	if err := gateway.ValidateSensorID(sensorID); err != nil {
		return c, fmt.Errorf("SENSOR_ID is invalid")
	}
	c.SensorID = sensorID

	if c.BrokerURL, ok = nonEmpty(lookup, "GATEWAY_MQTT_BROKER_URL"); !ok {
		return c, fmt.Errorf("GATEWAY_MQTT_BROKER_URL is required")
	}
	if err := validateTLSBrokerURL(c.BrokerURL); err != nil {
		return c, err
	}

	if c.ClientID, ok = nonEmpty(lookup, "GATEWAY_MQTT_CLIENT_ID"); !ok {
		return c, fmt.Errorf("GATEWAY_MQTT_CLIENT_ID is required")
	}
	if err := validateToken("GATEWAY_MQTT_CLIENT_ID", c.ClientID, maxClientIDLength); err != nil {
		return c, err
	}
	if c.ClientID == c.GatewayID {
		return c, fmt.Errorf("GATEWAY_MQTT_CLIENT_ID must differ from GATEWAY_ID")
	}

	if c.Username, ok = nonEmpty(lookup, "GATEWAY_MQTT_USERNAME"); !ok {
		return c, fmt.Errorf("GATEWAY_MQTT_USERNAME is required")
	}
	if err := validateToken("GATEWAY_MQTT_USERNAME", c.Username, maxUsernameLength); err != nil {
		return c, err
	}
	if c.Username != c.GatewayID {
		return c, fmt.Errorf("GATEWAY_MQTT_USERNAME must equal GATEWAY_ID")
	}

	if err = loadPassword(lookup, &c); err != nil {
		return c, err
	}

	if c.CAFile, ok = nonEmpty(lookup, "GATEWAY_MQTT_CA_FILE"); !ok {
		return c, fmt.Errorf("GATEWAY_MQTT_CA_FILE is required")
	}
	if err = validateCleanAbsolutePath("GATEWAY_MQTT_CA_FILE", c.CAFile); err != nil {
		return c, err
	}

	if c.OutboxPath, ok = nonEmpty(lookup, "GATEWAY_OUTBOX_PATH"); !ok {
		return c, fmt.Errorf("GATEWAY_OUTBOX_PATH is required")
	}
	if err = validateOutboxPath(c.OutboxPath); err != nil {
		return c, err
	}

	if c.SampleRateHz, err = integer(lookup, "GATEWAY_SAMPLE_RATE_HZ", defaultSampleRateHz, 1, maxSampleRateHz); err != nil {
		return c, err
	}
	if c.SamplesPerBatch, err = integer(lookup, "GATEWAY_SAMPLES_PER_BATCH", defaultSamplesPerBatch, 1, maxSamplesPerBatch); err != nil {
		return c, err
	}
	intervalUS, span, err := validateSampleProfile(c.GatewayID, c.SensorID, c.SampleRateHz, c.SamplesPerBatch)
	if err != nil {
		return c, err
	}
	c.SampleIntervalUS = intervalUS
	c.BatchSpan = span
	c.MaxPayloadBytes = ServerLimits().MaxPayloadBytes

	if c.AckTimeout, err = boundedDuration(lookup, "GATEWAY_ACK_TIMEOUT", defaultAckTimeout, minTimeout, maxTimeout); err != nil {
		return c, err
	}
	if c.RetryMinDelay, err = boundedDuration(lookup, "GATEWAY_RETRY_MIN_DELAY", defaultRetryMin, minTimeout, maxRetryDelay); err != nil {
		return c, err
	}
	if c.RetryMaxDelay, err = boundedDuration(lookup, "GATEWAY_RETRY_MAX_DELAY", defaultRetryMax, minTimeout, maxRetryDelay); err != nil {
		return c, err
	}
	if c.RetryMinDelay > c.RetryMaxDelay {
		return c, fmt.Errorf("GATEWAY_RETRY_MIN_DELAY must be less than or equal to GATEWAY_RETRY_MAX_DELAY")
	}

	if c.ConnectTimeout, err = boundedDuration(lookup, "GATEWAY_MQTT_CONNECT_TIMEOUT", defaultConnectTimeout, minTimeout, maxTimeout); err != nil {
		return c, err
	}
	if c.SubscribeTimeout, err = boundedDuration(lookup, "GATEWAY_MQTT_SUBSCRIBE_TIMEOUT", defaultSubscribeTimeout, minTimeout, maxTimeout); err != nil {
		return c, err
	}
	if c.PublishTimeout, err = boundedDuration(lookup, "GATEWAY_MQTT_PUBLISH_TIMEOUT", defaultPublishTimeout, minTimeout, maxTimeout); err != nil {
		return c, err
	}
	if c.SQLiteTimeout, err = boundedDuration(lookup, "GATEWAY_SQLITE_TIMEOUT", defaultSQLiteTimeout, minTimeout, maxTimeout); err != nil {
		return c, err
	}
	if c.ShutdownTimeout, err = boundedDuration(lookup, "GATEWAY_SHUTDOWN_TIMEOUT", defaultShutdownTimeout, minTimeout, maxTimeout); err != nil {
		return c, err
	}

	if c.MaxInflight, err = integer(lookup, "GATEWAY_MAX_INFLIGHT", defaultMaxInflight, 1, maxMaxInflight); err != nil {
		return c, err
	}
	if c.OutboxMaxRows, err = integer(lookup, "GATEWAY_OUTBOX_MAX_ROWS", defaultOutboxMaxRows, 1, maxOutboxMaxRows); err != nil {
		return c, err
	}
	if c.OutboxMaxBytes, err = integer64(lookup, "GATEWAY_OUTBOX_MAX_BYTES", defaultOutboxMaxBytes, 1, maxOutboxMaxBytes); err != nil {
		return c, err
	}
	if c.MaxInflight > c.OutboxMaxRows {
		return c, fmt.Errorf("GATEWAY_MAX_INFLIGHT must not exceed GATEWAY_OUTBOX_MAX_ROWS")
	}

	return c, nil
}

// loadPassword reads the MQTT password preserving its raw bytes while rejecting
// blank, oversized or control-character values. The value is never logged.
func loadPassword(lookup LookupFunc, c *Config) error {
	raw, ok := lookup("GATEWAY_MQTT_PASSWORD")
	if !ok || strings.TrimSpace(raw) == "" {
		return fmt.Errorf("GATEWAY_MQTT_PASSWORD is required")
	}
	if len(raw) > maxPasswordLength || strings.ContainsAny(raw, "\x00\r\n") {
		return fmt.Errorf("GATEWAY_MQTT_PASSWORD is invalid or too long")
	}
	c.Password = raw
	return nil
}

// validateTLSBrokerURL accepts only an ssl:// URL with an explicit host and
// port and no userinfo, path, query or fragment. CA and hostname/SAN
// verification still happen when the transport connects.
func validateTLSBrokerURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "ssl" || u.Hostname() == "" || u.Port() == "" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("GATEWAY_MQTT_BROKER_URL must be an ssl:// URL with host and port and no userinfo, path, query or fragment")
	}
	return nil
}

// validateToken rejects blank, oversized or control-character identity values.
// The raw value is preserved, never trimmed.
func validateToken(key, value string, max int) error {
	if value == "" || len(value) > max || strings.ContainsAny(value, "\x00\r\n") {
		return fmt.Errorf("%s is invalid or too long", key)
	}
	return nil
}

// validateCleanAbsolutePath requires a clean absolute path with no embedded
// control characters so a relative or ambiguous path cannot silently resolve.
func validateCleanAbsolutePath(key, value string) error {
	if !filepath.IsAbs(value) || filepath.Clean(value) != value || strings.ContainsAny(value, "\x00\r\n") {
		return fmt.Errorf("%s must be a clean absolute path", key)
	}
	return nil
}

// validateOutboxPath requires a durable file path and rejects an in-memory
// database, which would lose pending batches when the process stops.
func validateOutboxPath(value string) error {
	if value == ":memory:" {
		return fmt.Errorf("GATEWAY_OUTBOX_PATH must be a durable file path, not the in-memory database")
	}
	return validateCleanAbsolutePath("GATEWAY_OUTBOX_PATH", value)
}

func nonEmpty(lookup LookupFunc, key string) (string, bool) {
	value, ok := lookup(key)
	value = strings.TrimSpace(value)
	return value, ok && value != ""
}

func integer(lookup LookupFunc, key string, fallback, minimum, maximum int) (int, error) {
	raw, ok := nonEmpty(lookup, key)
	if !ok {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < minimum || value > maximum {
		return 0, fmt.Errorf("%s must be an integer between %d and %d", key, minimum, maximum)
	}
	return value, nil
}

func integer64(lookup LookupFunc, key string, fallback, minimum, maximum int64) (int64, error) {
	raw, ok := nonEmpty(lookup, key)
	if !ok {
		return fallback, nil
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value < minimum || value > maximum {
		return 0, fmt.Errorf("%s must be an integer between %d and %d", key, minimum, maximum)
	}
	return value, nil
}

func boundedDuration(lookup LookupFunc, key string, fallback, minimum, maximum time.Duration) (time.Duration, error) {
	raw, ok := nonEmpty(lookup, key)
	if !ok {
		return fallback, nil
	}
	value, err := time.ParseDuration(raw)
	if err != nil || value < minimum || value > maximum {
		return 0, fmt.Errorf("%s must be a duration between %s and %s", key, minimum, maximum)
	}
	return value, nil
}
