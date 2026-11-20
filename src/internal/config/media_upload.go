package config

import (
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"iot-platform/internal/mediaprotocol"
	"iot-platform/internal/mqttcredential"
	"iot-platform/internal/mqttingress"
)

// MediaRuntimeConfig defines the runtime options for media MQTT transport, Storage client, and worker pools.
type MediaRuntimeConfig struct {
	Data                                                         mqttingress.TransportOptions
	Management                                                   mqttcredential.DynSecConfig
	StorageInternalURL, StoragePublicOrigin, StorageServiceToken string
	SigningTimeout, GrantTTL, InFlightGrace                      time.Duration
	Workers                                                      int
}

func (MediaRuntimeConfig) String() string { return "MediaRuntimeConfig{redacted}" }

// GoString returns a redacted string representation of MediaRuntimeConfig to prevent credential leakage.
func (c MediaRuntimeConfig) GoString() string { return c.String() }

func loadMediaRuntime(lookup LookupFunc, enabled bool) (MediaRuntimeConfig, error) {
	c := MediaRuntimeConfig{}
	var err error
	if c.SigningTimeout, err = boundedDuration(lookup, "MEDIA_UPLOAD_SIGN_TIMEOUT", 5*time.Second, time.Millisecond, time.Minute); err != nil {
		return c, err
	}
	ttlSeconds, err := integer(lookup, "MEDIA_UPLOAD_GRANT_TTL_SECONDS", 60, 1, 86400)
	if err != nil {
		return c, err
	}
	c.GrantTTL = time.Duration(ttlSeconds) * time.Second
	if c.InFlightGrace, err = boundedDuration(lookup, "MEDIA_UPLOAD_INFLIGHT_GRACE", 2*time.Minute, time.Second, 10*time.Minute); err != nil {
		return c, err
	}
	if c.Workers, err = integer(lookup, "MEDIA_UPLOAD_REQUEST_WORKERS", 2, 1, 4); err != nil {
		return c, err
	}
	if !enabled {
		return c, nil
	}

	for _, p := range []struct {
		key string
		dst *string
		max int
	}{
		{"MQTT_BROKER_URL", &c.Data.BrokerURL, 256},
		{"MEDIA_UPLOAD_MQTT_CLIENT_ID", &c.Data.ClientID, 128},
		{"MQTT_USERNAME", &c.Data.Username, 128},
		{"MQTT_PASSWORD", &c.Data.Password, 512},
		{"MQTT_TLS_CA_FILE", &c.Data.CAFile, 1024},
	} {
		if *p.dst, err = boundedIngressString(lookup, p.key, p.max); err != nil {
			return c, fmt.Errorf("%s required and bounded when MEDIA_UPLOAD_ENABLED", p.key)
		}
	}
	if err = validateTLSBrokerURL(c.Data.BrokerURL); err != nil {
		return c, err
	}
	if !filepath.IsAbs(c.Data.CAFile) || filepath.Clean(c.Data.CAFile) != c.Data.CAFile {
		return c, fmt.Errorf("media MQTT CA must be a clean absolute path")
	}
	if c.Data.ClientID == valueOrDefault(lookup, "MQTT_CLIENT_ID", "backend_ingress") ||
		c.Data.ClientID == valueOrDefault(lookup, "STAGE5_MQTT_CLIENT_ID", "backend_control") {
		return c, fmt.Errorf("media MQTT client ID must be dedicated")
	}
	if c.Data.Username == valueOrDefault(lookup, "MQTT_DYNSEC_MANAGER_USERNAME", "") ||
		c.Data.Password == valueOrDefault(lookup, "MQTT_DYNSEC_MANAGER_PASSWORD", "") {
		return c, fmt.Errorf("media MQTT data and manager identities must differ")
	}
	c.Data.Filter = "gateways/+/media/up/#"
	c.Data.ConnectTimeout = 5 * time.Second
	c.Data.SubscribeTimeout = 3 * time.Second
	c.Data.PublishTimeout = 3 * time.Second
	c.Data.CloseWaitTimeout = time.Second
	c.Data.MaxConcurrentPublishes = c.Workers
	c.Management.BrokerURL, _ = nonEmpty(lookup, "MQTT_INTERNAL_MANAGEMENT_URL")
	c.Management.ManagerUsername, _ = nonEmpty(lookup, "MQTT_DYNSEC_MANAGER_USERNAME")
	c.Management.ManagerPassword, _ = nonEmpty(lookup, "MQTT_DYNSEC_MANAGER_PASSWORD")
	if c.Management.BrokerURL == "" || c.Management.ManagerUsername == "" || c.Management.ManagerPassword == "" {
		return c, fmt.Errorf("MQTT management readback settings required when MEDIA_UPLOAD_ENABLED")
	}
	if err = validateTLSBrokerURL(c.Management.BrokerURL); err != nil {
		return c, err
	}
	c.Management.Timeout = 5 * time.Second
	c.Management.MaxPayloadBytes, c.Management.MaxInflight, c.Management.QueueSize = 65536, 1, 8
	c.Management.ProtectedUsernames = []string{c.Data.Username, c.Management.ManagerUsername}
	internal := valueOrDefault(lookup, "STORAGE_INTERNAL_URL", "http://supabase-storage:5000")
	u, parseErr := url.Parse(internal)
	if parseErr != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil ||
		(u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
		return c, fmt.Errorf("STORAGE_INTERNAL_URL must be an internal HTTP(S) origin")
	}
	c.StorageInternalURL = strings.TrimRight(internal, "/")
	c.StoragePublicOrigin, _ = boundedIngressString(lookup, "MEDIA_UPLOAD_PUBLIC_ORIGIN", 512)
	c.StorageServiceToken, _ = boundedIngressString(lookup, "SERVICE_ROLE_KEY", 2048)
	public, publicErr := url.Parse(c.StoragePublicOrigin)
	if publicErr != nil || public.Scheme != "https" || public.Host == "" || public.User != nil ||
		public.Path != "" || public.RawPath != "" || public.RawQuery != "" || public.Fragment != "" {
		return c, fmt.Errorf("MEDIA_UPLOAD_PUBLIC_ORIGIN must be an HTTPS origin")
	}
	if c.StorageServiceToken == "" {
		return c, fmt.Errorf("media Storage origin and service token required when MEDIA_UPLOAD_ENABLED")
	}
	return c, nil
}

// MediaUploadConfig contains finite admission and worker budgets for the
// managed-media slices. These values are process configuration only; they do
// not create a Storage client or start workers.
type MediaUploadConfig struct {
	MaxUploadBytes int64
	// MaxPendingPerGateway caps unsettled managed intents for one Gateway,
	// defined identically to the unsettled scope (pending/verifying or
	// rejected/expired with unconfirmed cleanup).
	MaxPendingPerGateway int
	// MaxUnsettledIntents includes terminal intents whose object cleanup has
	// not been confirmed, because their bytes may still occupy temporary storage.
	MaxUnsettledIntents  int
	TemporaryBytes       int64
	StoredBytes          int64
	IntentTTL            time.Duration
	DBTimeout            time.Duration
	StorageTimeout       time.Duration
	VerifyWorkers        int
	VerifyBatchSize      int
	VerifyScanInterval   time.Duration
	VerifyRetryMinDelay  time.Duration
	VerifyRetryMaxDelay  time.Duration
	VerifyMaxAttempts    int
	CleanupWorkers       int
	CleanupBatchSize     int
	CleanupScanInterval  time.Duration
	CleanupRetryMinDelay time.Duration
	CleanupRetryMaxDelay time.Duration
	CleanupMaxAttempts   int
}

func loadMediaUpload(lookup LookupFunc) (MediaUploadConfig, error) {
	c := MediaUploadConfig{MaxUploadBytes: mediaprotocol.MaxUploadBytes}
	var err error
	ints := []struct {
		key           string
		dst           *int
		def, min, max int
	}{
		{"MEDIA_UPLOAD_MAX_PENDING_PER_GATEWAY", &c.MaxPendingPerGateway, 1, 1, 8},
		{"MEDIA_UPLOAD_MAX_UNSETTLED_INTENTS", &c.MaxUnsettledIntents, 8, 1, 64},
		{"MEDIA_UPLOAD_VERIFY_WORKERS", &c.VerifyWorkers, 1, 1, 8},
		{"MEDIA_UPLOAD_VERIFY_BATCH_SIZE", &c.VerifyBatchSize, 4, 1, 64},
		{"MEDIA_UPLOAD_VERIFY_MAX_ATTEMPTS", &c.VerifyMaxAttempts, 5, 1, 20},
		{"MEDIA_UPLOAD_CLEANUP_WORKERS", &c.CleanupWorkers, 1, 1, 4},
		{"MEDIA_UPLOAD_CLEANUP_BATCH_SIZE", &c.CleanupBatchSize, 4, 1, 64},
		{"MEDIA_UPLOAD_CLEANUP_MAX_ATTEMPTS", &c.CleanupMaxAttempts, 5, 2, 20},
	}
	for _, item := range ints {
		if *item.dst, err = integer(lookup, item.key, item.def, item.min, item.max); err != nil {
			return MediaUploadConfig{}, err
		}
	}
	int64s := []struct {
		key      string
		dst      *int64
		def, max int64
	}{
		{"MEDIA_UPLOAD_TEMPORARY_BYTES", &c.TemporaryBytes, 640_000_000, 5_120_000_000},
		{"MEDIA_UPLOAD_STORED_BYTES", &c.StoredBytes, 1_600_000_000, 80_000_000_000},
	}
	for _, item := range int64s {
		if *item.dst, err = integer64(lookup, item.key, item.def, c.MaxUploadBytes, item.max); err != nil {
			return MediaUploadConfig{}, err
		}
	}
	durations := []struct {
		key      string
		dst      *time.Duration
		def, max time.Duration
	}{
		{"MEDIA_UPLOAD_INTENT_TTL", &c.IntentTTL, 15 * time.Minute, 24 * time.Hour},
		{"MEDIA_UPLOAD_DB_TIMEOUT", &c.DBTimeout, 5 * time.Second, time.Minute},
		{"MEDIA_UPLOAD_STORAGE_TIMEOUT", &c.StorageTimeout, 30 * time.Second, 5 * time.Minute},
		{"MEDIA_UPLOAD_VERIFY_SCAN_INTERVAL", &c.VerifyScanInterval, 5 * time.Second, time.Hour},
		{"MEDIA_UPLOAD_VERIFY_RETRY_MIN_DELAY", &c.VerifyRetryMinDelay, 5 * time.Second, time.Hour},
		{"MEDIA_UPLOAD_VERIFY_RETRY_MAX_DELAY", &c.VerifyRetryMaxDelay, time.Minute, 24 * time.Hour},
		{"MEDIA_UPLOAD_CLEANUP_SCAN_INTERVAL", &c.CleanupScanInterval, 10 * time.Second, time.Hour},
		{"MEDIA_UPLOAD_CLEANUP_RETRY_MIN_DELAY", &c.CleanupRetryMinDelay, 10 * time.Second, time.Hour},
		{"MEDIA_UPLOAD_CLEANUP_RETRY_MAX_DELAY", &c.CleanupRetryMaxDelay, 5 * time.Minute, 24 * time.Hour},
	}
	for _, item := range durations {
		if *item.dst, err = boundedDuration(lookup, item.key, item.def, time.Millisecond, item.max); err != nil {
			return MediaUploadConfig{}, err
		}
	}
	if c.MaxPendingPerGateway > c.MaxUnsettledIntents {
		return MediaUploadConfig{}, fmt.Errorf("MEDIA_UPLOAD_MAX_PENDING_PER_GATEWAY must not exceed MEDIA_UPLOAD_MAX_UNSETTLED_INTENTS")
	}
	minTemporary := int64(c.MaxUnsettledIntents) * c.MaxUploadBytes
	if c.TemporaryBytes < minTemporary {
		return MediaUploadConfig{}, fmt.Errorf("MEDIA_UPLOAD_TEMPORARY_BYTES must cover MAX_UNSETTLED_INTENTS times the fixed upload cap")
	}
	if c.VerifyRetryMinDelay > c.VerifyRetryMaxDelay {
		return MediaUploadConfig{}, fmt.Errorf("MEDIA_UPLOAD_VERIFY_RETRY_MIN_DELAY must not exceed MEDIA_UPLOAD_VERIFY_RETRY_MAX_DELAY")
	}
	if c.CleanupRetryMinDelay > c.CleanupRetryMaxDelay {
		return MediaUploadConfig{}, fmt.Errorf("MEDIA_UPLOAD_CLEANUP_RETRY_MIN_DELAY must not exceed MEDIA_UPLOAD_CLEANUP_RETRY_MAX_DELAY")
	}
	return c, nil
}
