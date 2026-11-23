package config

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"iot-platform/internal/mediastorage"
)

// MediaPlaybackConfig defines the configuration options for media playback signed URL issuing.
type MediaPlaybackConfig struct {
	PublicOrigin        string
	StorageInternalURL  string
	StorageServiceToken string `json:"-"`
	SigningTimeout      time.Duration
}

func (MediaPlaybackConfig) String() string { return "MediaPlaybackConfig{redacted}" }

// GoString returns a redacted string representation of MediaPlaybackConfig to prevent credential leakage.
func (c MediaPlaybackConfig) GoString() string { return c.String() }

func strictPlaybackBoolValue(lookup LookupFunc, key string, fallback bool) (bool, error) {
	val, ok := lookup(key)
	if !ok || val == "" {
		return fallback, nil
	}
	switch val {
	case "true":
		return true, nil
	case "false":
		return false, nil
	default:
		return false, fmt.Errorf("%s must be exact literal 'true' or 'false'", key)
	}
}

func loadMediaPlayback(lookup LookupFunc, enabled bool) (MediaPlaybackConfig, error) {
	c := MediaPlaybackConfig{}
	var err error
	if c.SigningTimeout, err = boundedDuration(lookup, "MEDIA_PLAYBACK_SIGN_TIMEOUT", 5*time.Second, time.Millisecond, time.Minute); err != nil {
		return c, err
	}
	if !enabled {
		return c, nil
	}

	internal := valueOrDefault(lookup, "STORAGE_INTERNAL_URL", "http://supabase-storage:5000")
	u, parseErr := url.Parse(internal)
	if parseErr != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil ||
		(u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
		return c, fmt.Errorf("STORAGE_INTERNAL_URL must be an internal HTTP(S) origin")
	}
	c.StorageInternalURL = strings.TrimRight(internal, "/")

	c.PublicOrigin, _ = boundedIngressString(lookup, "MEDIA_PLAYBACK_PUBLIC_ORIGIN", 512)
	c.StorageServiceToken, _ = boundedIngressString(lookup, "MEDIA_PLAYBACK_STORAGE_TOKEN", 2048)
	if c.PublicOrigin == "" {
		return c, fmt.Errorf("MEDIA_PLAYBACK_PUBLIC_ORIGIN is required when MEDIA_PLAYBACK_ENABLED")
	}
	if _, err := mediastorage.ValidatePlaybackOrigin(c.PublicOrigin); err != nil {
		return c, fmt.Errorf("MEDIA_PLAYBACK_PUBLIC_ORIGIN must be an HTTPS origin: %w", err)
	}
	if c.StorageServiceToken == "" {
		return c, fmt.Errorf("MEDIA_PLAYBACK_STORAGE_TOKEN is required when MEDIA_PLAYBACK_ENABLED")
	}
	if err := validatePlaybackStorageToken(c.StorageServiceToken); err != nil {
		return c, err
	}
	return c, nil
}

func validatePlaybackStorageToken(token string) error {
	parts := strings.Split(token, ".")
	if len(parts) == 3 {
		payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
		if err != nil {
			payloadBytes, err = base64.URLEncoding.DecodeString(parts[1])
		}
		if err == nil {
			var claims struct {
				Role string `json:"role"`
			}
			if err := json.Unmarshal(payloadBytes, &claims); err == nil && claims.Role != "" {
				if claims.Role != "storage_playback_signer" {
					return fmt.Errorf("MEDIA_PLAYBACK_STORAGE_TOKEN must have role 'storage_playback_signer', got %q", claims.Role)
				}
			}
		}
	}
	return nil
}
