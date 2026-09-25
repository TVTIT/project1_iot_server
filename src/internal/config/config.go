// Package config loads and validates backend process configuration.
package config

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// LookupFunc returns an environment value and whether it is set.
type LookupFunc func(string) (string, bool)

// Config contains process-level settings required by the backend.
type Config struct {
	ServerPort             int
	ServerEnv              string
	DatabaseURL            string
	DatabaseMaxConns       int32
	DatabaseMinConns       int32
	DatabaseConnectTimeout time.Duration
	ReadinessTimeout       time.Duration
	HTTPReadHeaderTimeout  time.Duration
	HTTPReadTimeout        time.Duration
	HTTPWriteTimeout       time.Duration
	HTTPIdleTimeout        time.Duration
	ShutdownTimeout        time.Duration
	MQTTQueueCapacity      int
	MQTTWorkerCount        int
	MQTTMaxPayloadBytes    int
}

// LoadFromEnvironment loads configuration from the process environment.
func LoadFromEnvironment() (Config, error) {
	return Load(os.LookupEnv)
}

// Load parses and validates configuration using lookup.
func Load(lookup LookupFunc) (Config, error) {
	databaseURL, ok := nonEmpty(lookup, "DATABASE_URL")
	if !ok {
		return Config{}, fmt.Errorf("DATABASE_URL is required")
	}
	parsedURL, err := url.Parse(databaseURL)
	if err != nil || (parsedURL.Scheme != "postgres" && parsedURL.Scheme != "postgresql") || parsedURL.Host == "" {
		return Config{}, fmt.Errorf("DATABASE_URL must be a valid postgres URL")
	}

	cfg := Config{
		DatabaseURL: databaseURL,
		ServerEnv:   valueOrDefault(lookup, "SERVER_ENV", "development"),
	}

	if cfg.ServerPort, err = integer(lookup, "SERVER_PORT", 8080, 1, 65535); err != nil {
		return Config{}, err
	}
	maxConns, err := integer(lookup, "DATABASE_MAX_CONNS", 10, 1, 1000)
	if err != nil {
		return Config{}, err
	}
	cfg.DatabaseMaxConns = int32(maxConns)
	minConns, err := integer(lookup, "DATABASE_MIN_CONNS", 1, 0, maxConns)
	if err != nil {
		return Config{}, err
	}
	cfg.DatabaseMinConns = int32(minConns)

	if cfg.DatabaseConnectTimeout, err = duration(lookup, "DATABASE_CONNECT_TIMEOUT", 10*time.Second); err != nil {
		return Config{}, err
	}
	if cfg.ReadinessTimeout, err = duration(lookup, "READINESS_TIMEOUT", 2*time.Second); err != nil {
		return Config{}, err
	}
	if cfg.HTTPReadHeaderTimeout, err = duration(lookup, "HTTP_READ_HEADER_TIMEOUT", 5*time.Second); err != nil {
		return Config{}, err
	}
	if cfg.HTTPReadTimeout, err = duration(lookup, "HTTP_READ_TIMEOUT", 15*time.Second); err != nil {
		return Config{}, err
	}
	if cfg.HTTPWriteTimeout, err = duration(lookup, "HTTP_WRITE_TIMEOUT", 30*time.Second); err != nil {
		return Config{}, err
	}
	if cfg.HTTPIdleTimeout, err = duration(lookup, "HTTP_IDLE_TIMEOUT", 60*time.Second); err != nil {
		return Config{}, err
	}
	if cfg.ShutdownTimeout, err = duration(lookup, "SHUTDOWN_TIMEOUT", 10*time.Second); err != nil {
		return Config{}, err
	}

	if cfg.MQTTQueueCapacity, err = integer(lookup, "MQTT_QUEUE_CAPACITY", 256, 1, 1_000_000); err != nil {
		return Config{}, err
	}
	if cfg.MQTTWorkerCount, err = integer(lookup, "MQTT_WORKER_COUNT", 4, 1, 10_000); err != nil {
		return Config{}, err
	}
	if cfg.MQTTMaxPayloadBytes, err = integer(lookup, "MQTT_MAX_PAYLOAD_BYTES", 1_048_576, 1, 268_435_455); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

func nonEmpty(lookup LookupFunc, key string) (string, bool) {
	value, ok := lookup(key)
	value = strings.TrimSpace(value)
	return value, ok && value != ""
}

func valueOrDefault(lookup LookupFunc, key, fallback string) string {
	if value, ok := nonEmpty(lookup, key); ok {
		return value
	}
	return fallback
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

func duration(lookup LookupFunc, key string, fallback time.Duration) (time.Duration, error) {
	raw, ok := nonEmpty(lookup, key)
	if !ok {
		return fallback, nil
	}
	value, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s has invalid duration syntax: %w", key, err)
	}
	if value <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration", key)
	}
	return value, nil
}
