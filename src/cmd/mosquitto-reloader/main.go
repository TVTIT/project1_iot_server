// Package main runs the bounded Unix-socket broker signal sidecar.
//
//nolint:misspell // Mosquitto is the product name used in diagnostics.
package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"iot-platform/internal/mosquittoreload"
)

// This standalone process reads only its three control settings, never backend
// config (JWT/database/MQTT passwords are not required or logged).
func loadConfig(get func(string) string) (mosquittoreload.Config, error) {
	c := mosquittoreload.Config{SocketPath: get("MQTT_RELOAD_SOCKET"), Timeout: 2 * time.Second, MaxConnections: 8}
	var e error
	if v := get("MQTT_RELOAD_TIMEOUT"); v != "" {
		c.Timeout, e = time.ParseDuration(v)
		if e != nil || c.Timeout <= 0 {
			return c, mosquittoreload.ErrUnavailable
		}
	}
	if v := get("MQTT_RELOAD_MAX_CONNECTIONS"); v != "" {
		c.MaxConnections, e = strconv.Atoi(v)
		if e != nil || c.MaxConnections <= 0 {
			return c, mosquittoreload.ErrUnavailable
		}
	}
	_, e = mosquittoreload.NewServer(c)
	return c, e
}

func run(ctx context.Context, get func(string) string) error {
	c, e := loadConfig(get)
	if e != nil {
		return errors.New("invalid reloader configuration")
	}
	s, e := mosquittoreload.NewServer(c)
	if e != nil {
		return mosquittoreload.ErrUnavailable
	}
	return s.Serve(ctx)
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(exitCode(ctx, os.Getenv))
}

func exitCode(ctx context.Context, get func(string) string) int {
	if run(ctx, get) != nil {
		log.Print("mosquitto reloader stopped: configuration or control unavailable")
		return 1
	}
	return 0
}
