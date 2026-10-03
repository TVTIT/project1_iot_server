// Package main initializes the broker authentication volume and drops bootstrap privileges.
//
//nolint:misspell // Mosquitto is the product name used in fixed runtime paths.
package main

import (
	"context"
	"errors"
	"log"
	"os"
	"syscall"
	"time"

	"iot-platform/internal/mqttcredential"
)

// Bootstrap touches only the two fixed named-volume roots, never descendants
// or host paths. Existing stores must already belong to the runtime identity.
func bootstrap() error {
	for _, path := range []string{"/mosquitto/auth", "/mosquitto/control"} {
		st, err := os.Lstat(path)
		if err != nil || !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
			return errors.New("invalid volume root")
		}
		s := st.Sys().(*syscall.Stat_t)
		if s.Uid == 0 && s.Gid == 0 {
			entries, err := os.ReadDir(path)
			if err != nil || len(entries) != 0 {
				return errors.New("nonempty unowned volume")
			}
			if os.Chown(path, 1883, 1883) != nil {
				return errors.New("volume ownership unavailable")
			}
			if os.Chmod(path, 0700) != nil {
				return errors.New("volume permissions unavailable")
			}
		} else if s.Uid != 1883 || s.Gid != 1883 {
			return errors.New("unexpected volume owner")
		} else if st.Mode().Perm() != 0700 {
			return errors.New("unexpected volume permissions")
		}
	}
	if syscall.Setgroups([]int{}) != nil || syscall.Setgid(1883) != nil || syscall.Setuid(1883) != nil {
		return errors.New("identity drop failed")
	}
	return nil
}

func run(get func(string) string) error {
	password := get("MQTT_PASSWORD")
	if get("MQTT_USERNAME") != mqttcredential.BackendUsername || mqttcredential.ValidateBackendPassword(password) != nil {
		return errors.New("invalid initializer configuration")
	}
	if os.Geteuid() == 0 {
		if err := bootstrap(); err != nil {
			return err
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return mqttcredential.Initialize(ctx, "/mosquitto/auth", password, mqttcredential.NativeTool{Path: "/usr/bin/mosquitto_passwd", Timeout: 3 * time.Second})
}

func main() {
	if run(os.Getenv) != nil {
		log.Print("auth initialization failed: configuration, permissions or recovery required")
		os.Exit(1)
	}
}
