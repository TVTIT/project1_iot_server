// Command mosquitto-credential-controller is an isolated lifecycle composition
// root, not wired into production images/Compose. Run as the broker UID (1883)
// and container PID1. Startup JSON on stdin contains paths/limits only, never
// manager passwords, PostgreSQL/JWT secrets or commands from an API request.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"iot-platform/internal/mqttcredential"
)

type startup struct {
	Lifecycle  mqttcredential.LifecycleConfig
	Controller mqttcredential.ControllerConfig
}

func run(ctx context.Context, input io.Reader) error {
	var cfg startup
	d := json.NewDecoder(io.LimitReader(input, 65537))
	d.DisallowUnknownFields()
	if d.Decode(&cfg) != nil {
		return mqttcredential.ErrLifecycleUnavailable
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return mqttcredential.ErrLifecycleUnavailable
	}
	// Child command identity is fixed; IPC cannot select exec/config targets.
	if cfg.Lifecycle.Executable != "/usr/sbin/mosquitto" || len(cfg.Lifecycle.Args) != 2 || cfg.Lifecycle.Args[0] != "-c" || cfg.Controller.UID != 1883 || os.Geteuid() != 1883 {
		return mqttcredential.ErrLifecycleUnavailable
	}
	if !filepath.IsAbs(cfg.Lifecycle.Args[1]) || filepath.Clean(cfg.Lifecycle.Args[1]) != cfg.Lifecycle.Args[1] {
		return mqttcredential.ErrLifecycleUnavailable
	}
	l, e := mqttcredential.NewBrokerLifecycle(cfg.Lifecycle)
	if e != nil {
		return e
	}
	controller, e := mqttcredential.NewLifecycleController(l, cfg.Controller)
	if e != nil {
		return e
	}
	if e = l.StartClosed(); e != nil {
		return e
	}
	defer l.Shutdown()
	if e = controller.Serve(ctx); e != nil {
		return e
	}
	select {
	case <-l.Failed():
		return mqttcredential.ErrLifecycleUnavailable
	default:
		return nil
	}
}
func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	if run(ctx, os.Stdin) != nil {
		fmt.Fprintln(os.Stderr, "broker controller unavailable")
		os.Exit(1)
	}
}
