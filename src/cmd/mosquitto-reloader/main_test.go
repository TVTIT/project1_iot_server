package main

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestMainFailure(t *testing.T) {
	if os.Getenv("RELOADER_TEST_MAIN") == "1" {
		main()
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestMainFailure$")
	cmd.Env = []string{"RELOADER_TEST_MAIN=1", "MQTT_RELOAD_SOCKET=relative", "DATABASE_PASSWORD=sentinel-do-not-log"}
	b, err := cmd.CombinedOutput()
	if err == nil || !strings.Contains(string(b), "control unavailable") || strings.Contains(string(b), "sentinel") {
		t.Fatalf("unsafe main failure: %v", err)
	}
}

func TestConfig(t *testing.T) {
	if exitCode(context.Background(), func(string) string { return "" }) != 1 {
		t.Fatal("missing configuration")
	}
	for _, v := range []map[string]string{
		{}, {"MQTT_RELOAD_SOCKET": "relative"},
		{"MQTT_RELOAD_SOCKET": "/control/r.sock", "MQTT_RELOAD_TIMEOUT": "bad"},
		{"MQTT_RELOAD_SOCKET": "/control/r.sock", "MQTT_RELOAD_TIMEOUT": "0s"},
		{"MQTT_RELOAD_SOCKET": "/control/r.sock", "MQTT_RELOAD_TIMEOUT": "31s"},
		{"MQTT_RELOAD_SOCKET": "/control/r.sock", "MQTT_RELOAD_MAX_CONNECTIONS": "bad"},
		{"MQTT_RELOAD_SOCKET": "/control/r.sock", "MQTT_RELOAD_MAX_CONNECTIONS": "0"},
		{"MQTT_RELOAD_SOCKET": "/control/r.sock", "MQTT_RELOAD_MAX_CONNECTIONS": "129"},
	} {
		if run(context.Background(), func(k string) string { return v[k] }) == nil {
			t.Fatal(v)
		}
	}
	v := map[string]string{"MQTT_RELOAD_SOCKET": "/control/r.sock", "MQTT_RELOAD_TIMEOUT": "1s", "MQTT_RELOAD_MAX_CONNECTIONS": "4"}
	get := func(k string) string { return v[k] }
	c, e := loadConfig(get)
	if e != nil || c.Timeout != time.Second || c.MaxConnections != 4 {
		t.Fatal(c, e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if e := run(ctx, get); e != nil {
		t.Fatal(e)
	}
	if exitCode(ctx, get) != 0 {
		t.Fatal("cancelled shutdown")
	}
	delete(v, "MQTT_RELOAD_TIMEOUT")
	delete(v, "MQTT_RELOAD_MAX_CONNECTIONS")
	c, e = loadConfig(get)
	if e != nil || c.Timeout != 2*time.Second || c.MaxConnections != 8 {
		t.Fatal(c, e)
	}
}
