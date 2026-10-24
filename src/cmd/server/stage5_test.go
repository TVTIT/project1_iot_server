package main

import (
	"testing"

	"iot-platform/internal/config"
	"iot-platform/internal/digitaltwin"
	"iot-platform/internal/httpserver"
)

func TestStage5CompositionFlagAndDependencies(t *testing.T) {
	var deps httpserver.RouterDependencies
	cfg := validTestConfig()
	if err := populateStage5Dependencies(&deps, cfg, nil); err != nil || deps.ControlAPIEnabled || deps.ControlRepository != nil {
		t.Fatal("disabled stage 5 required dependencies", err)
	}
	cfg.Stage5 = config.Stage5Config{Enabled: true, Control: digitaltwin.DefaultControlConfig()}
	if err := populateStage5Dependencies(&deps, cfg, nil); err == nil {
		t.Fatal("enabled without pool")
	}
	pool := testPool(t)
	if err := populateStage5Dependencies(&deps, cfg, pool); err != nil || deps.ControlRepository == nil || deps.ControlRequestTimeout != cfg.Stage5.Control.DBTimeout {
		t.Fatal("composition", err)
	}
	cfg.Stage5.Control.TTL = 0
	if err := populateStage5Dependencies(&deps, cfg, pool); err == nil {
		t.Fatal("invalid bounded config")
	}
	if err := populateStage5Dependencies(nil, cfg, pool); err == nil {
		t.Fatal("nil destination")
	}
}
