package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"iot-platform/internal/paperload"
)

func writeManifest(t *testing.T, profile string) string {
	t.Helper()
	m := paperload.Manifest{SchemaVersion: 1, Experiment: "E2", Arm: "reference", Profile: profile,
		Mode: "local", RunID: "cli-test", Seed: 1, Roles: []string{"telemetry"}, SamplesPerMessage: 1,
		Topology: paperload.Topology{Gateways: 1, SensorsPerGateway: 1, Users: 0, Viewers: 0},
		Targets:  []paperload.Target{{GatewayID: "gw-a", SensorID: "s-a"}},
		Traffic:  paperload.Traffic{RatePerSecond: 1}, Phases: paperload.Phases{MeasureSeconds: 1},
		Limits: paperload.Limits{Workers: 1, Connections: 1, PendingMessages: 1, PendingBytes: 1,
			DiskGrowthBytes: 1, WallSeconds: 1},
		DurabilityMode: "synthetic-load", RetryIntervalMS: 1000, PublishTimeoutMS: 1000,
		TLS: paperload.TLS{HTTPEndpoint: "https://localhost", MQTTEndpoint: "mqtts://localhost:8883",
			HTTPCAFile: "ca.pem", MqttCAFile: "mqtt-ca.pem"},
		IdentityFiles:        map[string]string{"telemetry": "identity.json"},
		GatewayIdentityFiles: map[string]string{"gw-a": "private/gw-a.json"}, BundlePath: "bundle"}
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "manifest.json")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestPreflightWritesEstimateOnlyAndOfficialNeedsFlag(t *testing.T) {
	manifest := writeManifest(t, "pilot")
	var stdout, stderr bytes.Buffer
	output := filepath.Join(t.TempDir(), "unused")
	if code := run([]string{"--manifest", manifest, "--output-dir", output, "--preflight"}, &stdout, &stderr); code != 0 {
		t.Fatalf("preflight code=%d stderr=%s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "PREFLIGHT_ONLY_NOT_RUN") || strings.Contains(stdout.String(), "unused") {
		t.Fatalf("unsafe/missing output: %s", stdout.String())
	}
	if _, err := os.Stat(output); !os.IsNotExist(err) {
		t.Fatal("preflight created output directory")
	}
	manifest = writeManifest(t, "official")
	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"--manifest", manifest, "--preflight"}, &stdout, &stderr); code != 2 || !strings.Contains(stderr.String(), "requires_opt_in") {
		t.Fatalf("official guard code=%d stderr=%s", code, stderr.String())
	}
	if code := run([]string{"--manifest", manifest, "--preflight", "--official-opt-in"}, &stdout, &stderr); code != 0 {
		t.Fatalf("official opt-in rejected: %s", stderr.String())
	}
}

func TestRunRequiresOutputAndHelpWorks(t *testing.T) {
	manifest := writeManifest(t, "pilot")
	var stdout, stderr bytes.Buffer
	if code := run([]string{"--manifest", manifest, "--run"}, &stdout, &stderr); code != 2 || !strings.Contains(stderr.String(), "output-dir is required") {
		t.Fatalf("run result code=%d: %s", code, stderr.String())
	}
	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"--help"}, &stdout, &stderr); code != 0 || !strings.Contains(stdout.String(), "--official-opt-in") {
		t.Fatalf("help result code=%d", code)
	}
}

func TestE4PreflightDoesNotOpenJournalAndRunUsesIntegratedRuntime(t *testing.T) {
	manifest := writeManifest(t, "pilot")
	f, err := os.Open(manifest)
	if err != nil {
		t.Fatal(err)
	}
	m, err := paperload.Decode(f)
	_ = f.Close()
	if err != nil {
		t.Fatal(err)
	}
	m.Experiment = "E4"
	m.E4 = paperload.E4Settings{Mode: "replay", JournalPath: filepath.Join(t.TempDir(), "does-not-exist.jsonl"), MaxMessages: 20, MaxBytes: 1 << 20, ReplayCount: 10, IgnoreFirstValidACKs: 1}
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(manifest, data, 0600); err != nil {
		t.Fatal(err)
	}
	var out, stderr bytes.Buffer
	if code := run([]string{"--manifest", manifest, "--preflight"}, &out, &stderr); code != 0 {
		t.Fatalf("preflight code=%d stderr=%s", code, stderr.String())
	}
	if strings.Contains(out.String(), "does-not-exist") {
		t.Fatalf("private path leaked into output: %s", out.String())
	}
	if _, err := os.Stat(m.E4.JournalPath); !os.IsNotExist(err) {
		t.Fatalf("preflight accessed/created journal: %v", err)
	}
	output := filepath.Join(t.TempDir(), "out")
	code := run([]string{"--manifest", manifest, "--output-dir", output, "--run"}, &out, &stderr)
	if code != 1 || strings.Contains(stderr.String(), "E4 runtime is not integrated") {
		t.Fatalf("E4 runtime dispatch code=%d stderr=%s", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "paperload runtime failed; see safe workload artifact") {
		t.Fatalf("E4 runtime was not dispatched: code=%d stderr=%s", code, stderr.String())
	}
}

func TestRunRejectsUnsupportedCombinedRoleBeforeCreatingOutput(t *testing.T) {
	manifest := writeManifest(t, "pilot")
	f, err := os.Open(manifest)
	if err != nil {
		t.Fatal(err)
	}
	m, err := paperload.Decode(f)
	_ = f.Close()
	if err != nil {
		t.Fatal(err)
	}
	m.Experiment = "E3"
	m.Roles = []string{"control"}
	m.DurabilityMode = ""
	m.Topology.Users = 1
	m.Topology.Viewers = 1
	m.IdentityFiles = map[string]string{"control": "private/control.json"}
	m.GatewayIdentityFiles = nil
	m.GatewayIdentityFiles = nil
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(manifest, b, 0600); err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(t.TempDir(), "fresh-output")
	var stdout, stderr bytes.Buffer
	if code := run([]string{"--manifest", manifest, "--output-dir", output, "--run"}, &stdout, &stderr); code != 2 || !strings.Contains(stderr.String(), "manifest_contract_rejected") {
		t.Fatalf("code=%d stderr=%s", code, stderr.String())
	}
	if _, err := os.Stat(output); !os.IsNotExist(err) {
		t.Fatal("unsupported role created output or attempted runtime")
	}
}
