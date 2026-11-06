package paperload

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
)

func validManifest() Manifest {
	return Manifest{
		SchemaVersion: 1, Experiment: "E1", Arm: "reference", Profile: "pilot", Mode: "remote",
		RunID: "pilot-1", Seed: 42, Roles: []string{"viewer"}, SamplesPerMessage: 100,
		Topology: Topology{Gateways: 2, SensorsPerGateway: 2, Users: 2, Viewers: 3},
		Targets: []Target{{GatewayID: "gw-a", SensorID: "s-a"}, {GatewayID: "gw-a", SensorID: "s-aa"},
			{GatewayID: "gw-b", SensorID: "s-b"}, {GatewayID: "gw-b", SensorID: "s-bb"}},
		Subscriptions: []Subscription{
			{ViewerID: "v1", UserID: "u1", GatewayID: "gw-a", SensorID: "s-a"},
			{ViewerID: "v1", UserID: "u1", GatewayID: "gw-a"},
			{ViewerID: "v3", UserID: "u2", GatewayID: "gw-b", SensorID: "s-b"},
			{ViewerID: "v4", UserID: "u2", GatewayID: "gw-b", SensorID: "s-b"},
		},
		Traffic: Traffic{RatePerSecond: 10, RampSeconds: 5, BurstMessages: 20},
		Phases:  Phases{WarmupSeconds: 1, MeasureSeconds: 10, DrainSeconds: 5, CooldownSeconds: 1},
		Limits: Limits{Retries: 2, Workers: 4, Connections: 8, PendingMessages: 100,
			PendingBytes: 1048576, DiskGrowthBytes: 1048576, WallSeconds: 30},
		RetryIntervalMS: 1000, PublishTimeoutMS: 5000,
		TLS: TLS{HTTPEndpoint: "https://localhost:8443", MQTTEndpoint: "mqtts://localhost:8883",
			HTTPCAFile: "bundle/https-ca.pem", MqttCAFile: "bundle/mqtt-ca.pem"},
		IdentityFiles: map[string]string{"viewer": "private/viewer.json"},
		BundlePath:    "bundle",
	}
}

func TestValidateManifestAndDeterministicEstimate(t *testing.T) {
	m := validManifest()
	got, err := Validate(m, false)
	if err != nil {
		t.Fatal(err)
	}
	want := Estimate{ExpectedRecipients: 4, ExpectedSubscriptionMatches: 5,
		UniqueUsers: 2, Connections: 3, SelectedRoleConnections: 3, ProposedMessages: 130, ProposedRawSamples: 13000,
		WindowSeconds: 17, PendingMessageLimit: 100, PendingByteLimit: 1048576,
		DiskGrowthLimit: 1048576, WallLimitSeconds: 30}
	if got != want {
		t.Fatalf("estimate = %#v, want %#v", got, want)
	}
}

func TestDecodeOfficialE2ManifestWithRunWideOfferedRate(t *testing.T) {
	m := validManifest()
	m.Experiment = "E2"
	m.Arm = "A"
	m.Profile = "official"
	m.Roles = []string{"telemetry"}
	m.Topology.Users = 0
	m.Topology.Viewers = 0
	m.Subscriptions = nil
	m.IdentityFiles = map[string]string{"telemetry": "identity.json"}
	m.GatewayIdentityFiles = map[string]string{"gw-a": "a.json", "gw-b": "b.json"}
	m.DurabilityMode = "synthetic-load"
	m.Traffic.RatePerSecond = 50
	m.Traffic.TotalRatePerSecond = 100

	data, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := Decode(strings.NewReader(string(data)))
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if decoded.Traffic.TotalRatePerSecond != 100 {
		t.Fatalf("total offered rate = %v, want 100", decoded.Traffic.TotalRatePerSecond)
	}
	if _, err := Validate(decoded, true); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestViewerManifestRejectsTelemetryOnlyDurabilityMode(t *testing.T) {
	// Fixture-derived from validManifest: E1 is the native viewer role, while
	// synthetic-load is accepted only by the separate E2 telemetry role.
	m := validManifest()
	m.DurabilityMode = "synthetic-load"
	if _, err := Validate(m, false); err == nil || err.Error() != "manifest_contract_rejected" {
		t.Fatalf("viewer durability mode error = %v, want manifest_contract_rejected", err)
	}
}

func TestExpectedRecipientsUseCompositeGatewaySensorIdentity(t *testing.T) {
	m := validManifest()
	m.Topology.Viewers = 2
	m.Targets = []Target{{GatewayID: "gw-a", SensorID: "same"}, {GatewayID: "gw-b", SensorID: "same"}}
	m.Subscriptions = []Subscription{
		{ViewerID: "v-a", UserID: "u-a", GatewayID: "gw-a", SensorID: "same"},
		{ViewerID: "v-b", UserID: "u-b", GatewayID: "gw-b", SensorID: "same"},
	}
	got, err := Validate(m, false)
	if err != nil {
		t.Fatal(err)
	}
	if got.ExpectedRecipients != 2 || got.ExpectedSubscriptionMatches != 2 {
		t.Fatalf("composite estimate = %#v", got)
	}
}

func TestTelemetryOnlyAllowsZeroViewerAndUserTopology(t *testing.T) {
	m := validManifest()
	m.Experiment = "E2"
	m.Roles = []string{"telemetry"}
	m.Topology.Users = 0
	m.Topology.Viewers = 0
	m.Subscriptions = nil
	m.IdentityFiles = map[string]string{"telemetry": "identity.json"}
	m.GatewayIdentityFiles = map[string]string{"gw-a": "private/a.json", "gw-b": "private/b.json"}
	m.DurabilityMode = "synthetic-load"
	got, err := Validate(m, false)
	if err != nil {
		t.Fatal(err)
	}
	if got.Connections != 2 || got.SelectedRoleConnections != 2 || got.UniqueUsers != 0 {
		t.Fatalf("telemetry role estimate: %+v", got)
	}
}

func TestE3BackgroundTelemetryRequiresPublishTimeout(t *testing.T) {
	// E3 reuses the bounded E2 telemetry schema; its manifest builder must carry
	// the E2 publish timeout rather than weakening Validate for offline plans.
	m := validManifest()
	m.Experiment, m.Roles = "E2", []string{"telemetry"}
	m.Topology.Users, m.Topology.Viewers = 0, 0
	m.Subscriptions = nil
	m.IdentityFiles = map[string]string{"telemetry": "identity.json"}
	m.GatewayIdentityFiles = map[string]string{"gw-a": "private/a.json", "gw-b": "private/b.json"}
	m.DurabilityMode = "synthetic-load"
	m.PublishTimeoutMS = 0
	if _, err := Validate(m, false); err == nil || err.Error() != "manifest_contract_rejected" {
		t.Fatalf("missing E2 publish timeout error = %v, want manifest_contract_rejected", err)
	}
	m.PublishTimeoutMS = 3000
	if _, err := Validate(m, false); err != nil {
		t.Fatalf("E3-derived E2 manifest with publish timeout rejected: %v", err)
	}
}

func e4Manifest() Manifest {
	m := validManifest()
	m.Experiment, m.Roles = "E4", []string{"telemetry"}
	m.Topology.Users, m.Topology.Viewers = 0, 0
	m.Subscriptions = nil
	m.IdentityFiles = map[string]string{"telemetry": "identity.json"}
	m.GatewayIdentityFiles = map[string]string{"gw-a": "private/a.json", "gw-b": "private/b.json"}
	m.DurabilityMode = "synthetic-load"
	m.E4 = E4Settings{Mode: "unique", JournalPath: "private/e4.jsonl", MaxMessages: 20, MaxBytes: 1 << 20}
	return m
}

func TestE4ManifestSettingsAreOptionalButRoleBoundAndFinite(t *testing.T) {
	m := e4Manifest()
	if _, err := Validate(m, false); err != nil {
		t.Fatal(err)
	}
	m.E4.IgnoreFirstValidACKs = 1
	if _, err := Validate(m, false); err != nil {
		t.Fatalf("unique mode should permit a bounded ACK-ignore retry budget: %v", err)
	}
	m.E4 = E4Settings{}
	if _, err := Validate(m, false); err != nil {
		t.Fatalf("E4 settings should be optional: %v", err)
	}
	m = e4Manifest()
	m.E4.Mode = "replay"
	m.E4.ReplayCount = 10
	m.E4.IgnoreFirstValidACKs = 1
	if _, err := Validate(m, false); err != nil {
		t.Fatal(err)
	}
	m.E4.ExpiryTTLSeconds = 600
	if _, err := Validate(m, false); err != nil {
		t.Fatalf("production TTL replay manifest rejected: %v", err)
	}
	m.E4.ExpiryTTLSeconds = 599
	if _, err := Validate(m, false); err == nil {
		t.Fatal("reduced production TTL must be rejected")
	}
	for _, mutate := range []func(*Manifest){
		func(m *Manifest) { m.E4.MaxMessages = 10001 },
		func(m *Manifest) { m.E4.MaxBytes = 64<<20 + 1 },
		func(m *Manifest) { m.E4.IgnoreFirstValidACKs = 11 },
		func(m *Manifest) { m.E4.Mode = "replay" },
		func(m *Manifest) { m.DurabilityMode = "durable" },
	} {
		bad := e4Manifest()
		mutate(&bad)
		if _, err := Validate(bad, false); err == nil {
			t.Fatalf("accepted invalid E4 config: %+v", bad.E4)
		}
	}
	for _, experiment := range []string{"E1", "E2", "E3"} {
		other := e4Manifest()
		other.Experiment = experiment
		switch experiment {
		case "E1":
			other.Roles = []string{"viewer"}
			other.IdentityFiles = map[string]string{"viewer": "private/viewer.json"}
			other.GatewayIdentityFiles = nil
		case "E3":
			other.Roles = []string{"control"}
			other.IdentityFiles = map[string]string{"control": "private/control.json"}
			other.GatewayIdentityFiles = nil
		default:
			other.E4 = E4Settings{Mode: "unique", JournalPath: "private/journal", MaxMessages: 1, MaxBytes: 1}
		}
		if _, err := Validate(other, false); err == nil {
			t.Fatalf("%s accepted E4 settings", experiment)
		}
	}
}

func TestValidateRejectsMissingInvalidNonfiniteAndOverboundFields(t *testing.T) {
	cases := map[string]func(*Manifest){
		"missing identity file": func(m *Manifest) { m.IdentityFiles = nil },
		"missing gateway identity file": func(m *Manifest) {
			m.Roles = []string{"telemetry"}
			m.Experiment = "E2"
			m.Subscriptions = nil
			m.Topology.Users = 0
			m.Topology.Viewers = 0
			m.IdentityFiles = map[string]string{"telemetry": "identity.json"}
			m.GatewayIdentityFiles = nil
		},
		"durable not implemented": func(m *Manifest) {
			m.Experiment = "E2"
			m.Roles = []string{"telemetry"}
			m.Topology.Users = 0
			m.Topology.Viewers = 0
			m.Subscriptions = nil
			m.IdentityFiles = map[string]string{"telemetry": "identity.json"}
			m.GatewayIdentityFiles = map[string]string{"gw-a": "a.json", "gw-b": "b.json"}
			m.DurabilityMode = "durable"
		},
		"bad experiment":    func(m *Manifest) { m.Experiment = "E6" },
		"bad samples":       func(m *Manifest) { m.SamplesPerMessage = 0 },
		"nonfinite rate":    func(m *Manifest) { m.Traffic.RatePerSecond = math.Inf(1) },
		"rate overbound":    func(m *Manifest) { m.Traffic.RatePerSecond = 20001 },
		"viewers overbound": func(m *Manifest) { m.Topology.Viewers = 10001 },
		"workers overbound": func(m *Manifest) { m.Limits.Workers = 10001 },
		"zero duration":     func(m *Manifest) { m.Phases.MeasureSeconds = 0 },
		"missing topology":  func(m *Manifest) { m.Topology.Gateways = 0 },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			m := validManifest()
			mutate(&m)
			if _, err := Validate(m, false); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
	var m Manifest
	if err := json.Unmarshal([]byte(`{"schema_version":1e999}`), &m); err == nil {
		t.Fatal("expected JSON number overflow")
	}
}

func TestValidateRejectsInvalidCombinationsAndCompositeIdentityDuplicates(t *testing.T) {
	cases := []func(*Manifest){
		func(m *Manifest) { m.Experiment = "E2"; m.Roles = []string{"viewer"} },
		func(m *Manifest) { m.Experiment = "E1"; m.Roles = []string{"telemetry"} },
		func(m *Manifest) { m.Topology.Viewers = 2 },
		func(m *Manifest) { m.Subscriptions[1].SensorID = "s-a" },
		func(m *Manifest) { m.Subscriptions[2].ViewerID = "v1" },
	}
	for i, mutate := range cases {
		m := validManifest()
		mutate(&m)
		if _, err := Validate(m, false); err == nil {
			t.Errorf("case %d accepted", i)
		}
	}
}

func TestValidateUsesProvisioningIdentifierContractForTargets(t *testing.T) {
	for _, target := range []Target{
		{GatewayID: "gateway.with.dot", SensorID: "sensor"},
		{GatewayID: "backend_service", SensorID: "sensor"},
		{GatewayID: strings.Repeat("g", 65), SensorID: "sensor"},
		{GatewayID: "gateway", SensorID: "sensor.with.dot"},
		{GatewayID: "gateway", SensorID: strings.Repeat("s", 65)},
	} {
		m := validManifest()
		m.Targets = []Target{target}
		if _, err := Validate(m, false); err == nil {
			t.Errorf("accepted invalid provisioned target: %+v", target)
		}
	}
}

func TestValidateOfficialRequiresExplicitOptInAndCapsViewers(t *testing.T) {
	m := validManifest()
	m.Profile = "official"
	m.Topology.Viewers = 3
	if _, err := Validate(m, false); err == nil {
		t.Fatal("official accepted without flag")
	}
	if _, err := Validate(m, true); err != nil {
		t.Fatal(err)
	}
	m.Topology.Viewers = 10001
	if _, err := Validate(m, true); err == nil {
		t.Fatal("viewer cap accepted")
	}
}

func TestValidateRejectsCredentialBearingEndpoints(t *testing.T) {
	for _, endpoint := range []string{"https://user:password@localhost", "https://localhost/path?token=abc", "mqtts://localhost:5432", "http://localhost"} {
		m := validManifest()
		m.TLS.HTTPEndpoint = endpoint
		if _, err := Validate(m, false); err == nil {
			t.Errorf("accepted endpoint %q", endpoint)
		}
	}
}

func TestValidateAllowsBoundedMQTTSHostTunnelPort(t *testing.T) {
	m := validManifest()
	m.TLS.MQTTEndpoint = "mqtts://localhost:18884"
	if _, err := Validate(m, false); err != nil {
		t.Fatal(err)
	}
	m.TLS.MQTTEndpoint = "mqtts://localhost:5432"
	if _, err := Validate(m, false); err == nil {
		t.Fatal("database port accepted for MQTT TLS")
	}
}

func TestDecodeRejectsUnknownFieldsAndCredentialText(t *testing.T) {
	if _, err := Decode(strings.NewReader(`{"secret":"dont-print"}`)); err == nil || strings.Contains(err.Error(), "dont-print") {
		t.Fatalf("unsafe decode error: %v", err)
	}
	if _, err := Decode(strings.NewReader(strings.Repeat(" ", maxManifestBytes+1))); err == nil {
		t.Fatal("oversized manifest accepted")
	}
}

func TestDecodeAcceptsBoundedManifestAboveLegacyOneMiB(t *testing.T) {
	raw, err := json.Marshal(validManifest())
	if err != nil {
		t.Fatal(err)
	}
	padded := append(raw, []byte(strings.Repeat(" ", (1<<20)+1-len(raw)))...)
	if len(padded) <= 1<<20 || len(padded) > maxManifestBytes {
		t.Fatalf("test payload size outside intended bounds: %d", len(padded))
	}
	if _, err := Decode(strings.NewReader(string(padded))); err != nil {
		t.Fatalf("bounded manifest above legacy limit rejected: %v", err)
	}
}
