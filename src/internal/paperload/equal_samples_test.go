package paperload

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func goldenEqualSamplesSettingsJSON() string {
	return `{
  "schema_version": 1,
  "workload_type": "equal_samples",
  "seed": 84,
  "total_samples": 6000,
  "sample_rate": 100,
  "duration_seconds": 60,
  "logical_producers": [
    "A",
    "B"
  ],
  "samples_per_producer": 3000,
  "producer_sample_rate": 50,
  "sample_interval_us": 20000,
  "stream_sequence_start": 0,
  "timestamp_anchor": "2026-10-31T11:58:00Z",
  "max_context_age_seconds": 86400,
  "context_sha256": "c27dc881d673d16486013066ee8a505d8d440a0317ef4194b6c8bd80cae36792",
  "expected_samples": 3000,
  "expected_messages": 30,
  "sample_stream_version": 1
}`
}

func goldenManifestJSONS100ArmA() string {
	return `{
  "mode": "remote",
  "run_id": "golden-run-100",
  "bundle_path": "bundle",
  "tls": {
    "https_endpoint": "https://localhost:8443",
    "mqtt_tls_endpoint": "mqtts://localhost:8883",
    "https_ca_file": "bundle/https-ca.pem",
    "mqtt_tls_ca_file": "bundle/mqtt-ca.pem"
  },
  "schema_version": 1,
  "experiment": "E2",
  "arm": "A",
  "profile": "pilot",
  "roles": [
    "telemetry"
  ],
  "seed": 84,
  "samples_per_message": 100,
  "targets": [
    {
      "gateway_id": "gw-a",
      "sensor_id": "s-a"
    }
  ],
  "subscriptions": [],
  "topology": {
    "gateways": 1,
    "sensors_per_gateway": 1,
    "users": 0,
    "viewers": 0
  },
  "traffic": {
    "rate_per_second": 0.5,
    "ramp_seconds": 0,
    "burst_messages": 0
  },
  "phases": {
    "warmup_seconds": 0,
    "measure_seconds": 60,
    "drain_seconds": 20,
    "cooldown_seconds": 0
  },
  "limits": {
    "retries": 16,
    "workers": 2,
    "connections": 1,
    "pending_messages": 64,
    "pending_bytes": 1048576,
    "disk_growth_bytes": 16777216,
    "wall_seconds": 90
  },
  "gateway_identity_files": {
    "gw-a": "priv/a.json"
  },
  "identity_files": {
    "telemetry": "priv/ta.json"
  },
  "durability_mode": "synthetic-load",
  "retry_interval_ms": 500,
  "publish_timeout_ms": 5000,
  "equal_samples": {
    "schema_version": 1,
    "workload_type": "equal_samples",
    "seed": 84,
    "total_samples": 6000,
    "sample_rate": 100,
    "duration_seconds": 60,
    "logical_producers": [
      "A",
      "B"
    ],
    "samples_per_producer": 3000,
    "producer_sample_rate": 50,
    "sample_interval_us": 20000,
    "stream_sequence_start": 0,
    "timestamp_anchor": "2026-10-31T11:58:00Z",
    "max_context_age_seconds": 86400,
    "context_sha256": "c27dc881d673d16486013066ee8a505d8d440a0317ef4194b6c8bd80cae36792",
    "expected_samples": 3000,
    "expected_messages": 30,
    "sample_stream_version": 1
  }
}`
}

func goldenManifestJSONS1ArmB() string {
	return `{
  "mode": "remote",
  "run_id": "golden-run-1",
  "bundle_path": "bundle",
  "tls": {
    "https_endpoint": "https://localhost:8443",
    "mqtt_tls_endpoint": "mqtts://localhost:8883",
    "https_ca_file": "bundle/https-ca.pem",
    "mqtt_tls_ca_file": "bundle/mqtt-ca.pem"
  },
  "schema_version": 1,
  "experiment": "E2",
  "arm": "B",
  "profile": "pilot",
  "roles": [
    "telemetry"
  ],
  "seed": 84,
  "samples_per_message": 1,
  "targets": [
    {
      "gateway_id": "gw-b",
      "sensor_id": "s-b"
    }
  ],
  "subscriptions": [],
  "topology": {
    "gateways": 1,
    "sensors_per_gateway": 1,
    "users": 0,
    "viewers": 0
  },
  "traffic": {
    "rate_per_second": 50,
    "ramp_seconds": 0,
    "burst_messages": 0
  },
  "phases": {
    "warmup_seconds": 0,
    "measure_seconds": 60,
    "drain_seconds": 20,
    "cooldown_seconds": 0
  },
  "limits": {
    "retries": 16,
    "workers": 2,
    "connections": 1,
    "pending_messages": 64,
    "pending_bytes": 1048576,
    "disk_growth_bytes": 16777216,
    "wall_seconds": 90
  },
  "gateway_identity_files": {
    "gw-b": "priv/b.json"
  },
  "identity_files": {
    "telemetry": "priv/tb.json"
  },
  "durability_mode": "synthetic-load",
  "retry_interval_ms": 500,
  "publish_timeout_ms": 5000,
  "equal_samples": {
    "schema_version": 1,
    "workload_type": "equal_samples",
    "seed": 84,
    "total_samples": 6000,
    "sample_rate": 100,
    "duration_seconds": 60,
    "logical_producers": [
      "A",
      "B"
    ],
    "samples_per_producer": 3000,
    "producer_sample_rate": 50,
    "sample_interval_us": 20000,
    "stream_sequence_start": 0,
    "timestamp_anchor": "2026-10-31T11:58:00Z",
    "max_context_age_seconds": 86400,
    "context_sha256": "c27dc881d673d16486013066ee8a505d8d440a0317ef4194b6c8bd80cae36792",
    "expected_samples": 3000,
    "expected_messages": 3000,
    "sample_stream_version": 1
  }
}`
}

func fixedGoldenNow() time.Time {
	return time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
}

func TestEqualSamplesGoldenContextAndManifestCrossLanguage(t *testing.T) {
	now := fixedGoldenNow()

	// 1. Verify cross-language canonical context SHA256 matches Python builder exactly.
	var eq EqualSamplesSettings
	if err := json.Unmarshal([]byte(goldenEqualSamplesSettingsJSON()), &eq); err != nil {
		t.Fatalf("failed to unmarshal golden context: %v", err)
	}

	computedHash, err := CalculateEqualSamplesContextSHA256(eq)
	if err != nil {
		t.Fatalf("CalculateEqualSamplesContextSHA256 failed: %v", err)
	}
	const wantHash = "c27dc881d673d16486013066ee8a505d8d440a0317ef4194b6c8bd80cae36792"
	if computedHash != wantHash {
		t.Fatalf("computedHash = %s, want %s", computedHash, wantHash)
	}
	if eq.ContextSHA256 != wantHash {
		t.Fatalf("eq.ContextSHA256 = %s, want %s", eq.ContextSHA256, wantHash)
	}

	// 2. Microsecond precision golden test.
	eqMicro := eq
	eqMicro.TimestampAnchor = "2026-10-31T11:58:00.654321Z"
	const wantMicroHash = "520a9069bad2bdc7a6d8a34e8b0f9f9fb900f7ea41426c1458cf13bc113dac08"
	computedMicroHash, err := CalculateEqualSamplesContextSHA256(eqMicro)
	if err != nil {
		t.Fatalf("CalculateEqualSamplesContextSHA256 with micro failed: %v", err)
	}
	if computedMicroHash != wantMicroHash {
		t.Fatalf("computedMicroHash = %s, want %s", computedMicroHash, wantMicroHash)
	}

	// 3. Decode and validate full golden manifest S=100 Arm A.
	m100, err := Decode(strings.NewReader(goldenManifestJSONS100ArmA()))
	if err != nil {
		t.Fatalf("Decode golden S=100 manifest failed: %v", err)
	}
	if m100.EqualSamples == nil {
		t.Fatal("expected EqualSamples to be non-nil")
	}

	est100, err := ValidateAt(m100, false, now)
	if err != nil {
		t.Fatalf("ValidateAt golden S=100 manifest failed: %v", err)
	}
	wantEst100 := Estimate{
		ExpectedRecipients:          0,
		ExpectedSubscriptionMatches: 0,
		UniqueUsers:                 0,
		Connections:                 1,
		SelectedRoleConnections:     1,
		ProposedMessages:            30,
		ProposedRawSamples:          3000,
		WindowSeconds:               80,
		PendingMessageLimit:         64,
		PendingByteLimit:            1048576,
		DiskGrowthLimit:             16777216,
		WallLimitSeconds:            90,
	}
	if est100 != wantEst100 {
		t.Fatalf("estimate S=100 = %+v, want %+v", est100, wantEst100)
	}

	// 4. Decode and validate full golden manifest S=1 Arm B.
	m1, err := Decode(strings.NewReader(goldenManifestJSONS1ArmB()))
	if err != nil {
		t.Fatalf("Decode golden S=1 manifest failed: %v", err)
	}
	if m1.EqualSamples == nil {
		t.Fatal("expected EqualSamples to be non-nil")
	}

	est1, err := ValidateAt(m1, false, now)
	if err != nil {
		t.Fatalf("ValidateAt golden S=1 manifest failed: %v", err)
	}
	wantEst1 := Estimate{
		ExpectedRecipients:          0,
		ExpectedSubscriptionMatches: 0,
		UniqueUsers:                 0,
		Connections:                 1,
		SelectedRoleConnections:     1,
		ProposedMessages:            3000,
		ProposedRawSamples:          3000,
		WindowSeconds:               80,
		PendingMessageLimit:         64,
		PendingByteLimit:            1048576,
		DiskGrowthLimit:             16777216,
		WallLimitSeconds:            90,
	}
	if est1 != wantEst1 {
		t.Fatalf("estimate S=1 = %+v, want %+v", est1, wantEst1)
	}
}

func TestEqualSamplesStrictDecodingNegatives(t *testing.T) {
	// A. Null equal_samples must be rejected as invalid JSON, not accepted as hidden opt-in.
	nullEQManifest := strings.Replace(goldenManifestJSONS100ArmA(), `"equal_samples": {`, `"equal_samples": null, "_dummy": {`, 1)
	if _, err := Decode(strings.NewReader(nullEQManifest)); err == nil {
		t.Fatal("expected Decode to reject null equal_samples")
	}

	// B. Unknown nested keys inside equal_samples must be rejected.
	unknownKeyManifest := strings.Replace(goldenManifestJSONS100ArmA(), `"schema_version": 1,`, `"schema_version": 1, "unknown_field": 42,`, 1)
	if _, err := Decode(strings.NewReader(unknownKeyManifest)); err == nil {
		t.Fatal("expected Decode to reject unknown nested key inside equal_samples")
	}

	// C. Omitted required zero-valued key (stream_sequence_start: 0) must be rejected.
	omittedZeroKeyManifest := strings.Replace(goldenManifestJSONS100ArmA(), `"stream_sequence_start": 0,`, ``, 1)
	if _, err := Decode(strings.NewReader(omittedZeroKeyManifest)); err == nil {
		t.Fatal("expected Decode to reject omitted required zero-valued stream_sequence_start")
	}

	// D. Omitted required non-zero key (schema_version inside equal_samples).
	omittedSchemaKeyManifest := strings.Replace(goldenManifestJSONS100ArmA(), "\"equal_samples\": {\n    \"schema_version\": 1,\n", "\"equal_samples\": {\n", 1)
	if _, err := Decode(strings.NewReader(omittedSchemaKeyManifest)); err == nil {
		t.Fatal("expected Decode to reject omitted schema_version inside equal_samples")
	}

	// E. Boolean for integer fields must be rejected.
	boolFields := []string{"stream_sequence_start", "seed", "total_samples", "schema_version"}
	for _, field := range boolFields {
		t.Run("bool_for_"+field, func(t *testing.T) {
			badManifest := strings.Replace(goldenManifestJSONS100ArmA(), `"`+field+`": 0,`, `"`+field+`": false,`, 1)
			badManifest = strings.Replace(badManifest, `"`+field+`": 1,`, `"`+field+`": true,`, 1)
			badManifest = strings.Replace(badManifest, `"`+field+`": 84,`, `"`+field+`": true,`, 1)
			badManifest = strings.Replace(badManifest, `"`+field+`": 6000,`, `"`+field+`": true,`, 1)
			if _, err := Decode(strings.NewReader(badManifest)); err == nil {
				t.Fatalf("expected Decode to reject boolean for %s", field)
			}
		})
	}

	// F. Float for integer fields must be rejected.
	floatManifest := strings.Replace(goldenManifestJSONS100ArmA(), `"stream_sequence_start": 0,`, `"stream_sequence_start": 0.0,`, 1)
	if _, err := Decode(strings.NewReader(floatManifest)); err == nil {
		t.Fatal("expected Decode to reject float 0.0 for integer stream_sequence_start")
	}

	// G. Null for integer field inside equal_samples must be rejected.
	nullFieldManifest := strings.Replace(goldenManifestJSONS100ArmA(), `"stream_sequence_start": 0,`, `"stream_sequence_start": null,`, 1)
	if _, err := Decode(strings.NewReader(nullFieldManifest)); err == nil {
		t.Fatal("expected Decode to reject null for field inside equal_samples")
	}

	// H. String for integer field inside equal_samples must be rejected.
	strFieldManifest := strings.Replace(goldenManifestJSONS100ArmA(), `"total_samples": 6000,`, `"total_samples": "6000",`, 1)
	if _, err := Decode(strings.NewReader(strFieldManifest)); err == nil {
		t.Fatal("expected Decode to reject string for total_samples")
	}

	// I. Duplicate key inside equal_samples must be rejected.
	dupKeyManifest := strings.Replace(goldenManifestJSONS100ArmA(), "\"equal_samples\": {\n    \"schema_version\": 1,\n", "\"equal_samples\": {\n    \"schema_version\": 1,\n    \"schema_version\": 1,\n", 1)
	if _, err := Decode(strings.NewReader(dupKeyManifest)); err == nil {
		t.Fatal("expected Decode to reject duplicate key inside equal_samples")
	}
}

func TestEqualSamplesValidationNegatives(t *testing.T) {
	now := fixedGoldenNow()

	// Helper to get valid decoded manifest
	getValid := func() Manifest {
		m, err := Decode(strings.NewReader(goldenManifestJSONS100ArmA()))
		if err != nil {
			t.Fatalf("Decode failed: %v", err)
		}
		return m
	}

	// 1. Corrupted context SHA256
	t.Run("corrupted_context_sha256", func(t *testing.T) {
		m := getValid()
		m.EqualSamples.ContextSHA256 = "c27dc881d673d16486013066ee8a505d8d440a0317ef4194b6c8bd80cae36793"
		if _, err := ValidateAt(m, false, now); err == nil || err.Error() != "manifest_contract_rejected" {
			t.Fatalf("got %v, want manifest_contract_rejected", err)
		}
		if err := ValidateEqualSamples(m, now, true); err == nil || err.Error() != "equal_samples_context_hash_mismatch" {
			t.Fatalf("ValidateEqualSamples error = %v, want equal_samples_context_hash_mismatch", err)
		}
	})

	// 2. Seed mismatch between manifest.Seed and equal_samples.Seed
	t.Run("seed_mismatch", func(t *testing.T) {
		m := getValid()
		m.Seed = 85
		if _, err := ValidateAt(m, false, now); err == nil || err.Error() != "manifest_contract_rejected" {
			t.Fatalf("got %v, want manifest_contract_rejected", err)
		}
	})

	// 3. Negative seed
	t.Run("negative_seed", func(t *testing.T) {
		m := getValid()
		m.EqualSamples.Seed = -1
		if _, err := ValidateAt(m, false, now); err == nil || err.Error() != "manifest_contract_rejected" {
			t.Fatalf("got %v, want manifest_contract_rejected", err)
		}
	})

	// 4. Non-E2 experiment with equal_samples
	for _, exp := range []string{"E1", "E3", "E4", "E5"} {
		t.Run("reject_experiment_"+exp, func(t *testing.T) {
			m := getValid()
			m.Experiment = exp
			if exp == "E1" {
				m.Roles = []string{"viewer"}
				m.Subscriptions = []Subscription{{ViewerID: "v1", UserID: "u1", GatewayID: "gw-a", SensorID: "s-a"}}
				m.Topology.Viewers = 1
				m.Topology.Users = 1
				m.IdentityFiles = map[string]string{"viewer": "priv/v.json"}
				m.GatewayIdentityFiles = nil
			}
			if _, err := ValidateAt(m, false, now); err == nil || err.Error() != "manifest_contract_rejected" {
				t.Fatalf("got %v, want manifest_contract_rejected for experiment %s", err, exp)
			}
		})
	}

	// 5. Arm other than A or B
	t.Run("invalid_arm", func(t *testing.T) {
		m := getValid()
		m.Arm = "reference"
		if _, err := ValidateAt(m, false, now); err == nil || err.Error() != "manifest_contract_rejected" {
			t.Fatalf("got %v, want manifest_contract_rejected", err)
		}
	})

	// 6. Profile official rejected for equal_samples (only pilot supported in Local MVP)
	t.Run("profile_official_rejected", func(t *testing.T) {
		m := getValid()
		m.Profile = "official"
		if _, err := ValidateAt(m, true, now); err == nil || err.Error() != "manifest_contract_rejected" {
			t.Fatalf("got %v, want manifest_contract_rejected", err)
		}
	})

	// 7. SamplesPerMessage not 1 or 100
	t.Run("invalid_samples_per_message", func(t *testing.T) {
		m := getValid()
		m.SamplesPerMessage = 50
		if _, err := ValidateAt(m, false, now); err == nil || err.Error() != "manifest_contract_rejected" {
			t.Fatalf("got %v, want manifest_contract_rejected", err)
		}
	})

	// 8. Non-zero burst/ramp/warmup/cooldown
	t.Run("burst_non_zero", func(t *testing.T) {
		m := getValid()
		m.Traffic.BurstMessages = 5
		if _, err := ValidateAt(m, false, now); err == nil || err.Error() != "manifest_contract_rejected" {
			t.Fatalf("got %v, want manifest_contract_rejected", err)
		}
	})
	t.Run("ramp_non_zero", func(t *testing.T) {
		m := getValid()
		m.Traffic.RampSeconds = 2
		if _, err := ValidateAt(m, false, now); err == nil || err.Error() != "manifest_contract_rejected" {
			t.Fatalf("got %v, want manifest_contract_rejected", err)
		}
	})
	t.Run("warmup_non_zero", func(t *testing.T) {
		m := getValid()
		m.Phases.WarmupSeconds = 5
		if _, err := ValidateAt(m, false, now); err == nil || err.Error() != "manifest_contract_rejected" {
			t.Fatalf("got %v, want manifest_contract_rejected", err)
		}
	})
	t.Run("cooldown_non_zero", func(t *testing.T) {
		m := getValid()
		m.Phases.CooldownSeconds = 5
		if _, err := ValidateAt(m, false, now); err == nil || err.Error() != "manifest_contract_rejected" {
			t.Fatalf("got %v, want manifest_contract_rejected", err)
		}
	})

	// 9. Drain not 20
	t.Run("drain_not_20", func(t *testing.T) {
		m := getValid()
		m.Phases.DrainSeconds = 15
		if _, err := ValidateAt(m, false, now); err == nil || err.Error() != "manifest_contract_rejected" {
			t.Fatalf("got %v, want manifest_contract_rejected", err)
		}
	})

	// 10. Wall seconds out of bounds [duration+20, duration+20+60]
	t.Run("wall_seconds_too_short", func(t *testing.T) {
		m := getValid()
		m.Limits.WallSeconds = 79 // min is 60 + 20 = 80
		if _, err := ValidateAt(m, false, now); err == nil || err.Error() != "manifest_contract_rejected" {
			t.Fatalf("got %v, want manifest_contract_rejected", err)
		}
	})
	t.Run("wall_seconds_too_long", func(t *testing.T) {
		m := getValid()
		m.Limits.WallSeconds = 141 // max is 60 + 20 + 60 = 140
		if _, err := ValidateAt(m, false, now); err == nil || err.Error() != "manifest_contract_rejected" {
			t.Fatalf("got %v, want manifest_contract_rejected", err)
		}
	})

	// 11. Count and rate mismatches
	t.Run("expected_messages_mismatch", func(t *testing.T) {
		m := getValid()
		m.EqualSamples.ExpectedMessages = 31
		if _, err := ValidateAt(m, false, now); err == nil || err.Error() != "manifest_contract_rejected" {
			t.Fatalf("got %v, want manifest_contract_rejected", err)
		}
	})
	t.Run("rate_per_second_mismatch", func(t *testing.T) {
		m := getValid()
		m.Traffic.RatePerSecond = 1.0 // should be 0.5 for S=100
		if _, err := ValidateAt(m, false, now); err == nil || err.Error() != "manifest_contract_rejected" {
			t.Fatalf("got %v, want manifest_contract_rejected", err)
		}
	})

	// 12. Multiple targets rejected
	t.Run("multiple_targets_rejected", func(t *testing.T) {
		m := getValid()
		m.Targets = append(m.Targets, Target{GatewayID: "gw-b", SensorID: "s-b"})
		m.Topology.Gateways = 2
		m.GatewayIdentityFiles["gw-b"] = "priv/b.json"
		if _, err := ValidateAt(m, false, now); err == nil || err.Error() != "manifest_contract_rejected" {
			t.Fatalf("got %v, want manifest_contract_rejected", err)
		}
	})
}

func TestEqualSamplesAnchorAndFreshnessNegatives(t *testing.T) {
	now := fixedGoldenNow()

	getValid := func() Manifest {
		m, err := Decode(strings.NewReader(goldenManifestJSONS100ArmA()))
		if err != nil {
			t.Fatalf("Decode failed: %v", err)
		}
		return m
	}

	// 1. Future anchor
	t.Run("future_anchor", func(t *testing.T) {
		m := getValid()
		m.EqualSamples.TimestampAnchor = "2026-10-31T12:05:00Z"
		hash, _ := CalculateEqualSamplesContextSHA256(*m.EqualSamples)
		m.EqualSamples.ContextSHA256 = hash
		if _, err := ValidateAt(m, false, now); err == nil || err.Error() != "manifest_contract_rejected" {
			t.Fatalf("got %v, want manifest_contract_rejected", err)
		}
		if err := ValidateEqualSamples(m, now, true); err == nil || err.Error() != "equal_samples_stream_not_in_past" {
			t.Fatalf("ValidateEqualSamples error = %v, want equal_samples_stream_not_in_past", err)
		}
	})

	// 2. Future stream end (anchor is in past, but stream end exceeds now)
	t.Run("future_stream_end", func(t *testing.T) {
		m := getValid()
		// samples_per_producer = 3000, sample_interval_us = 20000 -> stream span = 2999 * 20ms = 59.98s
		// If anchor is 11:59:30, stream end is 12:00:29.98 > 12:00:00!
		m.EqualSamples.TimestampAnchor = "2026-10-31T11:59:30Z"
		hash, _ := CalculateEqualSamplesContextSHA256(*m.EqualSamples)
		m.EqualSamples.ContextSHA256 = hash
		if _, err := ValidateAt(m, false, now); err == nil || err.Error() != "manifest_contract_rejected" {
			t.Fatalf("got %v, want manifest_contract_rejected", err)
		}
		if err := ValidateEqualSamples(m, now, true); err == nil || err.Error() != "equal_samples_stream_not_in_past" {
			t.Fatalf("ValidateEqualSamples error = %v, want equal_samples_stream_not_in_past", err)
		}
	})

	// 3. Expired context (anchor is older than max_context_age_seconds)
	t.Run("expired_context", func(t *testing.T) {
		m := getValid()
		m.EqualSamples.TimestampAnchor = "2026-10-29T10:00:00Z" // ~48 hours before now, max_age = 86400
		hash, _ := CalculateEqualSamplesContextSHA256(*m.EqualSamples)
		m.EqualSamples.ContextSHA256 = hash
		if _, err := ValidateAt(m, false, now); err == nil || err.Error() != "manifest_contract_rejected" {
			t.Fatalf("got %v, want manifest_contract_rejected", err)
		}
		if err := ValidateEqualSamples(m, now, true); err == nil || err.Error() != "equal_samples_context_expired" {
			t.Fatalf("ValidateEqualSamples error = %v, want equal_samples_context_expired", err)
		}
	})

	// 4. ParseRFC3339Anchor syntax tests matching Python
	anchorSyntaxErrors := []struct {
		name   string
		anchor string
	}{
		{"missing_Z", "2026-10-31T11:58:00"},
		{"lowercase_z", "2026-10-31T11:58:00z"},
		{"non_utc_offset", "2026-10-31T11:58:00+07:00"},
		{"year_0000", "0000-10-31T11:58:00Z"},
		{"year_10000", "10000-01-01T00:00:00Z"},
		{"empty_fraction", "2026-10-31T11:58:00.Z"},
		{"fraction_7_digits", "2026-10-31T11:58:00.1234567Z"},
		{"hour_24", "2026-10-31T24:00:00Z"},
		{"minute_60", "2026-10-31T11:60:00Z"},
		{"second_60", "2026-10-31T11:58:60Z"},
		{"invalid_calendar_date", "2026-02-30T11:58:00Z"},
	}
	for _, tc := range anchorSyntaxErrors {
		t.Run("anchor_syntax_"+tc.name, func(t *testing.T) {
			if _, err := ParseRFC3339Anchor(tc.anchor); err == nil {
				t.Fatalf("ParseRFC3339Anchor(%q) should have failed", tc.anchor)
			}
		})
	}
}

func TestEqualSamplesExpectedCountsHistoricalReplaySafe(t *testing.T) {
	// Historical context from 2024: older than max_context_age_seconds
	var eq EqualSamplesSettings
	if err := json.Unmarshal([]byte(goldenEqualSamplesSettingsJSON()), &eq); err != nil {
		t.Fatal(err)
	}
	eq.TimestampAnchor = "2024-01-01T00:00:00Z"
	hash, _ := CalculateEqualSamplesContextSHA256(eq)
	eq.ContextSHA256 = hash

	// Pure expected-count calculation must SUCCEED without checking freshness
	counts100, err := CalculateEqualSamplesExpectedCounts(eq, 100)
	if err != nil {
		t.Fatalf("CalculateEqualSamplesExpectedCounts(S=100) failed on historical context: %v", err)
	}
	if counts100.TotalSamples != 6000 || counts100.TotalMessages != 60 {
		t.Fatalf("counts100 = %+v", counts100)
	}
	if counts100.PerRole["A"].Messages != 30 || counts100.PerRole["B"].Messages != 30 {
		t.Fatalf("per_role counts100 = %+v", counts100.PerRole)
	}

	counts1, err := CalculateEqualSamplesExpectedCounts(eq, 1)
	if err != nil {
		t.Fatalf("CalculateEqualSamplesExpectedCounts(S=1) failed on historical context: %v", err)
	}
	if counts1.TotalSamples != 6000 || counts1.TotalMessages != 6000 {
		t.Fatalf("counts1 = %+v", counts1)
	}
	if counts1.PerRole["A"].Messages != 3000 || counts1.PerRole["B"].Messages != 3000 {
		t.Fatalf("per_role counts1 = %+v", counts1.PerRole)
	}

	// Malformed context (e.g. tampered hash) must still be rejected
	badEq := eq
	badEq.ContextSHA256 = "corrupted"
	if _, err := CalculateEqualSamplesExpectedCounts(badEq, 1); err == nil {
		t.Fatal("expected CalculateEqualSamplesExpectedCounts to reject tampered hash")
	}
}

func TestEqualSamplesDeterministicScheduleBoundaries(t *testing.T) {
	var m100 Manifest
	if err := json.Unmarshal([]byte(goldenManifestJSONS100ArmA()), &m100); err != nil {
		t.Fatalf("unmarshal golden S100: %v", err)
	}
	eq100 := *m100.EqualSamples

	var m1 Manifest
	if err := json.Unmarshal([]byte(goldenManifestJSONS1ArmB()), &m1); err != nil {
		t.Fatalf("unmarshal golden S1: %v", err)
	}
	eq1 := *m1.EqualSamples

	// 1. S=100 schedule: 30 messages, duration 60s, producer sample rate 50 Hz.
	// Due offsets: index 0 -> 2s, index 1 -> 4s, ..., index 29 -> 60s.
	for i := 0; i < eq100.ExpectedMessages; i++ {
		due, err := eq100.MessageDueOffset(i, 100)
		if err != nil {
			t.Fatalf("S100 message %d due offset error: %v", i, err)
		}
		expectedDue := time.Duration(i+1) * 2 * time.Second
		if due != expectedDue {
			t.Fatalf("S100 message %d due=%v want %v", i, due, expectedDue)
		}
	}
	// Last due at exactly duration
	lastDue100, err := eq100.MessageDueOffset(eq100.ExpectedMessages-1, 100)
	if err != nil || lastDue100 != time.Duration(eq100.DurationSeconds)*time.Second {
		t.Fatalf("S100 last message due=%v want %v (err=%v)", lastDue100, time.Duration(eq100.DurationSeconds)*time.Second, err)
	}
	// No N+1: index 30 must be rejected
	if _, err := eq100.MessageDueOffset(eq100.ExpectedMessages, 100); err == nil {
		t.Fatal("S100 message index 30 (N+1) must return error")
	}

	// 2. S=1 schedule: 3000 messages, duration 60s, producer sample rate 50 Hz.
	// Due offsets: index 0 -> 20ms, index 1 -> 40ms, ..., index 2999 -> 60s.
	for i := 0; i < eq1.ExpectedMessages; i++ {
		due, err := eq1.MessageDueOffset(i, 1)
		if err != nil {
			t.Fatalf("S1 message %d due offset error: %v", i, err)
		}
		expectedDue := time.Duration(i+1) * 20 * time.Millisecond
		if due != expectedDue {
			t.Fatalf("S1 message %d due=%v want %v", i, due, expectedDue)
		}
	}
	// Last due at exactly duration
	lastDue1, err := eq1.MessageDueOffset(eq1.ExpectedMessages-1, 1)
	if err != nil || lastDue1 != time.Duration(eq1.DurationSeconds)*time.Second {
		t.Fatalf("S1 last message due=%v want %v (err=%v)", lastDue1, time.Duration(eq1.DurationSeconds)*time.Second, err)
	}
	// No N+1: index 3000 must be rejected
	if _, err := eq1.MessageDueOffset(eq1.ExpectedMessages, 1); err == nil {
		t.Fatal("S1 message index 3000 (N+1) must return error")
	}

	// Direct EqualSamplesDueOffset helper tests
	if got := EqualSamplesDueOffset(0, 1, 20000); got != 20*time.Millisecond {
		t.Fatalf("EqualSamplesDueOffset(0, 1, 20000) = %v want 20ms", got)
	}
	if got := EqualSamplesDueOffset(2999, 1, 20000); got != 60*time.Second {
		t.Fatalf("EqualSamplesDueOffset(2999, 1, 20000) = %v want 60s", got)
	}
	if got := EqualSamplesDueOffset(0, 100, 20000); got != 2*time.Second {
		t.Fatalf("EqualSamplesDueOffset(0, 100, 20000) = %v want 2s", got)
	}
	if got := EqualSamplesDueOffset(29, 100, 20000); got != 60*time.Second {
		t.Fatalf("EqualSamplesDueOffset(29, 100, 20000) = %v want 60s", got)
	}
	if got := EqualSamplesDueOffset(-1, 100, 20000); got != 0 {
		t.Fatalf("negative index should return 0, got %v", got)
	}
}

func TestEqualSamplesNormalizedFullStreamIdentical(t *testing.T) {
	var m100 Manifest
	if err := json.Unmarshal([]byte(goldenManifestJSONS100ArmA()), &m100); err != nil {
		t.Fatalf("unmarshal golden S100: %v", err)
	}

	m100.Limits.PendingMessages = 4000
	m100.Limits.PendingBytes = 16 << 20

	var m1 Manifest
	if err := json.Unmarshal([]byte(goldenManifestJSONS1ArmB()), &m1); err != nil {
		t.Fatalf("unmarshal golden S1: %v", err)
	}
	// Align arm and target for direct normalized sample-stream comparison
	m1.Arm = "A"
	m1.Targets = []Target{{GatewayID: "gw-a", SensorID: "s-a"}}
	m1.GatewayIdentityFiles = map[string]string{"gw-a": "priv/a.json"}
	m1.Limits.PendingMessages = 4000
	m1.Limits.PendingBytes = 16 << 20

	transport100 := &fakeLoadTransport{}
	engine100, err := NewTelemetryEngine(m100, map[string]TelemetryTransport{"gw-a": transport100})
	if err != nil {
		t.Fatalf("NewTelemetryEngine S100: %v", err)
	}

	transport1 := &fakeLoadTransport{}
	engine1, err := NewTelemetryEngine(m1, map[string]TelemetryTransport{"gw-a": transport1})
	if err != nil {
		t.Fatalf("NewTelemetryEngine S1: %v", err)
	}

	now := fixedGoldenNow()
	// Generate all 30 batches for S=100
	for i := 0; i < m100.EqualSamples.ExpectedMessages; i++ {
		if err := engine100.generate(now, "measure"); err != nil {
			t.Fatalf("engine100 generate message %d: %v", i, err)
		}
	}
	// Verify No N+1 for S=100
	if err := engine100.generate(now, "measure"); err == nil {
		t.Fatal("engine100 must reject N+1 message generation")
	}

	// Generate all 3000 batches for S=1
	for i := 0; i < m1.EqualSamples.ExpectedMessages; i++ {
		if err := engine1.generate(now, "measure"); err != nil {
			t.Fatalf("engine1 generate message %d: %v", i, err)
		}
	}
	// Verify No N+1 for S=1
	if err := engine1.generate(now, "measure"); err == nil {
		t.Fatal("engine1 must reject N+1 message generation")
	}

	type sampleItem struct {
		Sequence   int64
		Value      float64
		MeasuredAt string
	}

	// Collect normalized samples from S=100
	var samples100 []sampleItem
	for _, p := range engine100.pending {
		batch := p.batch.Batch
		for idx, s := range batch.Samples {
			seq := batch.FirstSequence + int64(idx)
			samples100 = append(samples100, sampleItem{
				Sequence: seq,
				Value:    s,
			})
		}
	}

	// Collect normalized samples from S=1
	var samples1 []sampleItem
	for _, p := range engine1.pending {
		batch := p.batch.Batch
		for idx, s := range batch.Samples {
			seq := batch.FirstSequence + int64(idx)
			samples1 = append(samples1, sampleItem{
				Sequence: seq,
				Value:    s,
			})
		}
	}

	if len(samples100) != 3000 {
		t.Fatalf("samples100 count=%d want 3000", len(samples100))
	}
	if len(samples1) != 3000 {
		t.Fatalf("samples1 count=%d want 3000", len(samples1))
	}

	// Sort by sequence to ensure normalized ordering
	sortSamples := func(items []sampleItem) {
		for i := 0; i < len(items); i++ {
			for j := i + 1; j < len(items); j++ {
				if items[j].Sequence < items[i].Sequence {
					items[i], items[j] = items[j], items[i]
				}
			}
		}
	}
	sortSamples(samples100)
	sortSamples(samples1)

	anchorTime, _ := ParseRFC3339Anchor(m100.EqualSamples.TimestampAnchor)

	// Invariant: S=1 and S=100 normalized sequence, value, and timestamp are identical!
	for k := 0; k < 3000; k++ {
		wantSeq := int64(k)

		if samples100[k].Sequence != wantSeq || samples1[k].Sequence != wantSeq {
			t.Fatalf("sample %d sequence mismatch: S100=%d S1=%d want=%d", k, samples100[k].Sequence, samples1[k].Sequence, wantSeq)
		}
		if samples100[k].Value != samples1[k].Value {
			t.Fatalf("sample %d value mismatch: S100=%f S1=%f", k, samples100[k].Value, samples1[k].Value)
		}
	}

	// Check measured_at timestamps across batches
	// For S=100, batch m (m=0..29) has FirstSequence = m*100, MeasuredAt = anchor + m*100*interval
	for _, p := range engine100.pending {
		batch := p.batch.Batch
		wantMeasured := anchorTime.Add(time.Duration(batch.FirstSequence*m100.EqualSamples.SampleIntervalUS) * time.Microsecond).UTC()
		parsed, err := time.Parse(time.RFC3339Nano, batch.MeasuredAt)
		if err != nil || !parsed.Equal(wantMeasured) {
			t.Fatalf("S100 batch FirstSequence %d MeasuredAt=%v want %v", batch.FirstSequence, batch.MeasuredAt, wantMeasured)
		}
	}
	// For S=1, batch m (m=0..2999) has FirstSequence = m, MeasuredAt = anchor + m*interval
	for _, p := range engine1.pending {
		batch := p.batch.Batch
		wantMeasured := anchorTime.Add(time.Duration(batch.FirstSequence*m1.EqualSamples.SampleIntervalUS) * time.Microsecond).UTC()
		parsed, err := time.Parse(time.RFC3339Nano, batch.MeasuredAt)
		if err != nil || !parsed.Equal(wantMeasured) {
			t.Fatalf("S1 batch FirstSequence %d MeasuredAt=%v want %v", batch.FirstSequence, batch.MeasuredAt, wantMeasured)
		}
	}
}

func TestEqualSamplesByteExactRetryInvariants(t *testing.T) {
	var m Manifest
	if err := json.Unmarshal([]byte(goldenManifestJSONS100ArmA()), &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	transport := &fakeLoadTransport{}
	engine, err := NewTelemetryEngine(m, map[string]TelemetryTransport{"gw-a": transport})
	if err != nil {
		t.Fatalf("NewTelemetryEngine: %v", err)
	}

	now := fixedGoldenNow()
	if err := engine.generate(now, "measure"); err != nil {
		t.Fatalf("generate: %v", err)
	}

	var messageID uuid.UUID
	for id := range engine.pending {
		messageID = id
		break
	}
	origPayload := append([]byte(nil), engine.pending[messageID].batch.Payload...)

	// Initial job processing (attempt 1)
	ctx := context.Background()
	if !engine.processJob(ctx, messageID) {
		t.Fatal("processJob attempt 1 failed")
	}

	c1 := engine.Counters()
	if c1.PublishAttempts != 1 || c1.Retries != 0 || c1.OfferedUnique != 1 {
		t.Fatalf("after attempt 1 counters: %+v", c1)
	}
	if c1.AttemptedPayloadBytes != int64(len(origPayload)) || c1.OfferedUniquePayloadBytes != int64(len(origPayload)) {
		t.Fatalf("after attempt 1 bytes: %+v", c1)
	}

	// Second attempt (retry 1): queue job again
	engine.mu.Lock()
	p := engine.pending[messageID]
	p.queued = true
	engine.mu.Unlock()

	if !engine.processJob(ctx, messageID) {
		t.Fatal("processJob retry failed")
	}

	c2 := engine.Counters()
	if c2.PublishAttempts != 2 || c2.Retries != 1 || c2.OfferedUnique != 1 {
		t.Fatalf("after retry counters: %+v", c2)
	}
	if c2.AttemptedPayloadBytes != 2*int64(len(origPayload)) || c2.OfferedUniquePayloadBytes != int64(len(origPayload)) {
		t.Fatalf("after retry bytes: %+v", c2)
	}

	// Invariant: transport received identical payload bytes
	transport.mu.Lock()
	defer transport.mu.Unlock()
	if len(transport.publishes) != 2 {
		t.Fatalf("transport publishes count=%d want 2", len(transport.publishes))
	}
	if string(transport.publishes[0]) != string(origPayload) {
		t.Fatal("first attempt payload bytes differ from original")
	}
	if string(transport.publishes[1]) != string(origPayload) {
		t.Fatal("retried payload bytes differ from original (byte-exact retry violated)")
	}
}

func TestEqualSamplesPendingCapFailsVisibly(t *testing.T) {
	var m Manifest
	if err := json.Unmarshal([]byte(goldenManifestJSONS100ArmA()), &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	m.Limits.PendingMessages = 1
	m.Limits.Workers = 0 // no workers consuming jobs, pending will fill up

	transport := &fakeLoadTransport{}
	engine, err := NewTelemetryEngine(m, map[string]TelemetryTransport{"gw-a": transport})
	if err != nil {
		t.Fatalf("NewTelemetryEngine: %v", err)
	}

	now := fixedGoldenNow()
	// First batch fits
	if err := engine.generate(now, "measure"); err != nil {
		t.Fatalf("first generate failed: %v", err)
	}
	// Second batch must trigger pending_cap
	err = engine.generate(now, "measure")
	if err == nil || (err.Error() != "pending_cap" && err.Error() != "pending_message_cap") {
		t.Fatalf("second generate should fail with pending cap, got: %v", err)
	}
}

func TestEqualSamplesCancellationFailsVisibly(t *testing.T) {
	var m Manifest
	if err := json.Unmarshal([]byte(goldenManifestJSONS100ArmA()), &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	// Shorten duration for quick test execution
	m.EqualSamples.DurationSeconds = 1
	m.EqualSamples.ExpectedMessages = 10
	m.Phases.MeasureSeconds = 1

	transport := &fakeLoadTransport{}
	engine, err := NewTelemetryEngine(m, map[string]TelemetryTransport{"gw-a": transport})
	if err != nil {
		t.Fatalf("NewTelemetryEngine: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	// Cancel almost immediately
	time.AfterFunc(5*time.Millisecond, cancel)

	status, runErr := engine.Run(ctx)
	if status != "PARTIAL" && status != "FAILED" {
		t.Fatalf("cancelled run status=%s err=%v, want PARTIAL or FAILED", status, runErr)
	}
	if !engine.stopGeneration.Load() {
		t.Fatal("stopGeneration must be true after cancellation")
	}
}

func TestEqualSamplesDeadlineFailsVisibly(t *testing.T) {
	var m Manifest
	if err := json.Unmarshal([]byte(goldenManifestJSONS100ArmA()), &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	// Wall seconds too short to finish even 1 message
	m.Limits.WallSeconds = 0

	transport := &fakeLoadTransport{}
	engine, err := NewTelemetryEngine(m, map[string]TelemetryTransport{"gw-a": transport})
	if err != nil {
		t.Fatalf("NewTelemetryEngine: %v", err)
	}

	ctx := context.Background()
	status, runErr := engine.Run(ctx)
	if status != "PARTIAL" && status != "FAILED" {
		t.Fatalf("deadline exceeded status=%s err=%v, want PARTIAL or FAILED", status, runErr)
	}
}

func TestEqualSamplesRunResultProjection(t *testing.T) {
	var m Manifest
	if err := json.Unmarshal([]byte(goldenManifestJSONS100ArmA()), &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	counters := TelemetryCounters{
		Generated:                 30,
		OfferedUnique:             30,
		GeneratedPayloadBytes:     15000,
		OfferedUniquePayloadBytes: 15000,
		AttemptedPayloadBytes:     15000,
		PublishAttempts:           30,
		ApplicationACKs:           30,
		Missed:                    0,
		SchedulingLagMaxNS:        1250000,
	}

	summary := makeEqualSamplesSummary(m, counters, "COMPLETE")
	if summary == nil {
		t.Fatal("makeEqualSamplesSummary returned nil")
	}

	if summary.ContextSHA256 != m.EqualSamples.ContextSHA256 {
		t.Fatalf("ContextSHA256 = %q want %q", summary.ContextSHA256, m.EqualSamples.ContextSHA256)
	}
	if summary.WorkloadType != "equal_samples" {
		t.Fatalf("WorkloadType = %q want equal_samples", summary.WorkloadType)
	}
	if summary.LogicalProducer != "A" {
		t.Fatalf("LogicalProducer = %q want A", summary.LogicalProducer)
	}
	if summary.TotalSamples != 6000 || summary.SampleRate != 100 || summary.DurationSeconds != 60 {
		t.Fatalf("contract fields mismatch: total=%d rate=%d dur=%d", summary.TotalSamples, summary.SampleRate, summary.DurationSeconds)
	}
	if summary.SamplesPerMessage != 100 || summary.ExpectedSamples != 3000 || summary.ExpectedMessages != 30 {
		t.Fatalf("message fields mismatch: s=%d expSamples=%d expMsg=%d", summary.SamplesPerMessage, summary.ExpectedSamples, summary.ExpectedMessages)
	}
	if summary.GeneratedSamples != 3000 || summary.GeneratedMessages != 30 {
		t.Fatalf("generated mismatch: samples=%d messages=%d", summary.GeneratedSamples, summary.GeneratedMessages)
	}
	if summary.OfferedUniqueSamples != 3000 || summary.OfferedUniqueMessages != 30 {
		t.Fatalf("offered mismatch: samples=%d messages=%d", summary.OfferedUniqueSamples, summary.OfferedUniqueMessages)
	}
	if summary.ApplicationACKs != 30 {
		t.Fatalf("ApplicationACKs = %d want 30", summary.ApplicationACKs)
	}
	if summary.OfferedUniqueBytes != 15000 || summary.AttemptedBytes != 15000 {
		t.Fatalf("bytes mismatch: unique=%d attempted=%d", summary.OfferedUniqueBytes, summary.AttemptedBytes)
	}
	if summary.ScheduleMisses != 0 || summary.SchedulingLagMaxNS != 1250000 {
		t.Fatalf("timing mismatch: misses=%d lag=%d", summary.ScheduleMisses, summary.SchedulingLagMaxNS)
	}
	if !summary.CompletedOnTime {
		t.Fatal("CompletedOnTime must be true for COMPLETE with 0 misses")
	}
	if summary.SamplingQualification == "" {
		t.Fatal("SamplingQualification must be populated")
	}

	// Verify RunResult JSON encoding
	result := RunResult{
		Status:         "COMPLETE",
		Implementation: "test",
		Experiment:     "E2",
		Arm:            "A",
		Profile:        "pilot",
		Mode:           "remote",
		RunID:          "run-1",
		Counters:       counters,
		EqualSamples:   summary,
	}

	data, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("marshal RunResult: %v", err)
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("unmarshal raw RunResult: %v", err)
	}

	if _, ok := raw["equal_samples"]; !ok {
		t.Fatal("RunResult JSON missing 'equal_samples' key")
	}
	if _, ok := raw["telemetry"]; !ok {
		t.Fatal("RunResult JSON missing 'telemetry' key (old counters preserved)")
	}

	// Negative: with misses, CompletedOnTime must be false
	countersWithMisses := counters
	countersWithMisses.Missed = 2
	summaryMissed := makeEqualSamplesSummary(m, countersWithMisses, "COMPLETE")
	if summaryMissed.CompletedOnTime {
		t.Fatal("CompletedOnTime must be false when misses > 0")
	}

	// When m.EqualSamples is nil, makeEqualSamplesSummary must return nil
	mLegacy := m
	mLegacy.EqualSamples = nil
	if makeEqualSamplesSummary(mLegacy, counters, "COMPLETE") != nil {
		t.Fatal("makeEqualSamplesSummary must return nil when EqualSamples is nil")
	}
}

type autoAckLoadTransport struct {
	gatewayID string
	engine    **TelemetryEngine
}

func (a *autoAckLoadTransport) Start() error                    { return nil }
func (a *autoAckLoadTransport) WaitReady(context.Context) error { return nil }
func (a *autoAckLoadTransport) Publish(_ context.Context, _ string, payload []byte) error {
	var parsed struct {
		MessageID string `json:"message_id"`
	}
	_ = json.Unmarshal(payload, &parsed)
	if a.engine != nil && *a.engine != nil && parsed.MessageID != "" {
		ackTopic := "gateways/" + a.gatewayID + "/acks/" + parsed.MessageID
		nowStr := time.Now().UTC().Format(time.RFC3339Nano)
		ackPayload := []byte(`{"protocol_version":1,"message_type":"telemetry_ack","message_id":"` + parsed.MessageID + `","status":"committed","committed_at":"` + nowStr + `"}`)
		(*a.engine).Admit(a.gatewayID, ackTopic, ackPayload, false)
	}
	return nil
}
func (a *autoAckLoadTransport) StopReconnect()             {}
func (a *autoAckLoadTransport) Stop(context.Context) error { return nil }

func TestEqualSamplesSchedulerFullRunComplete(t *testing.T) {
	var m Manifest
	if err := json.Unmarshal([]byte(goldenManifestJSONS100ArmA()), &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	m.EqualSamples.DurationSeconds = 1
	m.EqualSamples.ExpectedMessages = 2
	m.EqualSamples.ExpectedSamples = 200
	m.EqualSamples.SamplesPerProducer = 200
	m.EqualSamples.SampleIntervalUS = 5000 // 5ms per sample
	m.EqualSamples.TimestampAnchor = time.Now().Add(-10 * time.Minute).UTC().Truncate(time.Second).Format("2006-01-02T15:04:05Z")
	m.SamplesPerMessage = 100
	m.Phases.MeasureSeconds = 1
	m.Phases.DrainSeconds = 1
	m.Limits.WallSeconds = 5
	m.Limits.PendingMessages = 10
	m.Limits.PendingBytes = 1 << 20
	m.Limits.Workers = 2
	m.Traffic.RatePerSecond = 2

	var engine *TelemetryEngine
	transport := &autoAckLoadTransport{gatewayID: "gw-a", engine: &engine}
	var err error
	engine, err = NewTelemetryEngine(m, map[string]TelemetryTransport{"gw-a": transport})
	if err != nil {
		t.Fatalf("NewTelemetryEngine: %v", err)
	}

	ctx := context.Background()
	status, runErr := engine.Run(ctx)
	if runErr != nil || status != "COMPLETE" {
		t.Fatalf("run status=%s err=%v want COMPLETE (remain=%d, cleanup=%v, phaseFailed=%v, counters=%+v)",
			status, runErr, engine.Counters().PendingMessages, engine.CleanupErrors(), engine.phaseFailed, engine.Counters())
	}

	c := engine.Counters()
	if c.Generated != 2 || c.OfferedUnique != 2 || c.ApplicationACKs != 2 {
		t.Fatalf("counters mismatch: gen=%d offered=%d acks=%d", c.Generated, c.OfferedUnique, c.ApplicationACKs)
	}
	if c.PendingMessages != 0 {
		t.Fatalf("pending messages remaining: %d", c.PendingMessages)
	}

	summary := makeEqualSamplesSummary(m, c, status)
	if !summary.CompletedOnTime {
		t.Fatal("summary.CompletedOnTime must be true")
	}
	if summary.GeneratedMessages != 2 || summary.GeneratedSamples != 200 {
		t.Fatalf("summary generated mismatch: msg=%d samples=%d", summary.GeneratedMessages, summary.GeneratedSamples)
	}
	if summary.OfferedUniqueMessages != 2 || summary.OfferedUniqueSamples != 200 {
		t.Fatalf("summary offered mismatch: msg=%d samples=%d", summary.OfferedUniqueMessages, summary.OfferedUniqueSamples)
	}
	if summary.ApplicationACKs != 2 {
		t.Fatalf("summary ACKs mismatch: %d", summary.ApplicationACKs)
	}
}
