package config

import (
	"strings"
	"testing"
	"time"

	"iot-platform/internal/digitaltwin"
)

func TestStage4ReadDefaults(t *testing.T) {
	t.Run("default constructors produce contract defaults", func(t *testing.T) {
		h := DefaultHistoryConfig()
		if h.QueryTimeout != 5*time.Second {
			t.Errorf("QueryTimeout = %v, want 5s", h.QueryTimeout)
		}
		if h.MaxRange != 744*time.Hour {
			t.Errorf("MaxRange = %v, want 744h", h.MaxRange)
		}
		if h.RawWindow != 10*time.Second {
			t.Errorf("RawWindow = %v, want 10s", h.RawWindow)
		}
		if h.DefaultPoints != 1000 {
			t.Errorf("DefaultPoints = %d, want 1000", h.DefaultPoints)
		}
		if h.MaxPoints != 2000 {
			t.Errorf("MaxPoints = %d, want 2000", h.MaxPoints)
		}
		if h.MinPoints != 100 {
			t.Errorf("MinPoints = %d, want 100", h.MinPoints)
		}

		tw := DefaultTwinConfig()
		if tw.ListDefaultLimit != 50 {
			t.Errorf("ListDefaultLimit = %d, want 50", tw.ListDefaultLimit)
		}
		if tw.ListMaxLimit != 200 {
			t.Errorf("ListMaxLimit = %d, want 200", tw.ListMaxLimit)
		}
		if tw.RelationshipDefaultLimit != 50 {
			t.Errorf("RelationshipDefaultLimit = %d, want 50", tw.RelationshipDefaultLimit)
		}
		if tw.RelationshipMaxLimit != 200 {
			t.Errorf("RelationshipMaxLimit = %d, want 200", tw.RelationshipMaxLimit)
		}
	})

	t.Run("Load with no env overrides yields contract defaults", func(t *testing.T) {
		cfg, err := Load(mapLookup(validConfigValues()))
		if err != nil {
			t.Fatalf("unexpected load error: %v", err)
		}

		// History assertions
		if cfg.History.QueryTimeout != 5*time.Second {
			t.Errorf("History.QueryTimeout = %v, want 5s", cfg.History.QueryTimeout)
		}
		if cfg.History.MaxRange != 744*time.Hour {
			t.Errorf("History.MaxRange = %v, want 744h", cfg.History.MaxRange)
		}
		if cfg.History.RawWindow != 10*time.Second {
			t.Errorf("History.RawWindow = %v, want 10s", cfg.History.RawWindow)
		}
		if cfg.History.DefaultPoints != 1000 {
			t.Errorf("History.DefaultPoints = %d, want 1000", cfg.History.DefaultPoints)
		}
		if cfg.History.MaxPoints != 2000 {
			t.Errorf("History.MaxPoints = %d, want 2000", cfg.History.MaxPoints)
		}
		if cfg.History.MinPoints != 100 {
			t.Errorf("History.MinPoints = %d, want 100", cfg.History.MinPoints)
		}

		// Twin assertions
		if cfg.Twin.ListDefaultLimit != 50 {
			t.Errorf("Twin.ListDefaultLimit = %d, want 50", cfg.Twin.ListDefaultLimit)
		}
		if cfg.Twin.ListMaxLimit != 200 {
			t.Errorf("Twin.ListMaxLimit = %d, want 200", cfg.Twin.ListMaxLimit)
		}
		if cfg.Twin.RelationshipDefaultLimit != 50 {
			t.Errorf("Twin.RelationshipDefaultLimit = %d, want 50", cfg.Twin.RelationshipDefaultLimit)
		}
		if cfg.Twin.RelationshipMaxLimit != 200 {
			t.Errorf("Twin.RelationshipMaxLimit = %d, want 200", cfg.Twin.RelationshipMaxLimit)
		}
	})
}

func TestStage4ReadValidNarrowOverride(t *testing.T) {
	values := validConfigValues()
	values["HISTORY_QUERY_TIMEOUT"] = "3s"
	values["HISTORY_MAX_RANGE"] = "48h"
	values["HISTORY_RAW_WINDOW"] = "5s"
	values["HISTORY_DEFAULT_POINTS"] = "500"
	values["HISTORY_MAX_POINTS"] = "1000"
	values["HISTORY_MIN_POINTS"] = "100"
	values["TWIN_LIST_DEFAULT_LIMIT"] = "25"
	values["TWIN_LIST_MAX_LIMIT"] = "100"
	values["TWIN_RELATIONSHIP_DEFAULT_LIMIT"] = "30"
	values["TWIN_RELATIONSHIP_MAX_LIMIT"] = "150"

	cfg, err := Load(mapLookup(values))
	if err != nil {
		t.Fatalf("unexpected error for valid narrow override: %v", err)
	}

	if cfg.History.QueryTimeout != 3*time.Second {
		t.Errorf("QueryTimeout = %v, want 3s", cfg.History.QueryTimeout)
	}
	if cfg.History.MaxRange != 48*time.Hour {
		t.Errorf("MaxRange = %v, want 48h", cfg.History.MaxRange)
	}
	if cfg.History.RawWindow != 5*time.Second {
		t.Errorf("RawWindow = %v, want 5s", cfg.History.RawWindow)
	}
	if cfg.History.DefaultPoints != 500 {
		t.Errorf("DefaultPoints = %d, want 500", cfg.History.DefaultPoints)
	}
	if cfg.History.MaxPoints != 1000 {
		t.Errorf("MaxPoints = %d, want 1000", cfg.History.MaxPoints)
	}
	if cfg.History.MinPoints != 100 {
		t.Errorf("MinPoints = %d, want 100", cfg.History.MinPoints)
	}

	if cfg.Twin.ListDefaultLimit != 25 {
		t.Errorf("ListDefaultLimit = %d, want 25", cfg.Twin.ListDefaultLimit)
	}
	if cfg.Twin.ListMaxLimit != 100 {
		t.Errorf("ListMaxLimit = %d, want 100", cfg.Twin.ListMaxLimit)
	}
	if cfg.Twin.RelationshipDefaultLimit != 30 {
		t.Errorf("RelationshipDefaultLimit = %d, want 30", cfg.Twin.RelationshipDefaultLimit)
	}
	if cfg.Twin.RelationshipMaxLimit != 150 {
		t.Errorf("RelationshipMaxLimit = %d, want 150", cfg.Twin.RelationshipMaxLimit)
	}
}

func TestStage4ReadNarrowQueryTimeoutAllowed(t *testing.T) {
	values := validConfigValues()
	values["HISTORY_QUERY_TIMEOUT"] = "50ms"

	cfg, err := Load(mapLookup(values))
	if err != nil {
		t.Fatalf("unexpected error for narrow query timeout 50ms: %v", err)
	}
	if cfg.History.QueryTimeout != 50*time.Millisecond {
		t.Errorf("QueryTimeout = %v, want 50ms", cfg.History.QueryTimeout)
	}
}

func TestStage4ReadZeroValueRejection(t *testing.T) {
	testCases := []struct {
		envKey string
		envVal string
	}{
		{"HISTORY_QUERY_TIMEOUT", "0s"},
		{"HISTORY_MAX_RANGE", "0s"},
		{"HISTORY_RAW_WINDOW", "0s"},
		{"HISTORY_DEFAULT_POINTS", "0"},
		{"HISTORY_MAX_POINTS", "0"},
		{"HISTORY_MIN_POINTS", "0"},
		{"TWIN_LIST_DEFAULT_LIMIT", "0"},
		{"TWIN_LIST_MAX_LIMIT", "0"},
		{"TWIN_RELATIONSHIP_DEFAULT_LIMIT", "0"},
		{"TWIN_RELATIONSHIP_MAX_LIMIT", "0"},
	}

	for _, tc := range testCases {
		t.Run(tc.envKey, func(t *testing.T) {
			values := validConfigValues()
			values[tc.envKey] = tc.envVal
			_, err := Load(mapLookup(values))
			if err == nil {
				t.Fatalf("expected error for %s=%s, got nil", tc.envKey, tc.envVal)
			}
		})
	}
}

func TestStage4ReadNegativeValueRejection(t *testing.T) {
	testCases := []struct {
		envKey string
		envVal string
	}{
		{"HISTORY_QUERY_TIMEOUT", "-1s"},
		{"HISTORY_MAX_RANGE", "-10h"},
		{"HISTORY_RAW_WINDOW", "-5s"},
		{"HISTORY_DEFAULT_POINTS", "-100"},
		{"HISTORY_MAX_POINTS", "-200"},
		{"HISTORY_MIN_POINTS", "-10"},
		{"TWIN_LIST_DEFAULT_LIMIT", "-5"},
		{"TWIN_LIST_MAX_LIMIT", "-20"},
		{"TWIN_RELATIONSHIP_DEFAULT_LIMIT", "-5"},
		{"TWIN_RELATIONSHIP_MAX_LIMIT", "-20"},
	}

	for _, tc := range testCases {
		t.Run(tc.envKey, func(t *testing.T) {
			values := validConfigValues()
			values[tc.envKey] = tc.envVal
			_, err := Load(mapLookup(values))
			if err == nil {
				t.Fatalf("expected error for %s=%s, got nil", tc.envKey, tc.envVal)
			}
		})
	}
}

func TestStage4ReadMalformedValueRejection(t *testing.T) {
	testCases := []struct {
		envKey  string
		envVal  string
		wantSub string
	}{
		{"HISTORY_QUERY_TIMEOUT", "invalid", "invalid duration syntax"},
		{"HISTORY_MAX_RANGE", "31d", "invalid duration syntax"},
		{"HISTORY_RAW_WINDOW", "bad-window", "invalid duration syntax"},
		{"HISTORY_DEFAULT_POINTS", "notanumber", "must be an integer"},
		{"HISTORY_MAX_POINTS", "two-thousand", "must be an integer"},
		{"HISTORY_MIN_POINTS", "one-hundred", "must be an integer"},
		{"TWIN_LIST_DEFAULT_LIMIT", "1.5", "must be an integer"},
		{"TWIN_LIST_MAX_LIMIT", "two-hundred", "must be an integer"},
		{"TWIN_RELATIONSHIP_DEFAULT_LIMIT", "fifty", "must be an integer"},
		{"TWIN_RELATIONSHIP_MAX_LIMIT", "200.0", "must be an integer"},
	}

	for _, tc := range testCases {
		t.Run(tc.envKey, func(t *testing.T) {
			values := validConfigValues()
			values[tc.envKey] = tc.envVal
			_, err := Load(mapLookup(values))
			if err == nil {
				t.Fatalf("expected error for %s=%s, got nil", tc.envKey, tc.envVal)
			}
			if tc.wantSub != "" && !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("error %q does not contain %q", err.Error(), tc.wantSub)
			}
		})
	}
}

func TestStage4ReadOverflowRejection(t *testing.T) {
	testCases := []struct {
		envKey string
		envVal string
	}{
		{"HISTORY_DEFAULT_POINTS", "9999999999999999999999999999999"},
		{"HISTORY_MAX_POINTS", "9999999999999999999999999999999"},
		{"HISTORY_MIN_POINTS", "9999999999999999999999999999999"},
		{"TWIN_LIST_DEFAULT_LIMIT", "9999999999999999999999999999999"},
		{"TWIN_LIST_MAX_LIMIT", "9999999999999999999999999999999"},
		{"TWIN_RELATIONSHIP_DEFAULT_LIMIT", "9999999999999999999999999999999"},
		{"TWIN_RELATIONSHIP_MAX_LIMIT", "9999999999999999999999999999999"},
	}

	for _, tc := range testCases {
		t.Run(tc.envKey, func(t *testing.T) {
			values := validConfigValues()
			values[tc.envKey] = tc.envVal
			_, err := Load(mapLookup(values))
			if err == nil {
				t.Fatalf("expected error for overflow %s=%s, got nil", tc.envKey, tc.envVal)
			}
		})
	}
}

func TestStage4ReadHardBoundRejection(t *testing.T) {
	testCases := []struct {
		name    string
		envKey  string
		envVal  string
		wantSub string
	}{
		{
			name:    "history max range exceeds 744h contract hard cap",
			envKey:  "HISTORY_MAX_RANGE",
			envVal:  "745h",
			wantSub: "cannot exceed contract hard cap of 744h",
		},
		{
			name:    "history raw window exceeds 10s contract hard cap",
			envKey:  "HISTORY_RAW_WINDOW",
			envVal:  "11s",
			wantSub: "cannot exceed contract hard cap of 10s",
		},
		{
			name:    "history max points exceeds 2000 contract hard cap",
			envKey:  "HISTORY_MAX_POINTS",
			envVal:  "2001",
			wantSub: "cannot exceed contract hard cap of 2000",
		},
		{
			name:    "twin list max limit exceeds 200 contract hard cap",
			envKey:  "TWIN_LIST_MAX_LIMIT",
			envVal:  "201",
			wantSub: "cannot exceed contract hard cap of 200",
		},
		{
			name:    "twin relationship max limit exceeds 200 contract hard cap",
			envKey:  "TWIN_RELATIONSHIP_MAX_LIMIT",
			envVal:  "201",
			wantSub: "cannot exceed contract hard cap of 200",
		},
		{
			name:    "history query timeout exceeds contract hard cap of 5s",
			envKey:  "HISTORY_QUERY_TIMEOUT",
			envVal:  "6s",
			wantSub: "cannot exceed contract hard cap of 5s",
		},
		{
			name:    "history min points below contract lower limit of 100",
			envKey:  "HISTORY_MIN_POINTS",
			envVal:  "99",
			wantSub: "cannot be less than contract lower limit of 100",
		},
		{
			name:    "history min points widens contract at 50",
			envKey:  "HISTORY_MIN_POINTS",
			envVal:  "50",
			wantSub: "cannot be less than contract lower limit of 100",
		},
		{
			name:    "history min points widens contract at 1",
			envKey:  "HISTORY_MIN_POINTS",
			envVal:  "1",
			wantSub: "cannot be less than contract lower limit of 100",
		},
		{
			name:    "history min points exceeds contract hard cap of 2000",
			envKey:  "HISTORY_MIN_POINTS",
			envVal:  "2001",
			wantSub: "cannot exceed contract hard cap of 2000",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			values := validConfigValues()
			values[tc.envKey] = tc.envVal
			_, err := Load(mapLookup(values))
			if err == nil {
				t.Fatalf("expected error for %s=%s, got nil", tc.envKey, tc.envVal)
			}
			if tc.wantSub != "" && !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("error %q does not contain %q", err.Error(), tc.wantSub)
			}
		})
	}
}

func TestStage4ReadDefaultMaxInconsistency(t *testing.T) {
	t.Run("history default points greater than max points", func(t *testing.T) {
		values := validConfigValues()
		values["HISTORY_DEFAULT_POINTS"] = "1500"
		values["HISTORY_MAX_POINTS"] = "1000"
		_, err := Load(mapLookup(values))
		if err == nil {
			t.Fatal("expected error when default_points > max_points, got nil")
		}
		if !strings.Contains(err.Error(), "HISTORY_DEFAULT_POINTS must be less than or equal to HISTORY_MAX_POINTS") {
			t.Errorf("unexpected error message: %v", err)
		}
	})

	t.Run("history default points less than min points", func(t *testing.T) {
		values := validConfigValues()
		values["HISTORY_DEFAULT_POINTS"] = "50"
		values["HISTORY_MIN_POINTS"] = "100"
		_, err := Load(mapLookup(values))
		if err == nil {
			t.Fatal("expected error when default_points < min_points, got nil")
		}
		if !strings.Contains(err.Error(), "HISTORY_DEFAULT_POINTS must be greater than or equal to HISTORY_MIN_POINTS") {
			t.Errorf("unexpected error message: %v", err)
		}
	})

	t.Run("twin list default limit greater than max limit", func(t *testing.T) {
		values := validConfigValues()
		values["TWIN_LIST_DEFAULT_LIMIT"] = "150"
		values["TWIN_LIST_MAX_LIMIT"] = "100"
		_, err := Load(mapLookup(values))
		if err == nil {
			t.Fatal("expected error when list default > max, got nil")
		}
		if !strings.Contains(err.Error(), "TWIN_LIST_DEFAULT_LIMIT must be less than or equal to TWIN_LIST_MAX_LIMIT") {
			t.Errorf("unexpected error message: %v", err)
		}
	})

	t.Run("twin relationship default limit greater than max limit", func(t *testing.T) {
		values := validConfigValues()
		values["TWIN_RELATIONSHIP_DEFAULT_LIMIT"] = "150"
		values["TWIN_RELATIONSHIP_MAX_LIMIT"] = "100"
		_, err := Load(mapLookup(values))
		if err == nil {
			t.Fatal("expected error when relationship default > max, got nil")
		}
		if !strings.Contains(err.Error(), "TWIN_RELATIONSHIP_DEFAULT_LIMIT must be less than or equal to TWIN_RELATIONSHIP_MAX_LIMIT") {
			t.Errorf("unexpected error message: %v", err)
		}
	})
}

func TestStage4ReadRawRangeInconsistency(t *testing.T) {
	t.Run("raw window exceeds max range", func(t *testing.T) {
		values := validConfigValues()
		values["HISTORY_MAX_RANGE"] = "5s"
		values["HISTORY_RAW_WINDOW"] = "10s"
		_, err := Load(mapLookup(values))
		if err == nil {
			t.Fatal("expected error when raw_window > max_range, got nil")
		}
		if !strings.Contains(err.Error(), "HISTORY_RAW_WINDOW cannot exceed HISTORY_MAX_RANGE") {
			t.Errorf("unexpected error message: %v", err)
		}
	})
}

func TestStage4ReadDirectValidationFunctions(t *testing.T) {
	t.Run("ValidateHistoryConfig accepts defaults", func(t *testing.T) {
		if err := ValidateHistoryConfig(DefaultHistoryConfig()); err != nil {
			t.Fatalf("unexpected validation error: %v", err)
		}
	})

	t.Run("ValidateTwinConfig accepts defaults", func(t *testing.T) {
		if err := ValidateTwinConfig(DefaultTwinConfig()); err != nil {
			t.Fatalf("unexpected validation error: %v", err)
		}
	})

	t.Run("ValidateHistoryConfig rejects zero timeout", func(t *testing.T) {
		h := DefaultHistoryConfig()
		h.QueryTimeout = 0
		if err := ValidateHistoryConfig(h); err == nil {
			t.Fatal("expected error for zero QueryTimeout, got nil")
		}
	})

	t.Run("ValidateHistoryConfig rejects query timeout exceeding contract hard cap of 5s", func(t *testing.T) {
		h := DefaultHistoryConfig()
		h.QueryTimeout = 6 * time.Second
		if err := ValidateHistoryConfig(h); err == nil {
			t.Fatal("expected error for QueryTimeout > 5s, got nil")
		}
	})

	t.Run("ToTelemetryPreparationConfig and ToReadPreparationConfig mapping", func(t *testing.T) {
		h := DefaultHistoryConfig()
		tw := DefaultTwinConfig()

		telemetryPrepCfg := h.ToTelemetryPreparationConfig()
		if telemetryPrepCfg.QueryTimeout != h.QueryTimeout ||
			telemetryPrepCfg.MaxRange != h.MaxRange ||
			telemetryPrepCfg.RawWindow != h.RawWindow ||
			telemetryPrepCfg.MinPoints != h.MinPoints ||
			telemetryPrepCfg.MaxPoints != h.MaxPoints {
			t.Fatalf("mismatched telemetry prep config: %+v", telemetryPrepCfg)
		}

		readPrepCfg := tw.ToReadPreparationConfig(h)
		if readPrepCfg.QueryTimeout != h.QueryTimeout ||
			readPrepCfg.ListMaxLimit != tw.ListMaxLimit ||
			readPrepCfg.RelationshipMaxLimit != tw.RelationshipMaxLimit ||
			readPrepCfg.HistoryMaxRange != h.MaxRange ||
			readPrepCfg.HistoryRawWindow != h.RawWindow ||
			readPrepCfg.HistoryMinPoints != h.MinPoints ||
			readPrepCfg.HistoryMaxPoints != h.MaxPoints {
			t.Fatalf("mismatched read prep config: %+v", readPrepCfg)
		}
	})

	t.Run("ValidateHistoryConfig rejects min points below 100", func(t *testing.T) {
		h := DefaultHistoryConfig()
		h.MinPoints = 99
		if err := ValidateHistoryConfig(h); err == nil {
			t.Fatal("expected error for MinPoints < 100, got nil")
		}
	})

	t.Run("ValidateHistoryConfig rejects negative range", func(t *testing.T) {
		h := DefaultHistoryConfig()
		h.MaxRange = -1 * time.Hour
		if err := ValidateHistoryConfig(h); err == nil {
			t.Fatal("expected error for negative MaxRange, got nil")
		}
	})

	t.Run("ValidateHistoryConfig rejects raw window exceeding max range", func(t *testing.T) {
		h := DefaultHistoryConfig()
		h.MaxRange = 5 * time.Second
		h.RawWindow = 10 * time.Second
		if err := ValidateHistoryConfig(h); err == nil {
			t.Fatal("expected error for raw > range, got nil")
		}
	})

	t.Run("ValidateHistoryConfig rejects max range exceeding contract hard cap", func(t *testing.T) {
		h := DefaultHistoryConfig()
		h.MaxRange = digitaltwin.ContractMaxHistoryRange + time.Hour
		if err := ValidateHistoryConfig(h); err == nil {
			t.Fatal("expected error for max range > 744h, got nil")
		}
	})

	t.Run("ValidateTwinConfig rejects list default greater than max", func(t *testing.T) {
		tw := DefaultTwinConfig()
		tw.ListDefaultLimit = 150
		tw.ListMaxLimit = 100
		if err := ValidateTwinConfig(tw); err == nil {
			t.Fatal("expected error for list default > max, got nil")
		}
	})

	t.Run("ValidateTwinConfig rejects relationship default greater than max", func(t *testing.T) {
		tw := DefaultTwinConfig()
		tw.RelationshipDefaultLimit = 150
		tw.RelationshipMaxLimit = 100
		if err := ValidateTwinConfig(tw); err == nil {
			t.Fatal("expected error for relationship default > max, got nil")
		}
	})
}
