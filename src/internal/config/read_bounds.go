package config

import (
	"fmt"
	"strconv"
	"time"

	"iot-platform/internal/digitaltwin"
	"iot-platform/internal/telemetry"
)

// HistoryConfig holds Stage 4 telemetry history query bounds and deadlines.
type HistoryConfig struct {
	QueryTimeout  time.Duration
	MaxRange      time.Duration
	RawWindow     time.Duration
	DefaultPoints int
	MaxPoints     int
	MinPoints     int
}

// TwinConfig holds Stage 4 Digital Twin list and relationship query limits.
type TwinConfig struct {
	ListDefaultLimit         int
	ListMaxLimit             int
	RelationshipDefaultLimit int
	RelationshipMaxLimit     int
}

// DefaultHistoryConfig returns the contract default Stage 4 history read bounds.
func DefaultHistoryConfig() HistoryConfig {
	return HistoryConfig{
		QueryTimeout:  digitaltwin.ContractDefaultQueryTimeout,
		MaxRange:      digitaltwin.ContractMaxHistoryRange,
		RawWindow:     digitaltwin.ContractMaxRawWindow,
		DefaultPoints: digitaltwin.ContractDefaultMaxPoints,
		MaxPoints:     digitaltwin.ContractMaxPointsUpperLimit,
		MinPoints:     digitaltwin.ContractMinPointsLowerLimit,
	}
}

// DefaultTwinConfig returns the contract default Stage 4 Digital Twin list and relationship limits.
func DefaultTwinConfig() TwinConfig {
	return TwinConfig{
		ListDefaultLimit:         digitaltwin.ContractDefaultLimit,
		ListMaxLimit:             digitaltwin.ContractMaxLimit,
		RelationshipDefaultLimit: digitaltwin.ContractDefaultLimit,
		RelationshipMaxLimit:     digitaltwin.ContractMaxLimit,
	}
}

// ValidateHistoryConfig validates that configured history bounds conform to positive
// duration constraints, contract hard caps, and internal consistency.
func ValidateHistoryConfig(cfg HistoryConfig) error {
	if cfg.QueryTimeout <= 0 {
		return fmt.Errorf("HISTORY_QUERY_TIMEOUT must be a positive duration")
	}
	if cfg.QueryTimeout > digitaltwin.ContractMaxQueryTimeout {
		return fmt.Errorf("HISTORY_QUERY_TIMEOUT cannot exceed contract hard cap of %s", digitaltwin.ContractMaxQueryTimeout)
	}

	if cfg.MaxRange <= 0 {
		return fmt.Errorf("HISTORY_MAX_RANGE must be a positive duration")
	}
	if cfg.MaxRange > digitaltwin.ContractMaxHistoryRange {
		return fmt.Errorf("HISTORY_MAX_RANGE cannot exceed contract hard cap of %s", digitaltwin.ContractMaxHistoryRange)
	}

	if cfg.RawWindow <= 0 {
		return fmt.Errorf("HISTORY_RAW_WINDOW must be a positive duration")
	}
	if cfg.RawWindow > digitaltwin.ContractMaxRawWindow {
		return fmt.Errorf("HISTORY_RAW_WINDOW cannot exceed contract hard cap of %s", digitaltwin.ContractMaxRawWindow)
	}
	if cfg.RawWindow > cfg.MaxRange {
		return fmt.Errorf("HISTORY_RAW_WINDOW cannot exceed HISTORY_MAX_RANGE")
	}

	if cfg.MinPoints < digitaltwin.ContractMinPointsLowerLimit {
		return fmt.Errorf("HISTORY_MIN_POINTS cannot be less than contract lower limit of %d", digitaltwin.ContractMinPointsLowerLimit)
	}
	if cfg.MinPoints > digitaltwin.ContractMaxPointsUpperLimit {
		return fmt.Errorf("HISTORY_MIN_POINTS cannot exceed contract hard cap of %d", digitaltwin.ContractMaxPointsUpperLimit)
	}
	if cfg.DefaultPoints < cfg.MinPoints {
		return fmt.Errorf("HISTORY_DEFAULT_POINTS must be greater than or equal to HISTORY_MIN_POINTS")
	}
	if cfg.MaxPoints > digitaltwin.ContractMaxPointsUpperLimit {
		return fmt.Errorf("HISTORY_MAX_POINTS cannot exceed contract hard cap of %d", digitaltwin.ContractMaxPointsUpperLimit)
	}
	if cfg.DefaultPoints > cfg.MaxPoints {
		return fmt.Errorf("HISTORY_DEFAULT_POINTS must be less than or equal to HISTORY_MAX_POINTS")
	}

	return nil
}

// ValidateTwinConfig validates that configured Twin list and relationship limits conform to positive
// integers, contract hard caps, and default <= max ordering.
func ValidateTwinConfig(cfg TwinConfig) error {
	if cfg.ListDefaultLimit < 1 {
		return fmt.Errorf("TWIN_LIST_DEFAULT_LIMIT must be at least 1")
	}
	if cfg.ListMaxLimit > digitaltwin.ContractMaxLimit {
		return fmt.Errorf("TWIN_LIST_MAX_LIMIT cannot exceed contract hard cap of %d", digitaltwin.ContractMaxLimit)
	}
	if cfg.ListDefaultLimit > cfg.ListMaxLimit {
		return fmt.Errorf("TWIN_LIST_DEFAULT_LIMIT must be less than or equal to TWIN_LIST_MAX_LIMIT")
	}

	if cfg.RelationshipDefaultLimit < 1 {
		return fmt.Errorf("TWIN_RELATIONSHIP_DEFAULT_LIMIT must be at least 1")
	}
	if cfg.RelationshipMaxLimit > digitaltwin.ContractMaxLimit {
		return fmt.Errorf("TWIN_RELATIONSHIP_MAX_LIMIT cannot exceed contract hard cap of %d", digitaltwin.ContractMaxLimit)
	}
	if cfg.RelationshipDefaultLimit > cfg.RelationshipMaxLimit {
		return fmt.Errorf("TWIN_RELATIONSHIP_DEFAULT_LIMIT must be less than or equal to TWIN_RELATIONSHIP_MAX_LIMIT")
	}

	return nil
}

// LoadHistory loads and validates HistoryConfig from lookup.
func LoadHistory(lookup LookupFunc) (HistoryConfig, error) {
	return loadHistory(lookup)
}

// LoadTwin loads and validates TwinConfig from lookup.
func LoadTwin(lookup LookupFunc) (TwinConfig, error) {
	return loadTwin(lookup)
}

func loadHistory(lookup LookupFunc) (HistoryConfig, error) {
	defaults := DefaultHistoryConfig()
	var err error

	cfg := defaults
	if cfg.QueryTimeout, err = parseReadDuration(lookup, "HISTORY_QUERY_TIMEOUT", defaults.QueryTimeout); err != nil {
		return HistoryConfig{}, err
	}
	if cfg.MaxRange, err = parseReadDuration(lookup, "HISTORY_MAX_RANGE", defaults.MaxRange); err != nil {
		return HistoryConfig{}, err
	}
	if cfg.RawWindow, err = parseReadDuration(lookup, "HISTORY_RAW_WINDOW", defaults.RawWindow); err != nil {
		return HistoryConfig{}, err
	}

	if cfg.DefaultPoints, err = parsePositiveInt(lookup, "HISTORY_DEFAULT_POINTS", defaults.DefaultPoints); err != nil {
		return HistoryConfig{}, err
	}
	if cfg.MaxPoints, err = parsePositiveInt(lookup, "HISTORY_MAX_POINTS", defaults.MaxPoints); err != nil {
		return HistoryConfig{}, err
	}
	if cfg.MinPoints, err = parsePositiveInt(lookup, "HISTORY_MIN_POINTS", defaults.MinPoints); err != nil {
		return HistoryConfig{}, err
	}

	if err := ValidateHistoryConfig(cfg); err != nil {
		return HistoryConfig{}, err
	}
	return cfg, nil
}

func loadTwin(lookup LookupFunc) (TwinConfig, error) {
	defaults := DefaultTwinConfig()
	var err error

	cfg := defaults
	if cfg.ListDefaultLimit, err = parsePositiveInt(lookup, "TWIN_LIST_DEFAULT_LIMIT", defaults.ListDefaultLimit); err != nil {
		return TwinConfig{}, err
	}
	if cfg.ListMaxLimit, err = parsePositiveInt(lookup, "TWIN_LIST_MAX_LIMIT", defaults.ListMaxLimit); err != nil {
		return TwinConfig{}, err
	}
	if cfg.RelationshipDefaultLimit, err = parsePositiveInt(lookup, "TWIN_RELATIONSHIP_DEFAULT_LIMIT", defaults.RelationshipDefaultLimit); err != nil {
		return TwinConfig{}, err
	}
	if cfg.RelationshipMaxLimit, err = parsePositiveInt(lookup, "TWIN_RELATIONSHIP_MAX_LIMIT", defaults.RelationshipMaxLimit); err != nil {
		return TwinConfig{}, err
	}

	if err := ValidateTwinConfig(cfg); err != nil {
		return TwinConfig{}, err
	}
	return cfg, nil
}

// ToTelemetryPreparationConfig maps HistoryConfig to telemetry.HistoryPreparationConfig.
func (c HistoryConfig) ToTelemetryPreparationConfig() telemetry.HistoryPreparationConfig {
	return telemetry.HistoryPreparationConfig{
		QueryTimeout: c.QueryTimeout,
		MaxRange:     c.MaxRange,
		RawWindow:    c.RawWindow,
		MinPoints:    c.MinPoints,
		MaxPoints:    c.MaxPoints,
	}
}

// ToReadPreparationConfig maps TwinConfig and HistoryConfig to digitaltwin.ReadPreparationConfig.
func (c TwinConfig) ToReadPreparationConfig(h HistoryConfig) digitaltwin.ReadPreparationConfig {
	return digitaltwin.ReadPreparationConfig{
		QueryTimeout:         h.QueryTimeout,
		ListMaxLimit:         c.ListMaxLimit,
		RelationshipMaxLimit: c.RelationshipMaxLimit,
		HistoryMaxRange:      h.MaxRange,
		HistoryRawWindow:     h.RawWindow,
		HistoryMinPoints:     h.MinPoints,
		HistoryMaxPoints:     h.MaxPoints,
	}
}

func parseReadDuration(lookup LookupFunc, key string, fallback time.Duration) (time.Duration, error) {
	raw, ok := nonEmpty(lookup, key)
	if !ok {
		return fallback, nil
	}
	val, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s has invalid duration syntax: %w", key, err)
	}
	if val <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration", key)
	}
	return val, nil
}

func parsePositiveInt(lookup LookupFunc, key string, fallback int) (int, error) {
	raw, ok := nonEmpty(lookup, key)
	if !ok {
		return fallback, nil
	}
	val, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer: %w", key, err)
	}
	if val <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", key)
	}
	return val, nil
}
