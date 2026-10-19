package httpserver

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"iot-platform/internal/config"
	"iot-platform/internal/digitaltwin"
	"iot-platform/internal/gateway"
	"iot-platform/internal/telemetry"
)

// QueryError represents a typed query parsing or validation failure.
// Code matches stable error codes defined in Stage 4 contract section 11.5.
type QueryError struct {
	Code    string
	Message string
	Err     error
}

func (e *QueryError) Error() string {
	if e == nil {
		return "query error"
	}
	return fmt.Sprintf("query error (%s): %s", e.Code, e.Message)
}

func (e *QueryError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// TemporalPresentationOptions contains presentation-only choices for the temporal endpoint.
// It is kept in the HTTP layer and must never be passed to domain services or repositories.
type TemporalPresentationOptions struct {
	Format      string   // "temporalValues" or "aggregatedValues"
	AggrMethods []string // e.g. ["avg", "min", "max", "totalCount"]
	PublicAttr  string   // "measurement" or "sample_interval_us"
}

// CanonicalDurationsToResolution maps the 9 canonical ISO durations to telemetry.Resolution.
var CanonicalDurationsToResolution = map[string]telemetry.Resolution{
	"PT0.1S": telemetry.Resolution100ms,
	"PT1S":   telemetry.Resolution1s,
	"PT10S":  telemetry.Resolution10s,
	"PT1M":   telemetry.Resolution1m,
	"PT5M":   telemetry.Resolution5m,
	"PT15M":  telemetry.Resolution15m,
	"PT1H":   telemetry.Resolution1h,
	"PT6H":   telemetry.Resolution6h,
	"P1D":    telemetry.Resolution1d,
}

// ParseTemporalHistoryQuery parses and validates query string for GET /v1/digital-twins/{entity_id}/temporal.
func (p *HistoryQueryParser) ParseTemporalHistoryQuery(rawQuery string, entityID string) (*digitaltwin.TwinHistoryQuery, *TemporalPresentationOptions, error) {
	parsedEntity, err := digitaltwin.ParseEntityURN(entityID)
	if err != nil {
		return nil, nil, &QueryError{
			Code:    "invalid_request",
			Message: "invalid entity_id",
			Err:     digitaltwin.ErrInvalidInput,
		}
	}

	if parsedEntity.Type == digitaltwin.EntityTypeGateway {
		return nil, nil, &QueryError{
			Code:    "invalid_property",
			Message: "gateway entity has no supported history properties",
			Err:     digitaltwin.ErrInvalidProperty,
		}
	}

	allowedKeys := map[string]struct{}{
		"attrs":              {},
		"timerel":            {},
		"timeAt":             {},
		"endTimeAt":          {},
		"timeproperty":       {},
		"aggrPeriodDuration": {},
		"aggrMethods":        {},
		"format":             {},
		"max_points":         {},
	}

	params, err := parseRawQuery(rawQuery, allowedKeys)
	if err != nil {
		return nil, nil, err
	}

	// 1. attrs: REQUIRED, exactly one token
	attrsVal, ok := params["attrs"]
	if !ok || attrsVal == "" {
		return nil, nil, &QueryError{
			Code:    "invalid_request",
			Message: "missing required attrs parameter",
			Err:     telemetry.ErrHistoryInvalidInput,
		}
	}
	if strings.Contains(attrsVal, ",") {
		return nil, nil, &QueryError{
			Code:    "invalid_request",
			Message: "multiple attrs tokens are not supported",
			Err:     telemetry.ErrHistoryInvalidInput,
		}
	}

	var (
		domainProp digitaltwin.HistorySelector
		publicAttr string
	)
	switch attrsVal {
	case "measurement":
		domainProp = digitaltwin.SelectorValue
		publicAttr = "measurement"
	case "value":
		domainProp = digitaltwin.SelectorValue
		publicAttr = "measurement"
	case "sample_interval_us":
		domainProp = digitaltwin.SelectorSampleIntervalUS
		publicAttr = "sample_interval_us"
	default:
		return nil, nil, &QueryError{
			Code:    "invalid_property",
			Message: fmt.Sprintf("unsupported attrs parameter: %s", attrsVal),
			Err:     digitaltwin.ErrInvalidProperty,
		}
	}

	// 2. timerel: REQUIRED, must be "between"
	timerelVal, ok := params["timerel"]
	if !ok || timerelVal == "" {
		return nil, nil, &QueryError{
			Code:    "invalid_request",
			Message: "missing required timerel parameter",
			Err:     telemetry.ErrHistoryInvalidInput,
		}
	}
	if timerelVal != "between" {
		return nil, nil, &QueryError{
			Code:    "invalid_request",
			Message: fmt.Sprintf("unsupported timerel: %s (must be between)", timerelVal),
			Err:     telemetry.ErrHistoryInvalidInput,
		}
	}

	// 3. timeAt and endTimeAt: REQUIRED, UTC Z, <= 6 fractional digits
	timeAtVal, ok := params["timeAt"]
	if !ok || timeAtVal == "" {
		return nil, nil, &QueryError{
			Code:    "invalid_request",
			Message: "missing required timeAt parameter",
			Err:     telemetry.ErrHistoryInvalidInput,
		}
	}
	timeAt, err := parseRFC3339MicroUTCZ(timeAtVal)
	if err != nil {
		return nil, nil, &QueryError{
			Code:    "invalid_request",
			Message: fmt.Sprintf("invalid timeAt timestamp: %v", err),
			Err:     telemetry.ErrHistoryInvalidInput,
		}
	}

	endTimeAtVal, ok := params["endTimeAt"]
	if !ok || endTimeAtVal == "" {
		return nil, nil, &QueryError{
			Code:    "invalid_request",
			Message: "missing required endTimeAt parameter",
			Err:     telemetry.ErrHistoryInvalidInput,
		}
	}
	endTimeAt, err := parseRFC3339MicroUTCZ(endTimeAtVal)
	if err != nil {
		return nil, nil, &QueryError{
			Code:    "invalid_request",
			Message: fmt.Sprintf("invalid endTimeAt timestamp: %v", err),
			Err:     telemetry.ErrHistoryInvalidInput,
		}
	}

	if !timeAt.Before(endTimeAt) {
		return nil, nil, &QueryError{
			Code:    "invalid_request",
			Message: "timeAt must be strictly before endTimeAt",
			Err:     telemetry.ErrHistoryInvalidInput,
		}
	}

	// 4. timeproperty: OPTIONAL, if present must be "observedAt"
	if tpVal, ok := params["timeproperty"]; ok {
		if tpVal != "observedAt" {
			return nil, nil, &QueryError{
				Code:    "invalid_request",
				Message: fmt.Sprintf("unsupported timeproperty: %s (only observedAt is supported)", tpVal),
				Err:     telemetry.ErrHistoryInvalidInput,
			}
		}
	}

	// 5. aggrPeriodDuration: OPTIONAL
	// If absent: ResolutionRaw (never auto!)
	// If present: canonical 9 ISO durations
	var (
		res      telemetry.Resolution
		isBucket bool
		format   string
	)

	durVal, hasDuration := params["aggrPeriodDuration"]
	if hasDuration && durVal != "" {
		mappedRes, ok := CanonicalDurationsToResolution[durVal]
		if !ok {
			return nil, nil, &QueryError{
				Code:    "invalid_resolution",
				Message: fmt.Sprintf("unsupported aggrPeriodDuration: %s", durVal),
				Err:     telemetry.ErrHistoryInvalidResolution,
			}
		}
		res = mappedRes
		isBucket = true
	} else {
		res = telemetry.ResolutionRaw
		isBucket = false
	}

	// 6. aggrMethods:
	// If raw mode: FORBIDDEN!
	// If bucket mode: default ["avg", "min", "max", "totalCount"], or valid non-repeating subset
	var selectedMethods []string
	if !isBucket {
		if _, hasMethods := params["aggrMethods"]; hasMethods {
			return nil, nil, &QueryError{
				Code:    "invalid_request",
				Message: "aggrMethods is not allowed when aggrPeriodDuration is not specified",
				Err:     telemetry.ErrHistoryInvalidInput,
			}
		}
	} else {
		methodsVal, hasMethods := params["aggrMethods"]
		if !hasMethods || methodsVal == "" {
			selectedMethods = []string{"avg", "min", "max", "totalCount"}
		} else {
			tokens := strings.Split(methodsVal, ",")
			seen := make(map[string]struct{}, len(tokens))
			selectedMethods = make([]string, 0, len(tokens))
			for _, t := range tokens {
				t = strings.TrimSpace(t)
				if t == "" {
					return nil, nil, &QueryError{
						Code:    "invalid_request",
						Message: "empty aggrMethod token",
						Err:     telemetry.ErrHistoryInvalidInput,
					}
				}
				if _, dup := seen[t]; dup {
					return nil, nil, &QueryError{
						Code:    "invalid_request",
						Message: fmt.Sprintf("duplicate aggrMethod: %s", t),
						Err:     telemetry.ErrHistoryInvalidInput,
					}
				}
				switch t {
				case "avg", "min", "max", "totalCount":
					seen[t] = struct{}{}
					selectedMethods = append(selectedMethods, t)
				default:
					return nil, nil, &QueryError{
						Code:    "invalid_request",
						Message: fmt.Sprintf("unsupported aggrMethod: %s", t),
						Err:     telemetry.ErrHistoryInvalidInput,
					}
				}
			}
		}
	}

	// 7. format: OPTIONAL
	// raw mode defaults to temporalValues; bucket mode defaults to aggregatedValues
	formatVal, hasFormat := params["format"]
	if !hasFormat || formatVal == "" {
		if isBucket {
			format = "aggregatedValues"
		} else {
			format = "temporalValues"
		}
	} else {
		switch formatVal {
		case "temporalValues":
			if isBucket {
				return nil, nil, &QueryError{
					Code:    "invalid_request",
					Message: "format temporalValues is not allowed with aggrPeriodDuration",
					Err:     telemetry.ErrHistoryInvalidInput,
				}
			}
			format = "temporalValues"
		case "aggregatedValues":
			if !isBucket {
				return nil, nil, &QueryError{
					Code:    "invalid_request",
					Message: "format aggregatedValues requires aggrPeriodDuration",
					Err:     telemetry.ErrHistoryInvalidInput,
				}
			}
			format = "aggregatedValues"
		default:
			return nil, nil, &QueryError{
				Code:    "invalid_request",
				Message: fmt.Sprintf("unsupported format: %s", formatVal),
				Err:     telemetry.ErrHistoryInvalidInput,
			}
		}
	}

	// 8. max_points: OPTIONAL
	maxPoints := p.cfg.DefaultPoints
	if pointsVal, ok := params["max_points"]; ok {
		parsed, err := parseASCIIInteger(pointsVal)
		if err != nil || parsed < p.cfg.MinPoints || parsed > p.cfg.MaxPoints {
			return nil, nil, &QueryError{
				Code:    "invalid_request",
				Message: "invalid max_points parameter",
				Err:     telemetry.ErrHistoryInvalidInput,
			}
		}
		maxPoints = parsed
	}

	// 9. Time range validation using existing telemetry.ValidateHistoryTimeRange
	tr := telemetry.TimeRange{From: timeAt, To: endTimeAt}
	if err := telemetry.ValidateHistoryTimeRange(tr, res, p.cfg.MaxRange, p.cfg.RawWindow, maxPoints); err != nil {
		if errors.Is(err, telemetry.ErrHistoryRangeTooLarge) {
			return nil, nil, &QueryError{
				Code:    "range_too_large",
				Message: "time range exceeds maximum allowed range",
				Err:     err,
			}
		}
		if errors.Is(err, telemetry.ErrHistoryRawLimitExceeded) {
			return nil, nil, &QueryError{
				Code:    "raw_limit_exceeded",
				Message: "raw window exceeds limit",
				Err:     err,
			}
		}
		if errors.Is(err, telemetry.ErrHistoryResolutionTooFine) {
			return nil, nil, &QueryError{
				Code:    "resolution_too_fine",
				Message: "resolution too fine for window",
				Err:     err,
			}
		}
		if errors.Is(err, telemetry.ErrHistoryInvalidResolution) {
			return nil, nil, &QueryError{
				Code:    "invalid_resolution",
				Message: "invalid resolution parameter",
				Err:     err,
			}
		}
		return nil, nil, &QueryError{
			Code:    "invalid_request",
			Message: "invalid time range",
			Err:     err,
		}
	}

	domainQuery := &digitaltwin.TwinHistoryQuery{
		EntityID:   entityID,
		Property:   domainProp,
		Range:      tr,
		Resolution: res,
		MaxPoints:  maxPoints,
	}

	presOptions := &TemporalPresentationOptions{
		Format:      format,
		AggrMethods: selectedMethods,
		PublicAttr:  publicAttr,
	}

	return domainQuery, presOptions, nil
}

// HistoryQueryParser parses and validates HTTP query strings for history and state endpoints.
type HistoryQueryParser struct {
	cfg config.HistoryConfig
}

// NewHistoryQueryParser constructs a HistoryQueryParser with the validated history configuration.
// It fails if cfg is nil or fails validation.
func NewHistoryQueryParser(cfg *config.HistoryConfig) (*HistoryQueryParser, error) {
	if cfg == nil {
		return nil, errors.New("history config must not be nil")
	}
	if err := config.ValidateHistoryConfig(*cfg); err != nil {
		return nil, fmt.Errorf("invalid history config: %w", err)
	}
	return &HistoryQueryParser{cfg: *cfg}, nil
}

// ParseTelemetryHistoryQuery parses and validates raw query string for GET /v1/telemetry/history.
func (p *HistoryQueryParser) ParseTelemetryHistoryQuery(rawQuery string) (*telemetry.TelemetryHistoryQuery, error) {
	allowedKeys := map[string]struct{}{
		"gateway_id": {},
		"sensor_id":  {},
		"from":       {},
		"to":         {},
		"resolution": {},
		"max_points": {},
	}

	params, err := parseRawQuery(rawQuery, allowedKeys)
	if err != nil {
		return nil, err
	}

	gatewayIDVal, ok := params["gateway_id"]
	if !ok || gatewayIDVal == "" {
		return nil, &QueryError{
			Code:    "invalid_request",
			Message: "missing required gateway_id parameter",
			Err:     telemetry.ErrHistoryInvalidInput,
		}
	}
	if err := gateway.ValidateGatewayID(gatewayIDVal); err != nil {
		return nil, &QueryError{
			Code:    "invalid_request",
			Message: "invalid gateway_id",
			Err:     telemetry.ErrHistoryInvalidInput,
		}
	}

	sensorIDVal, ok := params["sensor_id"]
	if !ok || sensorIDVal == "" {
		return nil, &QueryError{
			Code:    "invalid_request",
			Message: "missing required sensor_id parameter",
			Err:     telemetry.ErrHistoryInvalidInput,
		}
	}
	if err := gateway.ValidateSensorID(sensorIDVal); err != nil {
		return nil, &QueryError{
			Code:    "invalid_request",
			Message: "invalid sensor_id",
			Err:     telemetry.ErrHistoryInvalidInput,
		}
	}

	tr, res, maxPoints, err := p.parseCommonTimeAndResolution(params)
	if err != nil {
		return nil, err
	}

	return &telemetry.TelemetryHistoryQuery{
		GatewayID:  gatewayIDVal,
		SensorID:   sensorIDVal,
		Range:      tr,
		Resolution: res,
		MaxPoints:  maxPoints,
	}, nil
}

// ParseTwinHistoryQuery parses and validates raw query string for GET /v1/digital-twins/{entity_id}/history.
func (p *HistoryQueryParser) ParseTwinHistoryQuery(rawQuery string, entityID string) (*digitaltwin.TwinHistoryQuery, error) {
	parsedEntity, err := digitaltwin.ParseEntityURN(entityID)
	if err != nil {
		return nil, &QueryError{
			Code:    "invalid_request",
			Message: "invalid entity_id",
			Err:     digitaltwin.ErrInvalidInput,
		}
	}

	allowedKeys := map[string]struct{}{
		"property":   {},
		"from":       {},
		"to":         {},
		"resolution": {},
		"max_points": {},
	}

	params, err := parseRawQuery(rawQuery, allowedKeys)
	if err != nil {
		return nil, err
	}

	propertyVal, ok := params["property"]
	if !ok || propertyVal == "" {
		return nil, &QueryError{
			Code:    "invalid_request",
			Message: "missing required property parameter",
			Err:     telemetry.ErrHistoryInvalidInput,
		}
	}

	// Gateway entities have no history selectors in Stage 4
	if parsedEntity.Type == digitaltwin.EntityTypeGateway {
		return nil, &QueryError{
			Code:    "invalid_property",
			Message: "gateway entity has no supported history properties",
			Err:     digitaltwin.ErrInvalidProperty,
		}
	}

	// Only value and sample_interval_us selectors are allowed for Sensor
	if propertyVal != string(digitaltwin.SelectorValue) && propertyVal != string(digitaltwin.SelectorSampleIntervalUS) {
		return nil, &QueryError{
			Code:    "invalid_property",
			Message: "invalid property selector",
			Err:     digitaltwin.ErrInvalidProperty,
		}
	}

	tr, res, maxPoints, err := p.parseCommonTimeAndResolution(params)
	if err != nil {
		return nil, err
	}

	return &digitaltwin.TwinHistoryQuery{
		EntityID:   entityID,
		Property:   digitaltwin.HistorySelector(propertyVal),
		Range:      tr,
		Resolution: res,
		MaxPoints:  maxPoints,
	}, nil
}

// ParseTwinStateQuery parses and validates GET /v1/digital-twins/{entity_id}/state.
// The state endpoint accepts no query parameters.
func ParseTwinStateQuery(rawQuery string, entityID string) (*digitaltwin.TwinStateQuery, error) {
	if rawQuery != "" {
		return nil, &QueryError{
			Code:    "invalid_request",
			Message: "state endpoint accepts no query parameters",
			Err:     telemetry.ErrHistoryInvalidInput,
		}
	}

	if _, err := digitaltwin.ParseEntityURN(entityID); err != nil {
		return nil, &QueryError{
			Code:    "invalid_request",
			Message: "invalid entity_id",
			Err:     digitaltwin.ErrInvalidInput,
		}
	}

	return &digitaltwin.TwinStateQuery{
		EntityID: entityID,
	}, nil
}

func (p *HistoryQueryParser) parseCommonTimeAndResolution(params map[string]string) (telemetry.TimeRange, telemetry.Resolution, int, error) {
	fromVal, ok := params["from"]
	if !ok || fromVal == "" {
		return telemetry.TimeRange{}, "", 0, &QueryError{
			Code:    "invalid_request",
			Message: "missing required from parameter",
			Err:     telemetry.ErrHistoryInvalidInput,
		}
	}

	toVal, ok := params["to"]
	if !ok || toVal == "" {
		return telemetry.TimeRange{}, "", 0, &QueryError{
			Code:    "invalid_request",
			Message: "missing required to parameter",
			Err:     telemetry.ErrHistoryInvalidInput,
		}
	}

	fromTime, err := parseRFC3339Micro(fromVal)
	if err != nil {
		return telemetry.TimeRange{}, "", 0, &QueryError{
			Code:    "invalid_request",
			Message: "invalid from timestamp",
			Err:     telemetry.ErrHistoryInvalidInput,
		}
	}

	toTime, err := parseRFC3339Micro(toVal)
	if err != nil {
		return telemetry.TimeRange{}, "", 0, &QueryError{
			Code:    "invalid_request",
			Message: "invalid to timestamp",
			Err:     telemetry.ErrHistoryInvalidInput,
		}
	}

	res := telemetry.ResolutionAuto
	if resVal, ok := params["resolution"]; ok {
		res = telemetry.Resolution(resVal)
		if !res.Valid() {
			return telemetry.TimeRange{}, "", 0, &QueryError{
				Code:    "invalid_resolution",
				Message: "invalid resolution parameter",
				Err:     telemetry.ErrHistoryInvalidResolution,
			}
		}
	}

	maxPoints := p.cfg.DefaultPoints
	if pointsVal, ok := params["max_points"]; ok {
		parsed, err := parseASCIIInteger(pointsVal)
		if err != nil || parsed < p.cfg.MinPoints || parsed > p.cfg.MaxPoints {
			return telemetry.TimeRange{}, "", 0, &QueryError{
				Code:    "invalid_request",
				Message: "invalid max_points parameter",
				Err:     telemetry.ErrHistoryInvalidInput,
			}
		}
		maxPoints = parsed
	}

	tr := telemetry.TimeRange{From: fromTime, To: toTime}
	if err := telemetry.ValidateHistoryTimeRange(tr, res, p.cfg.MaxRange, p.cfg.RawWindow, maxPoints); err != nil {
		if errors.Is(err, telemetry.ErrHistoryRangeTooLarge) {
			return telemetry.TimeRange{}, "", 0, &QueryError{
				Code:    "range_too_large",
				Message: "time range exceeds maximum allowed range",
				Err:     err,
			}
		}
		if errors.Is(err, telemetry.ErrHistoryRawLimitExceeded) {
			return telemetry.TimeRange{}, "", 0, &QueryError{
				Code:    "raw_limit_exceeded",
				Message: "raw window exceeds limit",
				Err:     err,
			}
		}
		if errors.Is(err, telemetry.ErrHistoryResolutionTooFine) {
			return telemetry.TimeRange{}, "", 0, &QueryError{
				Code:    "resolution_too_fine",
				Message: "resolution too fine for window",
				Err:     err,
			}
		}
		if errors.Is(err, telemetry.ErrHistoryInvalidResolution) {
			return telemetry.TimeRange{}, "", 0, &QueryError{
				Code:    "invalid_resolution",
				Message: "invalid resolution parameter",
				Err:     err,
			}
		}
		return telemetry.TimeRange{}, "", 0, &QueryError{
			Code:    "invalid_request",
			Message: "invalid time range",
			Err:     err,
		}
	}

	return tr, res, maxPoints, nil
}

// parseRawQuery performs strict query cap enforcement and url parsing.
// It checks length <= 1024 bytes BEFORE url.ParseQuery, rejects semicolons, malformed escapes,
// unknown keys, repeated keys, and empty values.
func parseRawQuery(rawQuery string, allowedKeys map[string]struct{}) (map[string]string, error) {
	// Rejects > 1024 bytes strictly BEFORE url.ParseQuery
	if len(rawQuery) > digitaltwin.MaxRawQueryBytes {
		return nil, &QueryError{
			Code:    "invalid_request",
			Message: "raw query exceeds maximum allowed length of 1024 bytes",
			Err:     telemetry.ErrHistoryInvalidInput,
		}
	}

	if strings.ContainsRune(rawQuery, ';') {
		return nil, &QueryError{
			Code:    "invalid_request",
			Message: "semicolons are forbidden in query string",
			Err:     telemetry.ErrHistoryInvalidInput,
		}
	}

	parsed, err := url.ParseQuery(rawQuery)
	if err != nil {
		return nil, &QueryError{
			Code:    "invalid_request",
			Message: "malformed query string",
			Err:     telemetry.ErrHistoryInvalidInput,
		}
	}

	result := make(map[string]string, len(parsed))
	for key, values := range parsed {
		if _, ok := allowedKeys[key]; !ok {
			return nil, &QueryError{
				Code:    "invalid_request",
				Message: fmt.Sprintf("unknown query parameter: %s", key),
				Err:     telemetry.ErrHistoryInvalidInput,
			}
		}

		if len(values) > 1 {
			return nil, &QueryError{
				Code:    "invalid_request",
				Message: fmt.Sprintf("repeated query parameter: %s", key),
				Err:     telemetry.ErrHistoryInvalidInput,
			}
		}

		if len(values) == 1 {
			val := values[0]
			if val == "" {
				return nil, &QueryError{
					Code:    "invalid_request",
					Message: fmt.Sprintf("empty query parameter value: %s", key),
					Err:     telemetry.ErrHistoryInvalidInput,
				}
			}
			result[key] = val
		}
	}

	return result, nil
}

// parseASCIIInteger parses a strict base-10 ASCII unsigned integer without sign or exponent.
func parseASCIIInteger(s string) (int, error) {
	if s == "" {
		return 0, errors.New("empty integer string")
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return 0, errors.New("non-digit ASCII in integer")
		}
	}
	return strconv.Atoi(s)
}

// parseRFC3339Micro parses RFC3339 timestamps with required timezone offset and
// strictly enforces lexical 0–6 fractional digits (rejecting 7 digits even with trailing zeros).
// It normalizes to UTC with exact microsecond precision.
func parseRFC3339Micro(s string) (time.Time, error) {
	// Minimum length: "YYYY-MM-DDTHH:MM:SSZ" = 20 chars
	if len(s) < 20 {
		return time.Time{}, errors.New("timestamp too short")
	}

	// Lexical date check: YYYY-MM-DD
	for i := 0; i < 4; i++ {
		if s[i] < '0' || s[i] > '9' {
			return time.Time{}, errors.New("invalid year format")
		}
	}
	yearVal := int(s[0]-'0')*1000 + int(s[1]-'0')*100 + int(s[2]-'0')*10 + int(s[3]-'0')
	if yearVal < 1 {
		return time.Time{}, errors.New("invalid year: must be between 0001 and 9999")
	}
	if s[4] != '-' || s[7] != '-' {
		return time.Time{}, errors.New("invalid date separator")
	}
	if s[5] < '0' || s[5] > '9' || s[6] < '0' || s[6] > '9' {
		return time.Time{}, errors.New("invalid month format")
	}
	if s[8] < '0' || s[8] > '9' || s[9] < '0' || s[9] > '9' {
		return time.Time{}, errors.New("invalid day format")
	}

	// Delimiter 'T' or 't'
	if s[10] != 'T' && s[10] != 't' {
		return time.Time{}, errors.New("missing T delimiter")
	}

	// Time check: HH:MM:SS
	if s[13] != ':' || s[16] != ':' {
		return time.Time{}, errors.New("invalid time separator")
	}
	for _, idx := range []int{11, 12, 14, 15, 17, 18} {
		if s[idx] < '0' || s[idx] > '9' {
			return time.Time{}, errors.New("invalid time digits")
		}
	}

	// Check fractional seconds and offset
	idx := 19
	if s[idx] == '.' {
		// Period present: count fractional digits before offset indicator
		idx++
		dotStart := idx
		for idx < len(s) && s[idx] >= '0' && s[idx] <= '9' {
			idx++
		}
		numFracDigits := idx - dotStart
		if numFracDigits < 1 || numFracDigits > 6 {
			return time.Time{}, fmt.Errorf("fractional digits must be between 1 and 6, got %d", numFracDigits)
		}
	}

	if idx >= len(s) {
		return time.Time{}, errors.New("missing timezone offset indicator")
	}

	// Offset indicator must be 'Z', 'z', '+', or '-'
	offsetIndicator := s[idx]
	switch offsetIndicator {
	case 'Z', 'z':
		if idx != len(s)-1 {
			return time.Time{}, errors.New("unexpected trailing characters after Z offset")
		}
		if offsetIndicator == 'z' {
			s = s[:idx] + "Z"
		}
	case '+', '-':
		// Expect +HH:MM or -HH:MM (length 6)
		if idx+6 != len(s) {
			return time.Time{}, errors.New("invalid timezone offset length")
		}
		if s[idx+3] != ':' {
			return time.Time{}, errors.New("missing colon in timezone offset")
		}
		for _, offsetIdx := range []int{idx + 1, idx + 2, idx + 4, idx + 5} {
			if s[offsetIdx] < '0' || s[offsetIdx] > '9' {
				return time.Time{}, errors.New("invalid digits in timezone offset")
			}
		}
		offsetHours := int(s[idx+1]-'0')*10 + int(s[idx+2]-'0')
		offsetMins := int(s[idx+4]-'0')*10 + int(s[idx+5]-'0')
		if offsetHours > 23 || offsetMins > 59 {
			return time.Time{}, errors.New("invalid timezone offset values")
		}
	default:
		return time.Time{}, errors.New("invalid timezone offset character")
	}

	// Parse with standard library
	parsed, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, err
	}

	// Normalize to UTC exact microseconds
	utc := parsed.UTC()
	if utc.Year() < 1 || utc.Year() > 9999 {
		return time.Time{}, errors.New("UTC normalized year out of range 0001-9999")
	}
	return utc.Truncate(time.Microsecond), nil
}

// parseRFC3339MicroUTCZ parses RFC3339 timestamps strictly requiring an uppercase 'Z' offset
// and at most 6 fractional digits (0 to 6), with year in 0001-9999.
func parseRFC3339MicroUTCZ(s string) (time.Time, error) {
	if len(s) < 20 {
		return time.Time{}, errors.New("timestamp too short")
	}

	for i := 0; i < 4; i++ {
		if s[i] < '0' || s[i] > '9' {
			return time.Time{}, errors.New("invalid year format")
		}
	}
	yearVal := int(s[0]-'0')*1000 + int(s[1]-'0')*100 + int(s[2]-'0')*10 + int(s[3]-'0')
	if yearVal < 1 {
		return time.Time{}, errors.New("invalid year: must be between 0001 and 9999")
	}
	if s[4] != '-' || s[7] != '-' {
		return time.Time{}, errors.New("invalid date separator")
	}
	if s[5] < '0' || s[5] > '9' || s[6] < '0' || s[6] > '9' {
		return time.Time{}, errors.New("invalid month format")
	}
	if s[8] < '0' || s[8] > '9' || s[9] < '0' || s[9] > '9' {
		return time.Time{}, errors.New("invalid day format")
	}

	if s[10] != 'T' {
		return time.Time{}, errors.New("missing T delimiter")
	}

	if s[13] != ':' || s[16] != ':' {
		return time.Time{}, errors.New("invalid time separator")
	}
	for _, idx := range []int{11, 12, 14, 15, 17, 18} {
		if s[idx] < '0' || s[idx] > '9' {
			return time.Time{}, errors.New("invalid time digits")
		}
	}

	idx := 19
	if s[idx] == '.' {
		idx++
		dotStart := idx
		for idx < len(s) && s[idx] >= '0' && s[idx] <= '9' {
			idx++
		}
		numFracDigits := idx - dotStart
		if numFracDigits < 1 || numFracDigits > 6 {
			return time.Time{}, fmt.Errorf("fractional digits must be between 1 and 6, got %d", numFracDigits)
		}
	}

	if idx >= len(s) {
		return time.Time{}, errors.New("missing timezone offset indicator")
	}

	if s[idx] != 'Z' {
		return time.Time{}, errors.New("timezone offset must be UTC Z")
	}
	if idx != len(s)-1 {
		return time.Time{}, errors.New("unexpected trailing characters after Z offset")
	}

	parsed, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, err
	}

	utc := parsed.UTC()
	if utc.Year() < 1 || utc.Year() > 9999 {
		return time.Time{}, errors.New("UTC normalized year out of range 0001-9999")
	}
	return utc.Truncate(time.Microsecond), nil
}
