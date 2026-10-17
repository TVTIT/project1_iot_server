package digitaltwin

import (
	"iot-platform/internal/telemetry"
)

// Shared hard protocol caps across Stage 4 read and entity endpoints.
const (
	// MaxRawQueryBytes is the maximum allowed byte length of a raw HTTP query string across read endpoints.
	MaxRawQueryBytes = 1024

	// MaxDecodedURNLength is the maximum allowed character length of a decoded public NGSI-LD URN.
	MaxDecodedURNLength = 160

	// MaxEncodedCursorLength is the maximum allowed length of an encoded pagination cursor string.
	MaxEncodedCursorLength = 768

	// MaxDecodedCursorBytes is the maximum allowed byte length of a decoded cursor JSON document.
	MaxDecodedCursorBytes = 512

	// CursorVersionPrefix is the required version prefix for Stage 4 pagination cursors.
	CursorVersionPrefix = "v1."
)

// Shared contract bounds and defaults for Stage 4 read operations.
const (
	// ContractDefaultQueryTimeout is the contract default timeout for read queries (5s).
	ContractDefaultQueryTimeout = telemetry.DefaultHistoryQueryTimeout

	// ContractMaxQueryTimeout is the contract hard cap for read query timeout (5s).
	ContractMaxQueryTimeout = telemetry.MaxHistoryQueryTimeout

	// ContractMaxHistoryRange is the contract hard cap for history query time ranges (744h / 31 days).
	ContractMaxHistoryRange = telemetry.MaxHistoryMaxRange

	// ContractMaxRawWindow is the contract hard cap for explicit raw resolution queries (10s).
	ContractMaxRawWindow = telemetry.MaxHistoryRawWindow

	// ContractDefaultMaxPoints is the contract default point count for history queries (1000).
	ContractDefaultMaxPoints = 1000

	// ContractMaxPointsUpperLimit is the contract hard cap for history max_points (2000).
	ContractMaxPointsUpperLimit = telemetry.MaxHistoryPointsUpperLimit

	// ContractMinPointsLowerLimit is the contract minimum for requested max_points (100).
	ContractMinPointsLowerLimit = telemetry.MinHistoryPointsLowerLimit

	// ContractDefaultLimit is the contract default limit for Twin list and relationship queries (50).
	ContractDefaultLimit = 50

	// ContractMaxLimit is the contract hard cap for Twin list and relationship queries (200).
	ContractMaxLimit = 200
)
