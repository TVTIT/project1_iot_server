package gateway

import (
	"errors"
	"regexp"
)

var gatewayIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`)

// ErrInvalidGatewayID marks identifiers outside migration 000010's allowlist.
var ErrInvalidGatewayID = errors.New("invalid gateway identifier")

// ErrInvalidSensorID marks Sensor identifiers outside migration 000010's allowlist.
var ErrInvalidSensorID = errors.New("invalid sensor identifier")

// ValidateSensorID uses the same ASCII allowlist without Gateway reserved names.
// It does not normalize or unescape caller-supplied identifiers.
func ValidateSensorID(id string) error {
	if !gatewayIDPattern.MatchString(id) {
		return ErrInvalidSensorID
	}
	return nil
}

// ValidateGatewayID does not normalize or unescape caller-supplied identifiers.
func ValidateGatewayID(id string) error {
	if id == "backend_service" || !gatewayIDPattern.MatchString(id) {
		return ErrInvalidGatewayID
	}
	return nil
}
