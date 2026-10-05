package gateway

import (
	"errors"
	"strings"
	"testing"
)

func TestValidateGatewayID(t *testing.T) {
	for _, id := range []string{"a", "A_0-9", strings.Repeat("a", 64), "Backend_service"} {
		if err := ValidateGatewayID(id); err != nil {
			t.Errorf("valid ID %q: %v", id, err)
		}
	}
	for _, id := range []string{"", strings.Repeat("a", 65), "backend_service", "a/b", "a+b", "a#b", "a:b", " a", "a ", "a\n", "a\x00", "../a", "_a", "-a", "é"} {
		if err := ValidateGatewayID(id); err == nil {
			t.Errorf("accepted invalid ID %q", id)
		}
	}
}

func TestValidateSensorID(t *testing.T) {
	for _, id := range []string{"a", "0", "A_0-9", strings.Repeat("a", 64), "backend_service"} {
		if err := ValidateSensorID(id); err != nil {
			t.Errorf("valid ID %q: %v", id, err)
		}
	}
	for _, id := range []string{"", strings.Repeat("a", 65), "a/b", "../a", "a+b", "a#b", "a:b", "a%2Fb", " a", "a ", "a\n", "a\x00", "_a", "-a", "é", string([]byte{0xff})} {
		if err := ValidateSensorID(id); !errors.Is(err, ErrInvalidSensorID) {
			t.Errorf("invalid ID %q: got %v", id, err)
		}
	}
}
