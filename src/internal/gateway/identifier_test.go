package gateway

import (
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
