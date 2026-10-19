package digitaltwin

import (
	"testing"
	"time"
)

func TestProtocolCapsAndContractBounds(t *testing.T) {
	if MaxRawQueryBytes != 1024 {
		t.Errorf("MaxRawQueryBytes = %d, want 1024", MaxRawQueryBytes)
	}
	if MaxDecodedURNLength != 160 {
		t.Errorf("MaxDecodedURNLength = %d, want 160", MaxDecodedURNLength)
	}
	if MaxEncodedCursorLength != 768 {
		t.Errorf("MaxEncodedCursorLength = %d, want 768", MaxEncodedCursorLength)
	}
	if MaxDecodedCursorBytes != 512 {
		t.Errorf("MaxDecodedCursorBytes = %d, want 512", MaxDecodedCursorBytes)
	}
	if CursorVersionPrefix != "v1." {
		t.Errorf("CursorVersionPrefix = %q, want %q", CursorVersionPrefix, "v1.")
	}

	if ContractDefaultQueryTimeout != 5*time.Second {
		t.Errorf("ContractDefaultQueryTimeout = %v, want 5s", ContractDefaultQueryTimeout)
	}
	if ContractMaxQueryTimeout != 5*time.Second {
		t.Errorf("ContractMaxQueryTimeout = %v, want 5s", ContractMaxQueryTimeout)
	}
	if ContractMaxHistoryRange != 744*time.Hour {
		t.Errorf("ContractMaxHistoryRange = %v, want 744h", ContractMaxHistoryRange)
	}
	if ContractMaxRawWindow != 10*time.Second {
		t.Errorf("ContractMaxRawWindow = %v, want 10s", ContractMaxRawWindow)
	}
	if ContractDefaultMaxPoints != 1000 {
		t.Errorf("ContractDefaultMaxPoints = %d, want 1000", ContractDefaultMaxPoints)
	}
	if ContractMaxPointsUpperLimit != 2000 {
		t.Errorf("ContractMaxPointsUpperLimit = %d, want 2000", ContractMaxPointsUpperLimit)
	}
	if ContractMinPointsLowerLimit != 100 {
		t.Errorf("ContractMinPointsLowerLimit = %d, want 100", ContractMinPointsLowerLimit)
	}
	if ContractDefaultLimit != 50 {
		t.Errorf("ContractDefaultLimit = %d, want 50", ContractDefaultLimit)
	}
	if ContractMaxLimit != 200 {
		t.Errorf("ContractMaxLimit = %d, want 200", ContractMaxLimit)
	}
}
