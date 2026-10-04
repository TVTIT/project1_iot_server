package mqttcredential

import "testing"

func TestMaintenanceEpochMatchesControllerContract(t *testing.T) {
	epoch, err := freshIdentity()
	if err != nil || !validEpoch(epoch) {
		t.Fatal("controller epoch rejected")
	}
	for _, s := range []string{"", "unsafe diagnostic", epoch[:63], epoch + "0"} {
		if validEpoch(s) {
			t.Fatal("invalid epoch accepted")
		}
	}
}
