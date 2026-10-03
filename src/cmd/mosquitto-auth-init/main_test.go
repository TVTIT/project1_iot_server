package main

import "testing"

func TestMissingConfiguration(t *testing.T) {
	for _, username := range []string{"", "gw_fixture", "backend_service"} {
		if run(func(key string) string {
			if key == "MQTT_USERNAME" {
				return username
			}
			return ""
		}) == nil {
			t.Fatal("accepted invalid configuration")
		}
	}
}
