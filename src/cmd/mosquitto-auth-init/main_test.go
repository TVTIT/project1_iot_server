package main

import (
	"strings"
	"testing"
)

func TestUnsafePasswordRejectedBeforeBootstrap(t *testing.T) {
	for _, password := range []string{"replace_with_backend_mqtt_password", "CHANGE_ME", " \t ", "\u2003", "a\rb", "a\nb", "a\x00b"} {
		err := run(func(key string) string {
			if key == "MQTT_USERNAME" {
				return "backend_service"
			}
			if key == "MQTT_PASSWORD" {
				return password
			}
			return ""
		})
		if err == nil || err.Error() != "invalid initializer configuration" {
			t.Fatalf("expected pre-bootstrap configuration rejection, got %v", err)
		}
		if strings.Contains(err.Error(), password) {
			t.Fatal("error exposed password")
		}
	}
}

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
