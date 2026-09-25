// Command gateway runs the development Gateway simulator.
package main

import (
	"log/slog"

	// Retain MQTT dependency for subsequent gateway simulator implementation (Stage 3).
	_ "github.com/eclipse/paho.mqtt.golang"
)

func main() {
	slog.Info("IoT Gateway Simulator initialized.")
}
