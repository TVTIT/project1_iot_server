// Package mapper maps application identities to project Digital Twin identities.
package mapper

// GatewayEntityID builds a stable, case-sensitive Gateway URN.
// The caller must first validate gatewayID with gateway.ValidateGatewayID.
// This pure mapper neither normalizes identifiers nor fetches JSON-LD contexts.
func GatewayEntityID(gatewayID string) string {
	return "urn:ngsi-ld:Gateway:" + gatewayID
}

// SensorEntityID scopes Sensor identity to its parent Gateway.
// Callers must first validate both IDs with the Gateway and Sensor validators.
func SensorEntityID(gatewayID, sensorID string) string {
	return "urn:ngsi-ld:Sensor:" + gatewayID + ":" + sensorID
}
