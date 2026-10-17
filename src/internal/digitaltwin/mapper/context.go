package mapper

// Context defines the versioned inline JSON-LD context object according to Section 11.3.
type Context struct {
	Version          float64 `json:"@version"`
	ID               string  `json:"id"`
	Type             string  `json:"type"`
	Property         string  `json:"Property"`
	Relationship     string  `json:"Relationship"`
	Value            string  `json:"value"`
	ObservedAt       string  `json:"observedAt"`
	CreatedAt        string  `json:"createdAt"`
	ModifiedAt       string  `json:"modifiedAt"`
	Name             string  `json:"name"`
	Description      string  `json:"description"`
	Unit             string  `json:"unit"`
	Measurement      string  `json:"measurement"`
	SampleIntervalUs string  `json:"sampleIntervalUs"`
	HasSensor        string  `json:"hasSensor"`
	GatewayID        string  `json:"gateway_id"`
}

var projectInlineContext = Context{
	Version:          1.1,
	ID:               "@id",
	Type:             "@type",
	Property:         "https://uri.etsi.org/ngsi-ld/Property",
	Relationship:     "https://uri.etsi.org/ngsi-ld/Relationship",
	Value:            "https://uri.etsi.org/ngsi-ld/hasValue",
	ObservedAt:       "https://uri.etsi.org/ngsi-ld/observedAt",
	CreatedAt:        "https://uri.etsi.org/ngsi-ld/createdAt",
	ModifiedAt:       "https://uri.etsi.org/ngsi-ld/modifiedAt",
	Name:             "https://example.org/ngsi-ld/iot/v1#name",
	Description:      "https://example.org/ngsi-ld/iot/v1#description",
	Unit:             "https://example.org/ngsi-ld/iot/v1#unit",
	Measurement:      "https://example.org/ngsi-ld/iot/v1#measurement",
	SampleIntervalUs: "https://example.org/ngsi-ld/iot/v1#sampleIntervalUs",
	HasSensor:        "https://example.org/ngsi-ld/iot/v1#hasSensor",
	GatewayID:        "https://example.org/ngsi-ld/iot/v1#gatewayId",
}

// ProjectInlineContext returns the immutable versioned inline JSON-LD context object from Section 11.3.
func ProjectInlineContext() Context {
	return projectInlineContext
}
