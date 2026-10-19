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

// CoreContextV19 defines the authoritative ETSI NGSI-LD V1.9 core context URI.
const CoreContextV19 = "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"

// ProjectNamespace is the custom IRI namespace for project terms.
const ProjectNamespace = "https://example.org/ngsi-ld/iot/v1#"

// CustomTerms defines project-specific custom term mappings for NGSI-LD V1.9.1.
type CustomTerms struct {
	Gateway          string `json:"Gateway,omitempty"`
	Sensor           string `json:"Sensor,omitempty"`
	GatewayID        string `json:"gateway_id,omitempty"`
	Measurement      string `json:"measurement,omitempty"`
	SampleIntervalUs string `json:"sample_interval_us,omitempty"`
	HasSensor        string `json:"hasSensor,omitempty"`
	IsHostedBy       string `json:"isHostedBy,omitempty"`
	MeasurementUnit  string `json:"measurementUnit,omitempty"`
}

var projectCustomTerms = CustomTerms{
	Gateway:          ProjectNamespace + "Gateway",
	Sensor:           ProjectNamespace + "Sensor",
	GatewayID:        ProjectNamespace + "gatewayId",
	Measurement:      ProjectNamespace + "measurement",
	SampleIntervalUs: ProjectNamespace + "sampleIntervalUs",
	HasSensor:        ProjectNamespace + "hasSensor",
	IsHostedBy:       ProjectNamespace + "isHostedBy",
	MeasurementUnit:  ProjectNamespace + "measurementUnit",
}

// ProjectCustomTerms returns the project-specific custom term mappings.
func ProjectCustomTerms() CustomTerms {
	return projectCustomTerms
}

// ProjectContextArray returns the NGSI-LD V1.9.1 @context array with custom terms first and core context URI last.
func ProjectContextArray() []any {
	return []any{
		projectCustomTerms,
		CoreContextV19,
	}
}
