package digitaltwin

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func readbackQueryFixture() ConfigurationQuery {
	return ConfigurationQuery{
		ProtocolVersion: 1,
		MessageType:     "configuration_query",
		RequestID:       "0195e18d-1111-7a42-9064-69ea49e63bf3",
		GatewayID:       "gateway_001",
		EntityID:        "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
		IssuedAt:        time.Date(2026, 11, 17, 12, 0, 0, 123456000, time.UTC),
		ExpiresAt:       time.Date(2026, 11, 17, 12, 0, 5, 123456000, time.UTC),
	}
}

func readbackSnapshotFixture() ConfigurationSnapshot {
	durable, actual := int64(20000), int64(30000)
	return ConfigurationSnapshot{
		ProtocolVersion:         1,
		MessageType:             "configuration_snapshot",
		RequestID:               "0195e18d-1111-7a42-9064-69ea49e63bf3",
		GatewayID:               "gateway_001",
		EntityID:                "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
		WatermarkVersion:        12,
		ConfigurationState:      "configured",
		DurableSampleIntervalUS: &durable,
		ActualSampleIntervalUS:  &actual,
		ReadbackAt:              time.Date(2026, 11, 17, 12, 0, 1, 123456000, time.UTC),
	}
}

func TestMQTTConfigurationQueryStrictRoundTrip(t *testing.T) {
	query := readbackQueryFixture()
	payload, err := MarshalMQTTConfigurationQuery(query)
	if err != nil {
		t.Fatal(err)
	}
	got, err := ParseMQTTConfigurationQuery("gateways/"+query.GatewayID+"/commands/"+query.RequestID, payload, false)
	if err != nil || got != query {
		t.Fatalf("roundtrip got=%+v err=%v", got, err)
	}
}

func TestMQTTConfigurationQueryRejectsInvalidEnvelopes(t *testing.T) {
	q := readbackQueryFixture()
	valid, err := MarshalMQTTConfigurationQuery(q)
	if err != nil {
		t.Fatal(err)
	}
	duplicate := bytes.Replace(valid, []byte(`"protocol_version":1`), []byte(`"protocol_version":1,"protocol_version":1`), 1)
	unknown := bytes.Replace(valid, []byte(`"protocol_version":1`), []byte(`"unknown":true,"protocol_version":1`), 1)
	nullID := bytes.Replace(valid, []byte(`"request_id":"`+q.RequestID+`"`), []byte(`"request_id":null`), 1)
	badExponent := bytes.Replace(valid, []byte(`"protocol_version":1`), []byte(`"protocol_version":1e0`), 1)
	badTimeOrder := bytes.Replace(valid, []byte(`"expires_at":"2026-11-17T12:00:05.123456Z"`), []byte(`"expires_at":"2026-11-17T12:00:00.123456Z"`), 1)
	wrongEntity := bytes.Replace(valid, []byte(`urn:ngsi-ld:Sensor:gateway_001:sensor_001`), []byte(`urn:ngsi-ld:Sensor:other_gateway:sensor_001`), 1)
	tooLarge := append(bytes.TrimSuffix(valid, []byte("}")), []byte(`,"pad":"`+strings.Repeat("x", 1024)+`"}`)...)
	tests := map[string]struct {
		topic    string
		payload  []byte
		retained bool
	}{
		"duplicate":        {queryTopic(q), duplicate, false},
		"unknown":          {queryTopic(q), unknown, false},
		"null":             {queryTopic(q), nullID, false},
		"trailing":         {queryTopic(q), append(append([]byte(nil), valid...), []byte(` {}`)...), false},
		"exponent integer": {queryTopic(q), badExponent, false},
		"expiry order":     {queryTopic(q), badTimeOrder, false},
		"wrong entity":     {queryTopic(q), wrongEntity, false},
		"oversized":        {queryTopic(q), tooLarge, false},
		"retained":         {queryTopic(q), valid, true},
		"wrong topic ID":   {"gateways/" + q.GatewayID + "/commands/" + uuid.NewString(), valid, false},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseMQTTConfigurationQuery(tc.topic, tc.payload, tc.retained); err == nil {
				t.Fatal("invalid query accepted")
			}
		})
	}
}

func TestMQTTConfigurationSnapshotStrictRoundTripAndStates(t *testing.T) {
	snapshot := readbackSnapshotFixture()
	payload, err := MarshalMQTTConfigurationSnapshot(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	got, err := ParseMQTTConfigurationSnapshot("gateways/"+snapshot.GatewayID+"/responses/"+snapshot.RequestID, payload, false)
	if err != nil || got.ProtocolVersion != snapshot.ProtocolVersion || got.MessageType != snapshot.MessageType ||
		got.RequestID != snapshot.RequestID || got.GatewayID != snapshot.GatewayID || got.EntityID != snapshot.EntityID ||
		got.WatermarkVersion != snapshot.WatermarkVersion || got.ConfigurationState != snapshot.ConfigurationState ||
		got.DurableSampleIntervalUS == nil || *got.DurableSampleIntervalUS != *snapshot.DurableSampleIntervalUS ||
		got.ActualSampleIntervalUS == nil || *got.ActualSampleIntervalUS != *snapshot.ActualSampleIntervalUS ||
		!got.ReadbackAt.Equal(snapshot.ReadbackAt) {
		t.Fatalf("roundtrip got=%+v err=%v", got, err)
	}

	unconfigured := snapshot
	unconfigured.WatermarkVersion = 0
	unconfigured.ConfigurationState = "unconfigured"
	unconfigured.DurableSampleIntervalUS = nil
	if _, err := MarshalMQTTConfigurationSnapshot(unconfigured); err != nil {
		t.Fatalf("unconfigured with actual cadence should be valid: %v", err)
	}
	unavailable := unconfigured
	unavailable.ConfigurationState = "unavailable"
	unavailable.ActualSampleIntervalUS = nil
	if _, err := MarshalMQTTConfigurationSnapshot(unavailable); err != nil {
		t.Fatalf("unavailable snapshot should be valid: %v", err)
	}
}

func TestMQTTConfigurationSnapshotRejectsInvalidStateAndIntervals(t *testing.T) {
	base, err := MarshalMQTTConfigurationSnapshot(readbackSnapshotFixture())
	if err != nil {
		t.Fatal(err)
	}
	valid := base
	duplicate := bytes.Replace(valid, []byte(`"protocol_version":1`), []byte(`"protocol_version":1,"protocol_version":1`), 1)
	unknown := bytes.Replace(valid, []byte(`"protocol_version":1`), []byte(`"unknown":true,"protocol_version":1`), 1)
	tooLarge := append(bytes.TrimSuffix(valid, []byte("}")), []byte(`,"pad":"`+strings.Repeat("x", 1024)+`"}`)...)
	badTimestamp := bytes.Replace(valid, []byte(`2026-11-17T12:00:01.123456Z`), []byte(`2026-11-17T12:00:01+00:00`), 1)
	cases := map[string]struct {
		topic    string
		payload  []byte
		retained bool
	}{
		"duplicate":                      {topic: "gateways/gateway_001/responses/0195e18d-1111-7a42-9064-69ea49e63bf3", payload: duplicate},
		"unknown":                        {topic: "gateways/gateway_001/responses/0195e18d-1111-7a42-9064-69ea49e63bf3", payload: unknown},
		"oversized":                      {topic: "gateways/gateway_001/responses/0195e18d-1111-7a42-9064-69ea49e63bf3", payload: tooLarge},
		"retained":                       {topic: "gateways/gateway_001/responses/0195e18d-1111-7a42-9064-69ea49e63bf3", payload: valid, retained: true},
		"bad UTC timestamp":              {topic: "gateways/gateway_001/responses/0195e18d-1111-7a42-9064-69ea49e63bf3", payload: badTimestamp},
		"version number not string":      {topic: "gateways/gateway_001/responses/0195e18d-1111-7a42-9064-69ea49e63bf3", payload: bytes.Replace(base, []byte(`"watermark_version":"12"`), []byte(`"watermark_version":12`), 1)},
		"null version":                   {topic: "gateways/gateway_001/responses/0195e18d-1111-7a42-9064-69ea49e63bf3", payload: bytes.Replace(base, []byte(`"watermark_version":"12"`), []byte(`"watermark_version":null`), 1)},
		"fractional interval":            {topic: "gateways/gateway_001/responses/0195e18d-1111-7a42-9064-69ea49e63bf3", payload: bytes.Replace(base, []byte(`"actual_sample_interval_us":30000`), []byte(`"actual_sample_interval_us":30000.0`), 1)},
		"exponent interval":              {topic: "gateways/gateway_001/responses/0195e18d-1111-7a42-9064-69ea49e63bf3", payload: bytes.Replace(base, []byte(`"actual_sample_interval_us":30000`), []byte(`"actual_sample_interval_us":3e4`), 1)},
		"below minimum":                  {topic: "gateways/gateway_001/responses/0195e18d-1111-7a42-9064-69ea49e63bf3", payload: bytes.Replace(base, []byte(`"actual_sample_interval_us":30000`), []byte(`"actual_sample_interval_us":9999`), 1)},
		"configured without durable":     {topic: "gateways/gateway_001/responses/0195e18d-1111-7a42-9064-69ea49e63bf3", payload: bytes.Replace(base, []byte(`"durable_sample_interval_us":20000`), []byte(`"durable_sample_interval_us":null`), 1)},
		"unconfigured nonzero watermark": {topic: "gateways/gateway_001/responses/0195e18d-1111-7a42-9064-69ea49e63bf3", payload: bytes.Replace(base, []byte(`"configuration_state":"configured"`), []byte(`"configuration_state":"unconfigured"`), 1)},
		"wrong topic ID":                 {topic: "gateways/gateway_001/responses/0195e18d-1111-7a42-9064-69ea49e63bf4", payload: base},
		"entity gateway mismatch":        {topic: "gateways/gateway_001/responses/0195e18d-1111-7a42-9064-69ea49e63bf3", payload: bytes.Replace(base, []byte(`urn:ngsi-ld:Sensor:gateway_001:sensor_001`), []byte(`urn:ngsi-ld:Sensor:other_gateway:sensor_001`), 1)},
	}
	topic := "gateways/gateway_001/responses/0195e18d-1111-7a42-9064-69ea49e63bf3"
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if tc.topic == "" {
				tc.topic = topic
			}
			if _, err := ParseMQTTConfigurationSnapshot(tc.topic, tc.payload, tc.retained); err == nil {
				t.Fatal("invalid snapshot accepted")
			}
		})
	}
}

func queryTopic(q ConfigurationQuery) string {
	return "gateways/" + q.GatewayID + "/commands/" + q.RequestID
}
