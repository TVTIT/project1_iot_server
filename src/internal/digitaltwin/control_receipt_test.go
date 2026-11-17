package digitaltwin_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/digitaltwin"
)

func receiptJSON(id, gateway, urn, version, received, hash string) []byte {
	return []byte(`{"protocol_version":1,"message_type":"command_receipt","command_id":"` + id +
		`","gateway_id":"` + gateway + `","entity_id":"` + urn + `","desired_version":"` + version +
		`","received_at":"` + received + `","command_hash":"` + hash + `"}`)
}

func TestControlReceiptParserStrictCorrelationAndRoundTrip(t *testing.T) {
	id := uuid.NewString()
	urn := digitaltwin.SensorURNPrefix + "gateway_001:sensor_001"
	hash := strings.Repeat("a", 64)
	topic := "gateways/gateway_001/responses/" + id
	payload := receiptJSON(id, "gateway_001", urn, "9007199254740993", "2026-11-14T14:30:01.123456Z", hash)
	receipt, err := digitaltwin.ParseMQTTReceipt(topic, payload, false)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := digitaltwin.ParseMQTTReceipt(topic, encoded, false)
	if err != nil || parsed != receipt {
		t.Fatalf("round trip=%+v err=%v", parsed, err)
	}

	for name, input := range map[string][]byte{
		"unknown":       append(append([]byte(nil), payload[:len(payload)-1]...), []byte(`,"extra":1}`)...),
		"duplicate key": []byte(strings.Replace(string(payload), `"protocol_version":1`, `"protocol_version":1,"protocol_version":1`, 1)),
		"wrong type":    []byte(strings.Replace(string(payload), `"command_receipt"`, `"command_result"`, 1)),
		"bad hash case": []byte(strings.Replace(string(payload), hash, strings.ToUpper(hash), 1)),
		"bad version":   []byte(strings.Replace(string(payload), `"9007199254740993"`, `9007199254740993`, 1)),
		"bad time":      []byte(strings.Replace(string(payload), "123456Z", "123456+00:00", 1)),
		"trailing":      append(append([]byte(nil), payload...), []byte(` {}`)...),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := digitaltwin.ParseMQTTReceipt(topic, input, false); err == nil {
				t.Fatal("invalid receipt accepted")
			}
		})
	}
	if _, err := digitaltwin.ParseMQTTReceipt(topic, payload, true); err == nil {
		t.Fatal("retained receipt accepted")
	}
	if _, err := digitaltwin.ParseMQTTReceipt(topic, append(payload, bytesOf(8193-len(payload))...), false); err == nil {
		t.Fatal("oversized receipt accepted")
	}
	if _, err := digitaltwin.ParseMQTTReceipt("gateways/other/responses/"+id, payload, false); err == nil {
		t.Fatal("wrong topic accepted")
	}
	if _, err := digitaltwin.ParseMQTTResult(topic, payload, false); err == nil {
		t.Fatal("receipt was accepted as an execution result")
	}
	if receipt.ReceivedAt.Location() != time.UTC {
		t.Fatal("receipt time not normalized to UTC")
	}
}

func bytesOf(n int) []byte { return []byte(strings.Repeat(" ", n)) }
