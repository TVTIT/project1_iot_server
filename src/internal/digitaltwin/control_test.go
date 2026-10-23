package digitaltwin

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

const controlURN = "urn:ngsi-ld:Sensor:gateway_001:sensor_001"
const controlID = "2a405d67-ddbd-4433-ab96-4e652459336b"
const commandTopic = "gateways/gateway_001/commands/" + controlID
const resultTopic = "gateways/gateway_001/responses/" + controlID
const validCommand = `{"protocol_version":1,"message_type":"command","command_id":"` + controlID + `","gateway_id":"gateway_001","entity_id":"` + controlURN + `","action":"update_configuration","parameters":{"sample_interval_us":10000000},"desired_version":"9007199254740993","issued_at":"2026-10-23T08:00:00.000000Z","expires_at":"2026-10-23T08:01:00.000000Z"}`
const validResult = `{"protocol_version":1,"message_type":"command_result","command_id":"` + controlID + `","gateway_id":"gateway_001","entity_id":"` + controlURN + `","desired_version":"9007199254740993","status":"succeeded","reported_state":{"sample_interval_us":10000000},"completed_at":"2026-10-23T08:00:00.250000Z"}`

func desiredBody(hz string) []byte {
	return []byte(`{"frequency_hz":` + hz + `,"expected_desired_version":"0"}`)
}

func TestControlFrequency(t *testing.T) {
	for _, tc := range []struct {
		hz       string
		interval int64
	}{
		{"100", 10000}, {"60", 16667}, {"1.5", 666667}, {"0.5", 2000000}, {"0.1", 10000000}, {"0.01", 100000000}, {"25.6", 39063},
		{"0.00000000011102230246251565404236316680908203125", 1 << 53},
	} {
		t.Run(tc.hz, func(t *testing.T) {
			r, err := ParseDesiredRequest(desiredBody(tc.hz), "key")
			if err != nil {
				t.Fatal(err)
			}
			if r.Frequency.SampleIntervalUS() != tc.interval {
				t.Fatalf("interval=%d want %d", r.Frequency.SampleIntervalUS(), tc.interval)
			}
			if r.Frequency.EffectiveHz() <= 0 {
				t.Fatal("missing display")
			}
		})
	}
	var first string
	for _, hz := range []string{"0.1", "0.10", "1e-1"} {
		r, e := ParseDesiredRequest(desiredBody(hz), "key")
		if e != nil {
			t.Fatal(e)
		}
		if first == "" {
			first = r.Fingerprint()
		}
		if first != r.Fingerprint() {
			t.Fatal("equivalent decimal hashes differ")
		}
	}
	a, _ := ParseDesiredRequest(desiredBody("60"), "key")
	b, _ := ParseDesiredRequest(desiredBody("60.00001"), "key")
	if a.Frequency.SampleIntervalUS() != b.Frequency.SampleIntervalUS() || a.Fingerprint() == b.Fingerprint() {
		t.Fatal("rounded interval is not request identity")
	}
	c, e := ParseDesiredRequest([]byte(`{"frequency_hz":60,"expected_desired_version":"1"}`), "key")
	if e != nil || c.Fingerprint() == a.Fingerprint() {
		t.Fatal("version omitted from fingerprint")
	}
}

func TestControlDesiredRejects(t *testing.T) {
	for _, hz := range []string{"0", "-0", "-1", "100.0000000000000000000000000001", `"0.1"`, "null", "true", "NaN", "Infinity", "0x10", "1/10", "01", ".1", "1.", "1e129", "1e-129", "1e9999999999999999999999999", "1" + strings.Repeat("0", 128)} {
		t.Run(hz, func(t *testing.T) {
			if _, e := ParseDesiredRequest(desiredBody(hz), "key"); e == nil {
				t.Fatal("accepted invalid number")
			}
		})
	}
	for _, hz := range []string{"1e-10", "1e-128"} {
		if _, e := ParseDesiredRequest(desiredBody(hz), "key"); !errors.Is(e, ErrFrequencyNotRepresentable) {
			t.Fatalf("%s: %v", hz, e)
		}
	}
	for _, body := range []string{`{}`, `[]`, `null`, `{"frequency_hz":1}`, `{"frequency_hz":1,"expected_desired_version":0}`, `{"frequency_hz":1,"expected_desired_version":null}`, `{"frequency_hz":1,"expected_desired_version":"-1"}`, `{"frequency_hz":1,"expected_desired_version":"9223372036854775808"}`, `{"frequency_hz":1,"expected_desired_version":"1e1"}`, `{"frequency_hz":1,"expected_desired_version":"0","frequency_hz":2}`, `{"frequency_hz":1,"expected_desired_version":"0","parameters":{"a":1,"a":2}}`, string(desiredBody("1")) + ` {}`, strings.Repeat(" ", 8193)} {
		if _, e := ParseDesiredRequest([]byte(body), "key"); e == nil {
			t.Fatalf("accepted %s", body)
		}
	}
	for _, key := range []string{"", strings.Repeat("a", 129), "x\n", "é"} {
		if _, e := ParseDesiredRequest(desiredBody("1"), key); e == nil {
			t.Fatal("accepted key")
		}
	}
	for _, v := range []string{"0", "9007199254740993", "9223372036854775807"} {
		r, e := ParseDesiredRequest([]byte(`{"frequency_hz":1,"expected_desired_version":"`+v+`"}`), "key")
		if e != nil {
			t.Fatal(e)
		}
		b, e := json.Marshal(r.ExpectedDesiredVersion)
		if e != nil || string(b) != `"`+v+`"` {
			t.Fatalf("version precision: %s %v", b, e)
		}
	}
}

func TestControlMQTTCommand(t *testing.T) {
	c, e := ParseMQTTCommand(commandTopic, []byte(validCommand), false)
	if e != nil {
		t.Fatal(e)
	}
	if c.Parameters.SampleIntervalUS != 10000000 || c.DesiredVersion != 9007199254740993 {
		t.Fatal("precision lost")
	}
	for _, pair := range [][2]string{
		{`"protocol_version":1`, `"protocol_version":2`}, {`"protocol_version":1`, `"protocol_version":1.0`}, {`"message_type":"command"`, `"message_type":"other"`},
		{`update_configuration`, `reboot`}, {`"sample_interval_us":10000000`, `"sample_interval_us":1e7`}, {`"sample_interval_us":10000000`, `"sample_interval_us":10000.0`}, {`"sample_interval_us":10000000`, `"sample_interval_us":9007199254740993`}, {`"sample_interval_us":10000000`, `"sample_interval_us":9999`}, {`"sample_interval_us":10000000`, `"sample_interval_us":null`},
		{`"sample_interval_us":10000000`, `"sample_interval_us":10000,"sample_interval_us":10001`}, {`"sample_interval_us":10000000`, `"sample_interval_us":10000,"frequency_hz":100`},
		{`"action":"update_configuration"`, `"action":"update_configuration","action":"update_configuration"`},
		{`"desired_version":"9007199254740993"`, `"desired_version":9007199254740993`}, {`"desired_version":"9007199254740993"`, `"desired_version":"9223372036854775808"`},
		{`Sensor:gateway_001:`, `Sensor:gateway_002:`}, {`Sensor:gateway_001:sensor_001`, `Gateway:gateway_001`},
		{`08:01:00.000000Z`, `07:59:00.000000Z`}, {`08:01:00.000000Z`, `08:00:00.000000Z`}, {`08:01:00.000000Z`, `08:01:00+00:00`}, {`2026-10-23T08:00:00.000000Z`, `0000-10-23T08:00:00Z`}, {`08:01:00.000000Z`, `08:01:00.0000001Z`},
	} {
		bad := strings.Replace(validCommand, pair[0], pair[1], 1)
		if _, e := ParseMQTTCommand(commandTopic, []byte(bad), false); e == nil {
			t.Fatalf("accepted mutation %v", pair)
		}
	}
	for _, topic := range []string{resultTopic, commandTopic + "/extra", strings.Replace(commandTopic, "gateway_001", "gateway_002", 1), strings.Replace(commandTopic, controlID, "not-uuid", 1)} {
		if _, e := ParseMQTTCommand(topic, []byte(validCommand), false); e == nil {
			t.Fatal("accepted topic")
		}
	}
	if _, e := ParseMQTTCommand(commandTopic, []byte(validCommand), true); e == nil {
		t.Fatal("retained")
	}
	if _, e := ParseMQTTCommand(commandTopic, []byte(strings.Repeat(" ", 8193)), false); e == nil {
		t.Fatal("oversize")
	}
	for _, n := range []string{"10000", "9007199254740992"} {
		if _, e := ParseMQTTCommand(commandTopic, []byte(strings.Replace(validCommand, "10000000", n, 1)), false); e != nil {
			t.Fatal(e)
		}
	}
}

func TestControlMQTTResult(t *testing.T) {
	c, e := ParseMQTTCommand(commandTopic, []byte(validCommand), false)
	if e != nil {
		t.Fatal(e)
	}
	r, e := ParseMQTTResult(resultTopic, []byte(validResult), false)
	if e != nil {
		t.Fatal(e)
	}
	if e := ValidateResultForCommand(r, c); e != nil {
		t.Fatal(e)
	}
	for _, pair := range [][2]string{
		{`"status":"succeeded"`, `"status":"published"`}, {`"status":"succeeded"`, `"status":"failed"`},
		{`"reported_state":{"sample_interval_us":10000000},`, ``},
		{`"status":"succeeded"`, `"status":"succeeded","error_code":"APPLY_FAILED"`},
		{`"sample_interval_us":10000000`, `"sample_interval_us":10000000,"value":5`},
		{`"sample_interval_us":10000000`, `"sample_interval_us":10000000,"sample_interval_us":10000000`},
	} {
		if _, e := ParseMQTTResult(resultTopic, []byte(strings.Replace(validResult, pair[0], pair[1], 1)), false); e == nil {
			t.Fatalf("accepted mutation %v", pair)
		}
	}
	for _, pair := range [][2]string{{`10000000`, `10000001`}, {`9007199254740993`, `9007199254740994`}, {`sensor_001`, `sensor_002`}, {`08:00:00.250000Z`, `07:59:59Z`}, {`08:00:00.250000Z`, `08:01:00.000001Z`}} {
		r, e := ParseMQTTResult(resultTopic, []byte(strings.Replace(validResult, pair[0], pair[1], 1)), false)
		if e != nil {
			t.Fatal(e)
		}
		if ValidateResultForCommand(r, c) == nil {
			t.Fatalf("accepted correlation mismatch %v", pair)
		}
	}
	failed := strings.Replace(validResult, `"status":"succeeded","reported_state":{"sample_interval_us":10000000}`, `"status":"failed","error_code":"APPLY_FAILED","error_message":"driver unavailable"`, 1)
	for _, code := range []string{"COMMAND_EXPIRED", "STALE_DESIRED_VERSION", "UNKNOWN_SENSOR", "UNSUPPORTED_CONFIGURATION", "APPLY_FAILED"} {
		r, e := ParseMQTTResult(resultTopic, []byte(strings.Replace(failed, "APPLY_FAILED", code, 1)), false)
		if e != nil {
			t.Fatal(e)
		}
		if strings.Contains(r.ErrorMessage, "driver") {
			t.Fatal("raw diagnostic escaped into safe model")
		}
	}
	if _, e := ParseMQTTResult(resultTopic, []byte(strings.Replace(failed, "APPLY_FAILED", "SECRET_ERROR", 1)), false); e == nil {
		t.Fatal("unknown failure code")
	}
	if _, e := ParseMQTTResult(resultTopic, []byte(validResult), true); e == nil {
		t.Fatal("retained")
	}
	if _, e := ParseMQTTResult(commandTopic, []byte(validResult), false); e == nil {
		t.Fatal("wrong topic")
	}
	if _, e := ParseMQTTResult(resultTopic, []byte(strings.Repeat(" ", 8193)), false); e == nil {
		t.Fatal("oversize")
	}
}

func TestControlPrecisionAndDomainModels(t *testing.T) {
	r, err := ParseDesiredRequest(desiredBody("0.10"), "key")
	if err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(r.Frequency.RequestedHz())
	if err != nil || string(b) != "0.10" {
		t.Fatalf("requested decimal lost: %s %v", b, err)
	}
	// The ceiling really is duration-safe; rounding to it is representable.
	r, err = ParseDesiredRequest(desiredBody("0.000000000111022302462515654"), "key")
	if err != nil || r.Frequency.SampleIntervalUS() != MaxSampleIntervalUS {
		t.Fatal(r, err)
	}
	if d := time.Duration(r.Frequency.SampleIntervalUS()) * time.Microsecond; d <= 0 || int64(d/time.Microsecond) != MaxSampleIntervalUS {
		t.Fatal("duration overflow")
	}
	if _, err = ParseDesiredRequest(desiredBody("0.00000000011102230246251564"), "key"); !errors.Is(err, ErrFrequencyNotRepresentable) {
		t.Fatal(err)
	}
	for _, status := range []CommandStatus{CommandPending, CommandPublished, CommandAcknowledged, CommandSucceeded, CommandFailed, CommandTimeout} {
		if !status.Valid() || status.Active() == status.Terminal() {
			t.Fatal(status)
		}
	}
	if CommandStatus(RequestUnchanged).Valid() || CommandStatus("accepted").Valid() {
		t.Fatal("request outcome confused with lifecycle")
	}
	if _, err = json.Marshal(Version(-1)); err == nil {
		t.Fatal("negative version marshaled")
	}
	for _, raw := range []string{`""`, `"+1"`, `"1.0"`, `" 1"`, `"é"`, `true`, `{}`, `"` + strings.Repeat("0", 129) + `"`} {
		var v Version
		if json.Unmarshal([]byte(raw), &v) == nil {
			t.Fatalf("accepted version %s", raw)
		}
	}
	if SafeCommandErrorMessage("arbitrary") == "arbitrary" {
		t.Fatal("raw error leaked")
	}
	if (Frequency{}).EffectiveHz() != 0 {
		t.Fatal("zero value display")
	}
}

func TestControlAdditionalStrictJSON(t *testing.T) {
	for _, number := range []string{"1e128", "1e309", `{"x":1,"x":2}`, `[]`, "false"} {
		if _, err := ParseDesiredRequest(desiredBody(number), "key"); err == nil {
			t.Fatal(number)
		}
	}
	for _, change := range [][2]string{
		{`"parameters":{"sample_interval_us":10000000}`, `"parameters":null`},
		{`"sample_interval_us":10000000`, `"sample_interval_us":10000,"sample_\u0069nterval_us":10001`},
		{`"command_id":"` + controlID + `"`, `"command_id":"2a405d67-ddbd-4433-ab96-4e652459336c"`},
		{`"gateway_id":"gateway_001"`, `"gateway_id":"gateway_002"`},
		{`2026-10-23T08:00:00.000000Z`, `2026-02-30T08:00:00Z`},
		{`2026-10-23T08:00:00.000000Z`, `2026-10-23T24:00:00Z`},
		{`2026-10-23T08:00:00.000000Z`, `2026-10-23T08:00:00,123Z`},
		{`"parameters"`, `"Parameters"`},
		{`"action":"update_configuration"`, `"action":null`},
		{`"message_type":"command"`, `"message_type":"\ud800"`},
	} {
		if _, err := ParseMQTTCommand(commandTopic, []byte(strings.Replace(validCommand, change[0], change[1], 1)), false); err == nil {
			t.Fatal(change)
		}
	}
	for _, data := range []string{"[]", "{}", "null", validCommand + "{}", strings.Replace(validCommand, "sample_interval_us", "other", 1)} {
		if _, err := ParseMQTTCommand(commandTopic, []byte(data), false); err == nil {
			t.Fatal("accepted malformed command")
		}
	}
	badUTF8 := append([]byte(validCommand), 0xff)
	if _, err := ParseMQTTCommand(commandTopic, badUTF8, false); err == nil {
		t.Fatal("invalid utf8")
	}
	if _, err := ParseMQTTCommand(commandTopic, []byte(validCommand+strings.Repeat(" ", 8192-len(validCommand))), false); err != nil {
		t.Fatal("exact payload bound", err)
	}
	failed := strings.Replace(validResult, `"status":"succeeded","reported_state":{"sample_interval_us":10000000}`, `"status":"failed","error_code":"COMMAND_EXPIRED"`, 1)
	r, err := ParseMQTTResult(resultTopic, []byte(strings.Replace(failed, "08:00:00.250000Z", "08:02:00Z", 1)), false)
	if err != nil {
		t.Fatal(err)
	}
	c, err := ParseMQTTCommand(commandTopic, []byte(validCommand), false)
	if err != nil {
		t.Fatal(err)
	}
	if err = ValidateResultForCommand(r, c); err != nil {
		t.Fatal("expired failures may complete after expiry", err)
	}
	for _, change := range [][2]string{{`"error_code":"COMMAND_EXPIRED"`, `"error_code":"COMMAND_EXPIRED","reported_state":null`}, {`"error_code":"COMMAND_EXPIRED"`, `"error_code":"COMMAND_EXPIRED","error_message":null`}, {`"error_code":"COMMAND_EXPIRED"`, `"error_code":"COMMAND_EXPIRED","error_message":"unsafe\nmessage"`}, {`"error_code":"COMMAND_EXPIRED"`, `"error_code":"COMMAND_EXPIRED","unexpected":0`}, {`"error_code":"COMMAND_EXPIRED"`, `"error_code":"COMMAND_EXPIRED","error_code":"APPLY_FAILED"`}} {
		if _, err := ParseMQTTResult(resultTopic, []byte(strings.Replace(failed, change[0], change[1], 1)), false); err == nil {
			t.Fatal(change)
		}
	}
}
