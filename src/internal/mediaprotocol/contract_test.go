package mediaprotocol

import (
	"strings"
	"testing"
)

func TestParseUploadRequestAndGrant(t *testing.T) {
	request := []byte(`{"version":1,"gateway_id":"gw-1","upload_id":"up_01","captured_at":"2026-11-20T10:00:00Z","content_type":"video/mp4","size_bytes":80000000,"sha256":"` + strings.Repeat("a", 64) + `"}`)
	got, err := Decode(UploadRequestTopic("gw-1"), request)
	if err != nil {
		t.Fatal(err)
	}
	parsedRequest := got.(UploadRequest)
	if parsedRequest.UploadID != "up_01" {
		t.Fatalf("unexpected parsed request: %#v", got)
	}
	if parsedRequest.CapturedAt != "2026-11-20T10:00:00.000000Z" || parsedRequest.SHA256 != strings.Repeat("a", 64) {
		t.Fatalf("request metadata was not canonicalized: %#v", parsedRequest)
	}

	signed := "https://storage.example/upload/object?token=do-not-log"
	grant := UploadGrant{Version: 1, GatewayID: "gw-1", UploadID: "up_01", Status: GrantNeedUpload, MediaID: "media_01", UploadURL: signed, ExpiresAt: "2026-11-20T10:01:00Z"}
	wire, err := Encode(UploadGrantTopic("gw-1"), grant)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := Decode(UploadGrantTopic("gw-1"), wire)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.(UploadGrant).UploadURL != signed {
		t.Fatal("signed URL was not preserved")
	}
}

func TestRejectWrongDirectionGatewayAndMalformedGrant(t *testing.T) {
	payload := []byte(`{"version":1,"gateway_id":"gw-2","upload_id":"up_01","captured_at":"2026-11-20T10:00:00Z","content_type":"video/mp4","size_bytes":1,"sha256":"` + strings.Repeat("a", 64) + `"}`)
	for _, topic := range []string{UploadGrantTopic("gw-1"), UploadRequestTopic("bad/gw")} {
		if _, err := Decode(topic, payload); err == nil {
			t.Fatalf("Decode(%q) unexpectedly succeeded", topic)
		}
	}
	grant := UploadGrant{Version: 1, GatewayID: "gw-1", UploadID: "up_01", Status: GrantNeedUpload, MediaID: "media_01", UploadURL: "https://user:pass@storage.example/a?token=secret", ExpiresAt: "2026-11-20T10:01:00Z"}
	if _, err := Encode(UploadGrantTopic("gw-1"), grant); err == nil {
		t.Fatal("malformed grant URL unexpectedly accepted")
	}
}

func TestRejectStrictJSONAndOversizeWithoutLeakingInput(t *testing.T) {
	secret := `{"version":1,"gateway_id":"gw-1","upload_id":"../secret","token":"signed-secret"}`
	_, err := Decode(UploadRequestTopic("gw-1"), []byte(secret))
	if err == nil || strings.Contains(err.Error(), "signed-secret") || strings.Contains(err.Error(), secret) {
		t.Fatalf("unsafe error: %v", err)
	}
	if _, err := Decode(UploadRequestTopic("gw-1"), make([]byte, MaxEnvelopeBytes+1)); err == nil {
		t.Fatal("oversize envelope unexpectedly accepted")
	}
	duplicate := []byte(`{"version":1,"gateway_id":"gw-1","gateway_id":"gw-1","upload_id":"up_01","captured_at":"2026-11-20T10:00:00Z","content_type":"video/mp4","size_bytes":1,"sha256":"` + strings.Repeat("a", 64) + `"}`)
	if _, err := Decode(UploadRequestTopic("gw-1"), duplicate); err == nil {
		t.Fatal("duplicate JSON field unexpectedly accepted")
	}
}

func TestUploadResultSpellingAndRetry(t *testing.T) {
	result := UploadResult{Version: 1, GatewayID: "gw-1", UploadID: "up_01", Status: ResultConflict, Retryable: false, Code: "ID_METADATA_CONFLICT"}
	wire, err := Encode(UploadResultTopic("gw-1"), result)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Decode(UploadResultTopic("gw-1"), wire)
	if err != nil || got.(UploadResult).Status != ResultConflict {
		t.Fatalf("decode result: %v, %v", got, err)
	}
}

func TestStrictShapeAndTrustedTopicGateway(t *testing.T) {
	base := `"version":1,"upload_id":"up_01","captured_at":"2026-11-20T10:00:00Z","content_type":"video/mp4","size_bytes":1,"sha256":"` + strings.Repeat("a", 64) + `"`
	for _, payload := range []string{
		`{` + base + `,"gateway_id":"gw-1","GATEWAY_ID":"gw-1"}`,
		`{` + base + `,"Gateway_ID":"gw-1"}`,
		`{` + base + `,"gateway_id":null}`,
		`{` + base + `,"gateway_id":""}`,
		`{` + base + `,"gateway_id":"gw-2"}`,
		`null`,
		`[]`,
		"{\"version\":1,\"gateway_id\":\"gw-1\",\"upload_id\":\"up_01\",\"captured_at\":\"2026-11-20T10:00:00Z\",\"content_type\":\"video/mp4\",\"size_bytes\":1,\"sha256\":\"" + strings.Repeat("a", 63) + string([]byte{0xff}) + "\"}",
	} {
		if _, err := Decode(UploadRequestTopic("gw-1"), []byte(payload)); err == nil {
			t.Fatalf("invalid request accepted: %q", payload)
		}
	}
	got, err := Decode(UploadRequestTopic("gw-1"), []byte(`{"version":1,"upload_id":"up_01","captured_at":"2026-11-20T10:00:00Z","content_type":"video/mp4","size_bytes":1,"sha256":"`+strings.Repeat("a", 64)+`"}`))
	if err != nil || got.(UploadRequest).GatewayID != "gw-1" {
		t.Fatalf("topic gateway not trusted/populated: %#v, %v", got, err)
	}
}

func TestGrantAndResultStateConstraints(t *testing.T) {
	for _, grant := range []UploadGrant{
		{Version: 1, GatewayID: "gw-1", UploadID: "up_01", Status: GrantRetryAfterCompletion, MediaID: "media_01", UploadURL: "https://storage.example/path?token=x", ExpiresAt: "2026-11-20T10:01:00Z"},
		{Version: 1, GatewayID: "gw-1", UploadID: "up_01", Status: GrantNeedUpload, MediaID: "media_01", UploadURL: "https://storage.example?token=x", ExpiresAt: "2026-11-20T10:01:00Z"},
		{Version: 1, GatewayID: "gw-1", UploadID: "up_01", Status: GrantNeedUpload, MediaID: "media_01", UploadURL: "https://storage.example/path", ExpiresAt: "2026-11-20T10:01:00Z"},
		{Version: 1, GatewayID: "gw-1", UploadID: "up_01", Status: GrantNeedUpload, MediaID: "bad/id", UploadURL: "https://storage.example/path?token=x", ExpiresAt: "2026-11-20T10:01:00Z"},
	} {
		if _, err := Encode(UploadGrantTopic("gw-1"), grant); err == nil {
			t.Fatalf("invalid grant accepted: %#v", grant)
		}
	}
	for _, result := range []UploadResult{
		{Version: 1, GatewayID: "gw-1", UploadID: "up_01", Status: ResultReady, Retryable: true, MediaID: "media_01"},
		{Version: 1, GatewayID: "gw-1", UploadID: "up_01", Status: ResultConflict, Code: "OTHER"},
		{Version: 1, GatewayID: "gw-1", UploadID: "up_01", Status: ResultRejected, Retryable: true, MediaID: "bad/id"},
	} {
		if _, err := Encode(UploadResultTopic("gw-1"), result); err == nil {
			t.Fatalf("invalid result accepted: %#v", result)
		}
	}
}

func TestMissingObjectResultIsRetryInstruction(t *testing.T) {
	result := UploadResult{Version: 1, GatewayID: "gw-1", UploadID: "up_01", Status: ResultPending, Code: "OBJECT_MISSING", Retryable: true, MediaID: "23456789-abcd-4abc-8def-0123456789ab"}
	if _, err := Encode(UploadResultTopic("gw-1"), result); err != nil {
		t.Fatal(err)
	}
}

func TestRecoveryPayloadsRequireCanonicalUUIDAndRetryable(t *testing.T) {
	canonicalUUID := "23456789-abcd-4abc-8def-0123456789ab"
	nilUUID := "00000000-0000-0000-0000-000000000000"

	// 1. retry_after_completion must have canonical non-nil UUID and no URL/expiry
	for _, badID := range []string{"", "media_01", nilUUID, "BAD-UUID", "23456789-ABCD-4ABC-8DEF-0123456789AB"} {
		grant := UploadGrant{
			Version: 1, GatewayID: "gw-1", UploadID: "up_01",
			Status: GrantRetryAfterCompletion, MediaID: badID,
		}
		if _, err := Encode(UploadGrantTopic("gw-1"), grant); err == nil {
			t.Errorf("Encode unexpectedly accepted GrantRetryAfterCompletion with media_id=%q", badID)
		}
	}
	validGrant := UploadGrant{
		Version: 1, GatewayID: "gw-1", UploadID: "up_01",
		Status: GrantRetryAfterCompletion, MediaID: canonicalUUID,
	}
	wireGrant, err := Encode(UploadGrantTopic("gw-1"), validGrant)
	if err != nil {
		t.Fatalf("Encode valid GrantRetryAfterCompletion: %v", err)
	}
	parsedGrant, err := Decode(UploadGrantTopic("gw-1"), wireGrant)
	if err != nil || parsedGrant.(UploadGrant).MediaID != canonicalUUID {
		t.Fatalf("Decode valid GrantRetryAfterCompletion: %v, %#v", err, parsedGrant)
	}

	// 2. pending+OBJECT_MISSING must have canonical non-nil UUID and retryable=true
	for _, bad := range []struct {
		id        string
		retryable bool
	}{
		{"", true},
		{"media_01", true},
		{nilUUID, true},
		{canonicalUUID, false},
	} {
		res := UploadResult{
			Version: 1, GatewayID: "gw-1", UploadID: "up_01",
			Status: ResultPending, Code: "OBJECT_MISSING",
			MediaID: bad.id, Retryable: bad.retryable,
		}
		if _, err := Encode(UploadResultTopic("gw-1"), res); err == nil {
			t.Errorf("Encode unexpectedly accepted pending+OBJECT_MISSING with id=%q retryable=%v", bad.id, bad.retryable)
		}
	}
	validRes := UploadResult{
		Version: 1, GatewayID: "gw-1", UploadID: "up_01",
		Status: ResultPending, Code: "OBJECT_MISSING",
		MediaID: canonicalUUID, Retryable: true,
	}
	wireRes, err := Encode(UploadResultTopic("gw-1"), validRes)
	if err != nil {
		t.Fatalf("Encode valid pending+OBJECT_MISSING: %v", err)
	}
	parsedRes, err := Decode(UploadResultTopic("gw-1"), wireRes)
	if err != nil || parsedRes.(UploadResult).MediaID != canonicalUUID || !parsedRes.(UploadResult).Retryable {
		t.Fatalf("Decode valid pending+OBJECT_MISSING: %v, %#v", err, parsedRes)
	}
}
