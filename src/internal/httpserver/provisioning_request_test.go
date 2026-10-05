package httpserver

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestProvisioningParsingBoundary(t *testing.T) {
	good := `{"name":" raw ","owner_user_id":"11111111-1111-4111-8111-111111111111","description":null}`
	for _, tc := range []struct {
		name, body, media, query string
		limit                    int64
		status                   int
	}{
		{"valid", good, "application/json", "", 16384, 0},
		{"exact limit", good, "application/json; charset=UTF-8", "", int64(len(good)), 0},
		{"over limit", good, "application/json", "", int64(len(good) - 1), 413},
		{"unknown", strings.Replace(good, `"name"`, `"role"`, 1), "application/json", "", 16384, 400},
		{"duplicate", strings.Replace(good, `"name":" raw "`, `"name":"a","na\u006de":"b"`, 1), "application/json", "", 16384, 400},
		{"trailing", good + ` {}`, "application/json", "", 16384, 400},
		{"array", `[]`, "application/json", "", 16384, 400},
		{"null name", strings.Replace(good, `" raw "`, `null`, 1), "application/json", "", 16384, 400},
		{"type", strings.Replace(good, `null`, `4`, 1), "application/json", "", 16384, 400},
		{"utf8", strings.Replace(good, ` raw `, string([]byte{255}), 1), "application/json", "", 16384, 400},
		{"high surrogate", strings.Replace(good, ` raw `, `\ud800`, 1), "application/json", "", 16384, 400},
		{"low surrogate", strings.Replace(good, ` raw `, `\udc00`, 1), "application/json", "", 16384, 400},
		{"pair", strings.Replace(good, ` raw `, `\ud83d\ude00`, 1), "application/json", "", 16384, 0},
		{"nul", strings.Replace(good, ` raw `, `\u0000`, 1), "application/json", "", 16384, 400},
		{"query", good, "application/json", "?actor=x", 16384, 400},
		{"empty query", good, "application/json", "?", 16384, 400},
		{"media", good, "text/json", "", 16384, 415},
		{"charset", good, "application/json; charset=latin1", "", 16384, 415},
		{"parameter", good, "application/json; version=1", "", 16384, 415},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("PUT", "/v1/admin/gateways/test"+tc.query, strings.NewReader(tc.body))
			req.Header.Set("Content-Type", tc.media)
			_, status := parseGatewayProvisioning(req, tc.limit)
			if status != tc.status {
				t.Fatalf("status=%d want=%d", status, tc.status)
			}
		})
	}
}
