package httpserver

import (
	"bytes"
	"encoding/json"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"

	"iot-platform/internal/gateway"
)

// Validate string escapes before encoding/json can replace malformed Unicode.
func validJSONUnicode(data []byte) bool {
	if !utf8.Valid(data) {
		return false
	}
	for i := 0; i < len(data); i++ {
		if data[i] != '"' {
			continue
		}
		i++
		for ; i < len(data) && data[i] != '"'; i++ {
			if data[i] == 0 {
				return false
			}
			if data[i] != '\\' {
				continue
			}
			i++
			if i >= len(data) {
				return false
			}
			if data[i] != 'u' {
				continue
			}
			if i+4 >= len(data) {
				return false
			}
			n, err := strconv.ParseUint(string(data[i+1:i+5]), 16, 16)
			if err != nil || n == 0 {
				return false
			}
			i += 4
			if n >= 0xDC00 && n <= 0xDFFF {
				return false
			}
			if n >= 0xD800 && n <= 0xDBFF {
				if i+6 >= len(data) || string(data[i+1:i+3]) != "\\u" {
					return false
				}
				low, err := strconv.ParseUint(string(data[i+3:i+7]), 16, 16)
				if err != nil || low < 0xDC00 || low > 0xDFFF {
					return false
				}
				i += 6
			}
		}
	}
	return true
}

func parseGatewayProvisioning(r *http.Request, limit int64) (gateway.ProvisionGatewayInput, int) {
	var input gateway.ProvisionGatewayInput
	var owner string
	status := parseProvisioningObject(r, limit, map[string]any{"name": &input.Name, "description": &input.Description, "owner_user_id": &owner}, []string{"name", "owner_user_id"})
	if status != 0 {
		return input, status
	}
	var err error
	input.OwnerUserID, err = uuid.Parse(owner)
	if err != nil {
		return input, 400
	}
	return input, 0
}

func parseSensorProvisioning(r *http.Request, limit int64) (gateway.ProvisionSensorInput, int) {
	var input gateway.ProvisionSensorInput
	status := parseProvisioningObject(r, limit, map[string]any{"name": &input.Name, "unit": &input.Unit}, []string{"name"})
	return input, status
}

func parseProvisioningObject(r *http.Request, limit int64, fields map[string]any, required []string) int {
	if r.URL.RawQuery != "" || r.URL.ForceQuery {
		return 400
	}
	media, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || media != "application/json" {
		return 415
	}
	for key, value := range params {
		if key != "charset" || !strings.EqualFold(value, "utf-8") {
			return 415
		}
	}
	data, err := io.ReadAll(io.LimitReader(r.Body, limit+1))
	if int64(len(data)) > limit {
		return 413
	}
	if err != nil || !validJSONUnicode(data) {
		return 400
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	token, err := dec.Token()
	if err != nil || token != json.Delim('{') {
		return 400
	}
	seen := map[string]bool{}
	for dec.More() {
		token, err = dec.Token()
		if err != nil {
			return 400
		}
		key, ok := token.(string)
		if !ok || seen[key] {
			return 400
		}
		seen[key] = true
		var raw json.RawMessage
		if dec.Decode(&raw) != nil {
			return 400
		}
		target, allowed := fields[key]
		if !allowed {
			return 400
		}
		if _, nonnullable := target.(*string); nonnullable && bytes.Equal(raw, []byte("null")) {
			return 400
		}
		if json.Unmarshal(raw, target) != nil {
			return 400
		}
	}
	if token, err = dec.Token(); err != nil || token != json.Delim('}') {
		return 400
	}
	if _, err = dec.Token(); err != io.EOF {
		return 400
	}
	for _, key := range required {
		if !seen[key] {
			return 400
		}
	}
	return 0
}
