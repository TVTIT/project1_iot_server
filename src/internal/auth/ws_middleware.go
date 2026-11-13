package auth

import (
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"iot-platform/internal/httpapi"
)

const (
	// WSTelemetryProtocol is the versioned application protocol for telemetry WS.
	WSTelemetryProtocol  = "iot.telemetry.v1"
	maxWSCredentialToken = 8 * 1024
	maxWSCredentialInput = 16 * 1024
)

// wsCredential extracts a credential only from the two P7 WebSocket transports.
// It deliberately does not inspect query parameters or identity headers.
func wsCredential(headers http.Header) (string, bool) {
	authorization := headers.Values("Authorization")
	protocolFields := headers.Values("Sec-WebSocket-Protocol")
	total := 0
	for _, values := range [][]string{authorization, protocolFields} {
		for _, value := range values {
			total += len(value)
			if total > maxWSCredentialInput || hasControl(value) {
				return "", false
			}
		}
	}
	var authToken string
	if len(authorization) != 0 {
		if len(authorization) != 1 || strings.Contains(authorization[0], ",") {
			return "", false
		}
		value := authorization[0]
		scheme, token, ok := strings.Cut(value, " ")
		if !ok || !strings.EqualFold(scheme, "Bearer") || token == "" || strings.ContainsAny(token, " \t") || len(token) > maxWSCredentialToken {
			return "", false
		}
		authToken = token
	}
	protocols := make([]string, 0, 2)
	for _, field := range protocolFields {
		for _, item := range strings.Split(field, ",") {
			// HTTP list syntax permits surrounding SP; controls were rejected above.
			item = strings.Trim(item, " ")
			if item == "" || strings.ContainsAny(item, " \t") {
				return "", false
			}
			protocols = append(protocols, item)
		}
	}
	appCount, bearerCount := 0, 0
	var protocolToken string
	for _, protocol := range protocols {
		switch {
		case protocol == WSTelemetryProtocol:
			appCount++
		case strings.HasPrefix(protocol, "bearer."):
			bearerCount++
			protocolToken = strings.TrimPrefix(protocol, "bearer.")
			if protocolToken == "" || len(protocolToken) > maxWSCredentialToken || !validCredentialToken(protocolToken) {
				return "", false
			}
		default:
			return "", false
		}
	}
	if appCount != 1 || bearerCount > 1 || (authToken != "" && bearerCount != 0) {
		return "", false
	}
	if authToken != "" {
		return authToken, true
	}
	if bearerCount == 1 {
		return protocolToken, true
	}
	return "", false
}

func hasControl(value string) bool {
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			return true
		}
	}
	return false
}

func validCredentialToken(value string) bool {
	for _, r := range value {
		allowed := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || strings.ContainsRune("-._~", r)
		if !allowed {
			return false
		}
	}
	return true
}

// WSAuthenticationMiddleware is intentionally distinct from REST authentication:
// it accepts either native Bearer auth or a browser bearer subprotocol.
func WSAuthenticationMiddleware(verifier Verifier, observers ...*Metrics) gin.HandlerFunc {
	var observer *Metrics
	if len(observers) != 0 {
		observer = observers[0]
	}
	return func(c *gin.Context) {
		token, ok := wsCredential(c.Request.Header)
		if !ok {
			if observer != nil {
				observer.WebSocketDenied()
			}
			httpapi.WriteError(c, http.StatusUnauthorized, "unauthorized", "authentication required")
			return
		}
		started := time.Time{}
		if observer != nil {
			started = time.Now()
		}
		principal, err := verifier.Verify(c.Request.Context(), token)
		if observer != nil {
			observer.verify.Observe(time.Since(started))
		}
		if err != nil {
			if observer != nil {
				observer.WebSocketDenied()
			}
			httpapi.WriteError(c, http.StatusUnauthorized, "unauthorized", "authentication required")
			return
		}
		if observer != nil {
			observer.allow.Add(1)
		}
		SetPrincipal(c, principal)
		c.Next()
	}
}
