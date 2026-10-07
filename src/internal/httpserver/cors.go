package httpserver

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"iot-platform/internal/httpapi"
)

var allowedPreflightMethods = map[string]bool{
	http.MethodGet:     true,
	http.MethodPost:    true,
	http.MethodPut:     true,
	http.MethodPatch:   true,
	http.MethodDelete:  true,
	http.MethodOptions: true,
}

var allowedPreflightHeaders = map[string]bool{
	"authorization":   true,
	"content-type":    true,
	"idempotency-key": true,
	"x-request-id":    true,
}

const (
	preflightMaxAge       = "600"
	preflightAllowMethods = "GET, POST, PUT, PATCH, DELETE, OPTIONS"
	preflightAllowHeaders = "Authorization, Content-Type, Idempotency-Key, X-Request-ID"
	exposeHeaders         = "X-Request-ID"
)

// corsMiddleware enforces exact-origin CORS access control.
func corsMiddleware(allowedOrigins []string) gin.HandlerFunc {
	allowedMap := make(map[string]bool, len(allowedOrigins))
	for _, origin := range allowedOrigins {
		allowedMap[origin] = true
	}

	return func(c *gin.Context) {
		// Actual responses differ when Origin is present, allowed, or denied.
		// Set this before the Origin branches so caches also separate no-Origin
		// requests from cross-origin requests.
		addVary(c.Writer.Header(), "Origin")

		originValues := c.Request.Header.Values("Origin")
		requestMethodValues := c.Request.Header.Values("Access-Control-Request-Method")
		// A CORS preflight has both required header fields. OPTIONS requests without
		// that shape remain ordinary router requests.
		if c.Request.Method == http.MethodOptions && len(originValues) > 0 && len(requestMethodValues) > 0 {
			handlePreflight(c, originValues, requestMethodValues, allowedMap)
			return
		}

		// Actual requests:
		origin := c.GetHeader("Origin")
		if origin != "" && allowedMap[origin] {
			c.Header("Access-Control-Allow-Origin", origin)
			c.Header("Access-Control-Expose-Headers", exposeHeaders)
		}

		c.Next()
	}
}

func handlePreflight(c *gin.Context, originValues, requestMethodValues []string, allowedMap map[string]bool) {
	// Preflight Vary must always include Origin, Access-Control-Request-Method, and Access-Control-Request-Headers.
	addVary(c.Writer.Header(), "Origin", "Access-Control-Request-Method", "Access-Control-Request-Headers")

	// Origin and Access-Control-Request-Method are singleton fields. Do not use
	// Header.Get: it silently discards duplicate values.
	if len(originValues) != 1 || originValues[0] == "" || strings.Contains(originValues[0], ",") || !allowedMap[originValues[0]] {
		httpapi.WriteError(c, http.StatusForbidden, "forbidden", "cross-origin request denied")
		return
	}
	origin := originValues[0]

	// Validate Access-Control-Request-Method
	if len(requestMethodValues) != 1 || !isHTTPToken(requestMethodValues[0]) || !allowedPreflightMethods[requestMethodValues[0]] {
		httpapi.WriteError(c, http.StatusForbidden, "forbidden", "method not allowed by CORS")
		return
	}

	// Validate Access-Control-Request-Headers
	if requestHeaderValues := c.Request.Header.Values("Access-Control-Request-Headers"); len(requestHeaderValues) > 0 {
		for _, requestHeaders := range requestHeaderValues {
			for _, rawHeader := range strings.Split(requestHeaders, ",") {
				headerName := strings.ToLower(strings.TrimSpace(rawHeader))
				if !isHTTPToken(headerName) || !allowedPreflightHeaders[headerName] {
					httpapi.WriteError(c, http.StatusForbidden, "forbidden", "header not allowed by CORS")
					return
				}
			}
		}
	}

	c.Header("Access-Control-Allow-Origin", origin)
	c.Header("Access-Control-Allow-Methods", preflightAllowMethods)
	c.Header("Access-Control-Allow-Headers", preflightAllowHeaders)
	c.Header("Access-Control-Max-Age", preflightMaxAge)
	c.Header("Access-Control-Expose-Headers", exposeHeaders)
	c.AbortWithStatus(http.StatusNoContent)
}

// isHTTPToken accepts RFC 9110 token characters. HTTP methods and field names
// are tokens; callers retain method case sensitivity and normalize field names
// only where HTTP specifies case-insensitive comparison.
func isHTTPToken(value string) bool {
	if value == "" {
		return false
	}
	for _, character := range []byte(value) {
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') || strings.ContainsRune("!#$%&'*+-.^_`|~", rune(character)) {
			continue
		}
		return false
	}
	return true
}

func addVary(header http.Header, fields ...string) {
	existing := header.Values("Vary")
	seen := make(map[string]bool)
	var items []string

	for _, v := range existing {
		for _, part := range strings.Split(v, ",") {
			trimmed := strings.TrimSpace(part)
			if trimmed == "*" {
				header.Set("Vary", "*")
				return
			}
			if trimmed != "" {
				key := strings.ToLower(trimmed)
				if !seen[key] {
					seen[key] = true
					items = append(items, trimmed)
				}
			}
		}
	}

	for _, field := range fields {
		trimmed := strings.TrimSpace(field)
		if trimmed != "" {
			key := strings.ToLower(trimmed)
			if !seen[key] {
				seen[key] = true
				items = append(items, trimmed)
			}
		}
	}

	if len(items) > 0 {
		header.Set("Vary", strings.Join(items, ", "))
	}
}
