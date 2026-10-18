package httpserver

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

// stage4CacheMiddleware ensures Cache-Control: no-store on all Stage 4 endpoints,
// and Pragma: no-cache on mutation endpoints, before CORS, auth, and routing dispatch.
func stage4CacheMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		path := c.Request.URL.Path
		if isStage4Route(path) {
			c.Header("Cache-Control", "no-store")
			if isStage4Mutation(path, c.Request.Method, c.Request.Header.Get("Access-Control-Request-Method")) {
				c.Header("Pragma", "no-cache")
			}
		}
		c.Next()
	}
}

// isStage4Route matches exactly the seven Stage 4 endpoints by path:
// - /v1/telemetry/history
// - /v1/digital-twins
// - /v1/digital-twins/:entity_id
// - /v1/digital-twins/:entity_id/state
// - /v1/digital-twins/:entity_id/history
// It does NOT match future routes like /v1/digital-twins/:entity_id/desired-state,
// /v1/digital-twins/:entity_id/commands, /v1/ws, or Stage 2 routes.
func isStage4Route(path string) bool {
	p := strings.TrimSuffix(path, "/")
	if p == "/v1/telemetry/history" || p == "/v1/digital-twins" {
		return true
	}
	parts := strings.Split(p, "/")
	// Expected parts for /v1/digital-twins/<entity_id>:
	// ["", "v1", "digital-twins", <entity_id>] -> len == 4
	if len(parts) == 4 && parts[1] == "v1" && parts[2] == "digital-twins" && parts[3] != "" {
		return true
	}
	// Expected parts for /v1/digital-twins/<entity_id>/{state|history}:
	// ["", "v1", "digital-twins", <entity_id>, "state"] -> len == 5
	// ["", "v1", "digital-twins", <entity_id>, "history"] -> len == 5
	if len(parts) == 5 && parts[1] == "v1" && parts[2] == "digital-twins" && parts[3] != "" {
		if parts[4] == "state" || parts[4] == "history" {
			return true
		}
	}
	return false
}

// isStage4Mutation determines whether a request targeting a Stage 4 route represents
// a mutation (POST /v1/digital-twins, PATCH /v1/digital-twins/:entity_id, or non-safe method like 405, or CORS preflight requesting them).
func isStage4Mutation(path string, method string, reqMethod string) bool {
	effMethod := method
	if method == http.MethodOptions && reqMethod != "" {
		effMethod = reqMethod
	}
	p := strings.TrimSuffix(path, "/")
	// /v1/digital-twins: mutation if non-GET/HEAD/OPTIONS
	if p == "/v1/digital-twins" {
		return effMethod != http.MethodGet && effMethod != http.MethodHead && effMethod != http.MethodOptions
	}
	parts := strings.Split(p, "/")
	// /v1/digital-twins/:entity_id: mutation if non-GET/HEAD/OPTIONS
	if len(parts) == 4 && parts[1] == "v1" && parts[2] == "digital-twins" && parts[3] != "" {
		return effMethod != http.MethodGet && effMethod != http.MethodHead && effMethod != http.MethodOptions
	}
	return false
}
