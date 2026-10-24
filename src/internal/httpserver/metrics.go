package httpserver

import (
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"iot-platform/internal/metrics"
)

// Fixed templates, never request paths. Setup/scrape/health are not business load.
var metricRoutes = [...]string{
	"unmatched", "GET /healthz", "GET /readyz", "GET /v1/health",
	"GET /v1/admin/metrics", "GET /v1/admin/gateways", "PUT /v1/admin/gateways/:gateway_id",
	"PUT /v1/admin/gateways/:gateway_id/sensors/:sensor_id", "POST /v1/admin/users", "GET /v1/admin/users",
	"PATCH /v1/admin/users/:user_id", "PUT /v1/admin/gateways/:gateway_id/users/:user_id",
	"DELETE /v1/admin/gateways/:gateway_id/users/:user_id", "PUT /v1/admin/gateways/:gateway_id/owner",
	"POST /v1/admin/gateways/:gateway_id/mqtt-credential", "POST /v1/admin/gateways/:gateway_id/mqtt-credential/rotate",
	"POST /v1/admin/gateways/:gateway_id/mqtt-credential/reset",
	"DELETE /v1/admin/gateways/:gateway_id/mqtt-credential", "GET /v1/admin/gateways/:gateway_id/mqtt-credential",
	"GET /v1/me/permissions", "GET /v1/gateways", "GET /v1/gateways/:gateway_id/sensors",
	"PUT /v1/gateways/:gateway_id/sensors/:sensor_id", "GET /v1/ws", "GET /v1/digital-twins", "POST /v1/digital-twins",
	"GET /v1/digital-twins/:entity_id", "PATCH /v1/digital-twins/:entity_id/attrs",
	"GET /v1/digital-twins/:entity_id/temporal", "PATCH /v1/digital-twins/:entity_id/desired-state",
	"GET /v1/digital-twins/:entity_id/commands", "GET /v1/digital-twins/:entity_id/commands/:command_id",
}

// HTTPMetrics has a compile-time bound independent of request data.
type HTTPMetrics struct {
	durations [len(metricRoutes)]metrics.Histogram
}

// NewHTTPMetrics creates an empty observer, installed only when enabled.
func NewHTTPMetrics() *HTTPMetrics { return &HTTPMetrics{} }

// HTTPMetricSnapshot identifies one fixed route and its scope.
type HTTPMetricSnapshot struct {
	Route    string                    `json:"route"`
	Scope    string                    `json:"scope"`
	Duration metrics.HistogramSnapshot `json:"duration"`
}

func metricScope(route string) string {
	switch {
	case route == "GET /v1/admin/metrics":
		return "scrape"
	case route == "GET /healthz" || route == "GET /readyz" || route == "GET /v1/health":
		return "health"
	case strings.Contains(route, "/admin/") || route == "POST /v1/digital-twins" || route == "PUT /v1/gateways/:gateway_id/sensors/:sensor_id":
		return "setup"
	default:
		return "business"
	}
}

// Snapshot copies all fixed series, including routes not yet called.
func (m *HTTPMetrics) Snapshot() []HTTPMetricSnapshot {
	out := make([]HTTPMetricSnapshot, len(metricRoutes))
	for i, route := range metricRoutes {
		out[i] = HTTPMetricSnapshot{route, metricScope(route), m.durations[i].Snapshot()}
	}
	return out
}

func (m *HTTPMetrics) middleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		key, index := c.Request.Method+" "+c.FullPath(), 0
		for i, route := range metricRoutes {
			if key == route {
				index = i
				break
			}
		}
		m.durations[index].Observe(time.Since(start))
	}
}
