package auth

import (
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"iot-platform/internal/httpapi"
)

// AuthenticationMiddleware requires one strict Bearer credential and stores
// only the resulting typed principal in the request context.
func AuthenticationMiddleware(verifier Verifier, observers ...*Metrics) gin.HandlerFunc {
	var observer *Metrics
	if len(observers) > 0 {
		observer = observers[0]
	}
	return func(c *gin.Context) {
		rawToken, ok := bearerToken(c.Request.Header.Values("Authorization"))
		if !ok {
			if observer != nil {
				observer.deny.Add(1)
			}
			httpapi.WriteError(c, http.StatusUnauthorized, "unauthorized", "authentication required")
			return
		}

		var started time.Time
		if observer != nil {
			started = time.Now()
		}
		principal, err := verifier.Verify(c.Request.Context(), rawToken)
		if observer != nil {
			observer.verify.Observe(time.Since(started))
		}
		if err != nil {
			if observer != nil {
				observer.deny.Add(1)
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

func bearerToken(values []string) (string, bool) {
	if len(values) != 1 || strings.Contains(values[0], ",") {
		return "", false
	}
	scheme, credentials, ok := strings.Cut(values[0], " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return "", false
	}
	if credentials == "" || strings.ContainsAny(credentials, " \t\r\n") {
		return "", false
	}
	return credentials, true
}
