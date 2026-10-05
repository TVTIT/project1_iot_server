package auth

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"iot-platform/internal/httpapi"
)

// AuthenticationMiddleware requires one strict Bearer credential and stores
// only the resulting typed principal in the request context.
func AuthenticationMiddleware(verifier Verifier) gin.HandlerFunc {
	return func(c *gin.Context) {
		rawToken, ok := bearerToken(c.Request.Header.Values("Authorization"))
		if !ok {
			httpapi.WriteError(c, http.StatusUnauthorized, "unauthorized", "authentication required")
			return
		}

		principal, err := verifier.Verify(c.Request.Context(), rawToken)
		if err != nil {
			httpapi.WriteError(c, http.StatusUnauthorized, "unauthorized", "authentication required")
			return
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
