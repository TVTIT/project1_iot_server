// Package auth verifies human-user identity and carries authenticated principals.
package auth

import (
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

const principalContextKey = "authenticated_principal"

// Principal is the minimal verified human identity passed to application code.
type Principal struct {
	UserID uuid.UUID
	Role   string
	// ExpiresAt is the verified JWT exp instant in UTC, without applying
	// verifier clock skew. Session deadlines must close at this instant; skew
	// permits request verification only and does not extend a session.
	ExpiresAt time.Time
}

// SetPrincipal stores a verified principal without retaining the raw token.
func SetPrincipal(c *gin.Context, principal Principal) {
	c.Set(principalContextKey, principal)
}

// PrincipalFrom returns the verified principal associated with the request.
func PrincipalFrom(c *gin.Context) (Principal, bool) {
	value, ok := c.Get(principalContextKey)
	if !ok {
		return Principal{}, false
	}
	principal, ok := value.(Principal)
	return principal, ok
}
