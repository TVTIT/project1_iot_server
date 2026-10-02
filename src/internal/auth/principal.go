// Package auth verifies human-user identity and carries authenticated principals.
package auth

import (
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

const principalContextKey = "authenticated_principal"

// Principal is the minimal verified human identity passed to application code.
type Principal struct {
	UserID uuid.UUID
	Role   string
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
