// Package auth verifies human-user identity and carries authenticated principals.
package auth

import "github.com/google/uuid"

// Principal is the minimal verified human identity passed to application code.
type Principal struct {
	UserID uuid.UUID
	Role   string
}
