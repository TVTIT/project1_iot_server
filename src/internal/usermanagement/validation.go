package usermanagement

import (
	"fmt"
	"net/mail"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"
)

// ValidateCreateUserInput checks field bounds and formatting for account creation.
// Outside whitespace in email is trimmed while preserving exact case.
func ValidateCreateUserInput(input *CreateUserInput) error {
	if input == nil {
		return fmt.Errorf("%w: input cannot be nil", ErrInvalidInput)
	}

	trimmedName := strings.TrimSpace(input.FullName)
	if trimmedName == "" {
		return fmt.Errorf("%w: full_name cannot be blank", ErrInvalidInput)
	}
	if len(input.FullName) > 256 || !utf8.ValidString(input.FullName) {
		return fmt.Errorf("%w: full_name must be valid UTF-8 up to 256 bytes", ErrInvalidInput)
	}

	normalizedEmail, err := validateAndNormalizeEmail(input.Email)
	if err != nil {
		return err
	}
	input.Email = normalizedEmail

	if input.Password == "" {
		return fmt.Errorf("%w: password cannot be blank", ErrInvalidInput)
	}
	if utf8.RuneCountInString(input.Password) < 6 {
		return fmt.Errorf("%w: password must be at least 6 characters", ErrInvalidInput)
	}
	if len(input.Password) > 72 {
		return fmt.Errorf("%w: password cannot exceed 72 bytes", ErrInvalidInput)
	}
	if !utf8.ValidString(input.Password) {
		return fmt.Errorf("%w: password must be valid UTF-8", ErrInvalidInput)
	}

	return nil
}

func validateAndNormalizeEmail(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", fmt.Errorf("%w: email cannot be blank", ErrInvalidInput)
	}
	if len(trimmed) > 254 {
		return "", fmt.Errorf("%w: email exceeds maximum length", ErrInvalidInput)
	}
	if strings.ContainsAny(trimmed, " \t\r\n<>()[],;:\"") {
		return "", fmt.Errorf("%w: email contains invalid characters, whitespace, or display name", ErrInvalidInput)
	}
	parts := strings.Split(trimmed, "@")
	if len(parts) != 2 {
		return "", fmt.Errorf("%w: email must contain exactly one @ symbol", ErrInvalidInput)
	}
	localPart, domain := parts[0], parts[1]
	if localPart == "" || len(localPart) > 64 {
		return "", fmt.Errorf("%w: invalid local part", ErrInvalidInput)
	}
	if domain == "" || strings.HasPrefix(domain, ".") || strings.HasSuffix(domain, ".") || strings.Contains(domain, "..") || !strings.Contains(domain, ".") {
		return "", fmt.Errorf("%w: invalid domain part", ErrInvalidInput)
	}
	parsedAddr, err := mail.ParseAddress(trimmed)
	if err != nil || parsedAddr.Address != trimmed {
		return "", fmt.Errorf("%w: invalid email address format", ErrInvalidInput)
	}
	return trimmed, nil
}

// ValidateUpdateUserInput checks field bounds for PATCH updates.
func ValidateUpdateUserInput(input UpdateUserInput) error {
	if input.FullName == nil && input.PlatformRole == nil {
		return fmt.Errorf("%w: at least one field must be provided", ErrInvalidInput)
	}

	if input.FullName != nil {
		trimmed := strings.TrimSpace(*input.FullName)
		if trimmed == "" {
			return fmt.Errorf("%w: full_name cannot be blank", ErrInvalidInput)
		}
		if len(*input.FullName) > 256 || !utf8.ValidString(*input.FullName) {
			return fmt.Errorf("%w: full_name must be valid UTF-8 up to 256 bytes", ErrInvalidInput)
		}
	}

	if input.PlatformRole != nil {
		role := *input.PlatformRole
		if role != "platform_admin" && role != "user" {
			return fmt.Errorf("%w: platform_role must be 'platform_admin' or 'user'", ErrInvalidInput)
		}
	}

	return nil
}

// CheckSelfDemotion ensures a platform admin cannot revoke their own admin role.
func CheckSelfDemotion(actorID, targetID uuid.UUID, input UpdateUserInput) error {
	if actorID == targetID && input.PlatformRole != nil && *input.PlatformRole == "user" {
		return ErrSelfDemotionForbidden
	}
	return nil
}

// ValidateGatewayRole validates role for PUT /gateways/:gateway_id/users/:user_id.
func ValidateGatewayRole(role string) error {
	if role == "owner" {
		return ErrOwnerRoleNotAllowed
	}
	if role == "operator" || role == "viewer" {
		return nil
	}
	return ErrInvalidRole
}
