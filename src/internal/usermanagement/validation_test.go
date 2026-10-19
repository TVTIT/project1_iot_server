package usermanagement_test

import (
	"strings"
	"testing"

	"github.com/google/uuid"

	"iot-platform/internal/usermanagement"
)

func TestValidateCreateUserInput(t *testing.T) {
	valid := usermanagement.CreateUserInput{
		FullName: "Nguyen Van A",
		Email:    "user@example.com",
		Password: "SecurePassword123",
	}

	tests := []struct {
		name    string
		mutate  func(*usermanagement.CreateUserInput)
		wantErr bool
	}{
		{
			name:   "valid input",
			mutate: func(_ *usermanagement.CreateUserInput) {},
		},
		{
			name: "empty full name",
			mutate: func(in *usermanagement.CreateUserInput) {
				in.FullName = ""
			},
			wantErr: true,
		},
		{
			name: "whitespace only full name",
			mutate: func(in *usermanagement.CreateUserInput) {
				in.FullName = "   \t\n  "
			},
			wantErr: true,
		},
		{
			name: "full name exceeds 256 bytes",
			mutate: func(in *usermanagement.CreateUserInput) {
				in.FullName = strings.Repeat("A", 257)
			},
			wantErr: true,
		},
		{
			name: "full name exact 256 bytes",
			mutate: func(in *usermanagement.CreateUserInput) {
				in.FullName = strings.Repeat("A", 256)
			},
			wantErr: false,
		},
		{
			name: "empty email",
			mutate: func(in *usermanagement.CreateUserInput) {
				in.Email = ""
			},
			wantErr: true,
		},
		{
			name: "email missing at symbol",
			mutate: func(in *usermanagement.CreateUserInput) {
				in.Email = "invalid-email"
			},
			wantErr: true,
		},
		{
			name: "email with header newline injection",
			mutate: func(in *usermanagement.CreateUserInput) {
				in.Email = "user@example.com\nBcc: evil@example.com"
			},
			wantErr: true,
		},
		{
			name: "email with display name",
			mutate: func(in *usermanagement.CreateUserInput) {
				in.Email = "Nguyen Van A <user@example.com>"
			},
			wantErr: true,
		},
		{
			name: "email with trailing comment",
			mutate: func(in *usermanagement.CreateUserInput) {
				in.Email = "user@example.com (Nguyen Van A)"
			},
			wantErr: true,
		},
		{
			name: "multiple email addresses separated by comma",
			mutate: func(in *usermanagement.CreateUserInput) {
				in.Email = "user1@example.com, user2@example.com"
			},
			wantErr: true,
		},
		{
			name: "email with internal space",
			mutate: func(in *usermanagement.CreateUserInput) {
				in.Email = "user @example.com"
			},
			wantErr: true,
		},
		{
			name: "email with missing local part",
			mutate: func(in *usermanagement.CreateUserInput) {
				in.Email = "@example.com"
			},
			wantErr: true,
		},
		{
			name: "email with missing domain",
			mutate: func(in *usermanagement.CreateUserInput) {
				in.Email = "user@"
			},
			wantErr: true,
		},
		{
			name: "email with malformed domain missing dot",
			mutate: func(in *usermanagement.CreateUserInput) {
				in.Email = "user@localhost"
			},
			wantErr: true,
		},
		{
			name: "email with consecutive dots in domain",
			mutate: func(in *usermanagement.CreateUserInput) {
				in.Email = "user@domain..com"
			},
			wantErr: true,
		},
		{
			name: "outside whitespace in email accepted and normalized",
			mutate: func(in *usermanagement.CreateUserInput) {
				in.Email = "  User.Name@Example.COM  "
			},
			wantErr: false,
		},
		{
			name: "email exceeds 254 bytes",
			mutate: func(in *usermanagement.CreateUserInput) {
				in.Email = strings.Repeat("a", 243) + "@example.com"
			},
			wantErr: true,
		},
		{
			name: "empty password",
			mutate: func(in *usermanagement.CreateUserInput) {
				in.Password = ""
			},
			wantErr: true,
		},
		{
			name: "password below 6 characters",
			mutate: func(in *usermanagement.CreateUserInput) {
				in.Password = "12345"
			},
			wantErr: true,
		},
		{
			name: "password exact 6 characters",
			mutate: func(in *usermanagement.CreateUserInput) {
				in.Password = "123456"
			},
			wantErr: false,
		},
		{
			name: "password exceeds 72 bytes",
			mutate: func(in *usermanagement.CreateUserInput) {
				in.Password = strings.Repeat("p", 73)
			},
			wantErr: true,
		},
		{
			name: "password exact 72 bytes",
			mutate: func(in *usermanagement.CreateUserInput) {
				in.Password = strings.Repeat("p", 72)
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := valid
			tt.mutate(&input)
			err := usermanagement.ValidateCreateUserInput(&input)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ValidateCreateUserInput() error = %v, wantErr = %v", err, tt.wantErr)
			}
			if !tt.wantErr && strings.Contains(tt.name, "outside whitespace") {
				if input.Email != "User.Name@Example.COM" {
					t.Fatalf("Email = %q, want %q (outside whitespace trimmed, case preserved)", input.Email, "User.Name@Example.COM")
				}
			}
		})
	}
}

func TestValidateUpdateUserInput(t *testing.T) {
	name := "New Name"
	roleAdmin := "platform_admin"
	roleUser := "user"
	invalidRole := "superuser"

	tests := []struct {
		name    string
		input   usermanagement.UpdateUserInput
		wantErr bool
	}{
		{
			name:    "update full name only",
			input:   usermanagement.UpdateUserInput{FullName: &name},
			wantErr: false,
		},
		{
			name:    "update platform role to platform_admin",
			input:   usermanagement.UpdateUserInput{PlatformRole: &roleAdmin},
			wantErr: false,
		},
		{
			name:    "update platform role to user",
			input:   usermanagement.UpdateUserInput{PlatformRole: &roleUser},
			wantErr: false,
		},
		{
			name:    "update both full name and role",
			input:   usermanagement.UpdateUserInput{FullName: &name, PlatformRole: &roleAdmin},
			wantErr: false,
		},
		{
			name:    "no fields present",
			input:   usermanagement.UpdateUserInput{},
			wantErr: true,
		},
		{
			name: "empty full name",
			input: func() usermanagement.UpdateUserInput {
				empty := ""
				return usermanagement.UpdateUserInput{FullName: &empty}
			}(),
			wantErr: true,
		},
		{
			name: "whitespace full name",
			input: func() usermanagement.UpdateUserInput {
				ws := "   "
				return usermanagement.UpdateUserInput{FullName: &ws}
			}(),
			wantErr: true,
		},
		{
			name: "full name exceeds 256 bytes",
			input: func() usermanagement.UpdateUserInput {
				long := strings.Repeat("x", 257)
				return usermanagement.UpdateUserInput{FullName: &long}
			}(),
			wantErr: true,
		},
		{
			name:    "invalid platform role",
			input:   usermanagement.UpdateUserInput{PlatformRole: &invalidRole},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := usermanagement.ValidateUpdateUserInput(tt.input)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ValidateUpdateUserInput() error = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}
}

func TestCheckSelfDemotion(t *testing.T) {
	actorID := uuid.New()
	otherID := uuid.New()
	roleUser := "user"
	roleAdmin := "platform_admin"
	name := "Updated Name"

	tests := []struct {
		name     string
		actorID  uuid.UUID
		targetID uuid.UUID
		input    usermanagement.UpdateUserInput
		wantErr  bool
	}{
		{
			name:     "actor demoting self to user is forbidden",
			actorID:  actorID,
			targetID: actorID,
			input:    usermanagement.UpdateUserInput{PlatformRole: &roleUser},
			wantErr:  true,
		},
		{
			name:     "actor combined rename and demoting self to user is forbidden",
			actorID:  actorID,
			targetID: actorID,
			input:    usermanagement.UpdateUserInput{FullName: &name, PlatformRole: &roleUser},
			wantErr:  true,
		},
		{
			name:     "actor demoting another admin to user is allowed",
			actorID:  actorID,
			targetID: otherID,
			input:    usermanagement.UpdateUserInput{PlatformRole: &roleUser},
			wantErr:  false,
		},
		{
			name:     "actor setting self platform_admin is allowed (no-op)",
			actorID:  actorID,
			targetID: actorID,
			input:    usermanagement.UpdateUserInput{PlatformRole: &roleAdmin},
			wantErr:  false,
		},
		{
			name:     "actor renaming self is allowed",
			actorID:  actorID,
			targetID: actorID,
			input:    usermanagement.UpdateUserInput{FullName: &name},
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := usermanagement.CheckSelfDemotion(tt.actorID, tt.targetID, tt.input)
			if (err != nil) != tt.wantErr {
				t.Fatalf("CheckSelfDemotion() error = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateAssignGatewayRole(t *testing.T) {
	tests := []struct {
		name    string
		role    string
		wantErr bool
		isOwner bool
	}{
		{name: "operator allowed", role: "operator", wantErr: false},
		{name: "viewer allowed", role: "viewer", wantErr: false},
		{name: "owner rejected as invalid role for assign", role: "owner", wantErr: true, isOwner: true},
		{name: "admin rejected", role: "admin", wantErr: true},
		{name: "empty rejected", role: "", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := usermanagement.ValidateGatewayRole(tt.role)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ValidateGatewayRole(%q) error = %v, wantErr = %v", tt.role, err, tt.wantErr)
			}
			if tt.isOwner && err != usermanagement.ErrOwnerRoleNotAllowed {
				t.Fatalf("expected ErrOwnerRoleNotAllowed, got %v", err)
			}
		})
	}
}
