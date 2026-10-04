package httpserver

import (
	"testing"
	"time"
)

func TestEnabledCredentialDependenciesRequired(t *testing.T) {
	for _, timeout := range []time.Duration{0, -time.Second, 6 * time.Minute} {
		d, _, _ := credentialDeps(t, &credentialSpy{})
		d.CredentialRequestTimeout = timeout
		if _, err := NewRouter(d); err == nil {
			t.Fatal("unbounded credential request accepted")
		}
	}
	for _, typed := range []bool{false, true} {
		d, _, _ := credentialDeps(t, &credentialSpy{})
		if typed {
			var p *credentialSpy
			d.CredentialManager = p
		} else {
			d.CredentialManager = nil
		}
		if _, e := NewRouter(d); e == nil {
			t.Fatal("enabled router accepted missing manager")
		}
		d, _, _ = credentialDeps(t, &credentialSpy{})
		if typed {
			var p *nilCredentialReadiness
			d.CredentialStartupReadiness = p
		} else {
			d.CredentialStartupReadiness = nil
		}
		if _, e := NewRouter(d); e == nil {
			t.Fatal("enabled router accepted missing barrier")
		}
		d.CredentialAPIEnabled = false
		if _, e := NewRouter(d); e != nil {
			t.Fatal("disabled router", e)
		}
	}
}
