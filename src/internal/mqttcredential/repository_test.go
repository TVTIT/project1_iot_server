package mqttcredential

import (
	"reflect"
	"strings"
	"testing"
)

// This is a contract test only, not evidence of transaction/authorization safety
// in PostgreSQL. Task 2.6.3 must test an actual implementation under contention.
func TestRepositoryContractIsDriverAndSecretFree(t *testing.T) {
	typ := reflect.TypeOf((*Repository)(nil)).Elem()
	for _, name := range []string{"IsPlatformAdmin", "BeginOperation", "ConditionalFinalize", "FailKnown", "RequireRecovery", "GetMetadata", "ResolveCommitAmbiguity", "ListUnresolved", "ListDurableRevocations"} {
		method, ok := typ.MethodByName(name)
		if !ok || method.Type.In(0).String() != "context.Context" {
			t.Fatalf("missing context method %s", name)
		}
		if strings.Contains(method.Type.String(), "pgx") || strings.Contains(method.Type.String(), "SecretResult") {
			t.Fatal("driver/secret in contract")
		}
	}
	for _, typ := range []reflect.Type{reflect.TypeOf(BeginRequest{}), reflect.TypeOf(OperationGuard{}), reflect.TypeOf(OperationUpdate{}), reflect.TypeOf(RevocationDecision{})} {
		for _, forbidden := range []string{"Password", "Hash", "Token", "JWT", "Transaction"} {
			if _, ok := typ.FieldByName(forbidden); ok {
				t.Fatalf("unsafe field %s", forbidden)
			}
		}
	}
}
