package main

import (
	"context"
	"strings"
	"testing"
)

func TestStartupRejectsUnknownOrExecutable(t *testing.T) {
	for _, body := range []string{`{}`, `{"extra":"secret"}`, `{"Lifecycle":{"Executable":"/bin/sh"}}`, strings.Repeat("x", 65538)} {
		if run(context.Background(), strings.NewReader(body)) == nil {
			t.Fatal("unsafe startup accepted")
		}
	}
}
