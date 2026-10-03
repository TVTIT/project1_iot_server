package main

import "testing"

func TestSnapshotContracts(t *testing.T) {
	for _, raw := range []string{
		`{"clients":[{"username":"A","disabled":true,"disabled":false}]}`,
		`{"clients":[{"username":"A"},{"username":"A"}]}`,
		`{"clients":[]}`, `{"clients":[{"username":"A","disabled":"true"}]}`,
		`{"clients":[{"username":"A"}]} {}`, `null`,
	} {
		if _, err := disabled([]byte(raw), "A"); err == nil {
			t.Fatal("accepted invalid snapshot")
		}
	}
	value, err := disabled([]byte(`{"clients":[{"username":"A","disabled":true}]}`), "A")
	if err != nil || !value {
		t.Fatal("valid snapshot rejected")
	}
	if _, err := disabled([]byte(`{}`), "../security"); err == nil {
		t.Fatal("path target accepted")
	}
	if _, err := disabled(make([]byte, (1<<20)+1), "A"); err == nil {
		t.Fatal("oversize accepted")
	}
}
