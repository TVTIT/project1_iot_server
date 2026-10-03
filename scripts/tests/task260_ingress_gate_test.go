package main

import "testing"

func TestInventoryFailClosed(t *testing.T) {
	valid := []decision{{Gateway: "A", Revoked: true, Epoch: 1, Operation: "0123456789abcdef0123456789abcdef", Status: "pending"}, {Gateway: "B"}}
	if err := validate(valid); err != nil {
		t.Fatal(err)
	}
	for _, rows := range [][]decision{nil, valid[:1], {valid[0], valid[0]}, {{Gateway: "A", Revoked: true}, {Gateway: "B"}}, {{Gateway: "A", Epoch: -1}, {Gateway: "B"}}} {
		if validate(rows) == nil {
			t.Fatal("invalid authority admitted")
		}
	}
}

func TestReceiptIsLifetimeLocal(t *testing.T) {
	if admit("new", receipt{Nonce: "old", Verified: true}) {
		t.Fatal("stale receipt admitted")
	}
	if admit("new", receipt{Nonce: "new"}) {
		t.Fatal("unverified admitted")
	}
	if !admit("new", receipt{Nonce: "new", Verified: true}) {
		t.Fatal("verified denied")
	}
}
