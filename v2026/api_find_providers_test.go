package connect

import (
	"encoding/json"
	"testing"
)

// ForceMinimum is a deliberate bootstrap mode: a newly connected provider has
// no latency or speed history until measurement traffic can reach it. Pin its
// wire name so API clients cannot silently fall back into that cold-start
// discovery deadlock.
func TestFindProviders2ArgsEncodesForceMinimum(t *testing.T) {
	body, err := json.Marshal(FindProviders2Args{ForceMinimum: true})
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded["force_minimum"] != true {
		t.Fatalf("force_minimum = %#v, want true", decoded["force_minimum"])
	}
}
