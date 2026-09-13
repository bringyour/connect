package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/urnetwork/connect"
)

// The generator refuses to write the empty resource by accident (A10).
//
// A run with no input used to write the empty list over the shipped one, which
// nothing downstream reports: the build succeeds and every client quietly
// stops fronting its extender dials. The refusal is the only thing standing
// between a mistyped flag and that.

// Nothing is written without an input, and an existing resource is left alone.
func TestWriteSpoofResourceRefusesWithoutAnInput(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "extender_spoof.bin")
	bundled := []byte("the shipped resource")
	if err := os.WriteFile(outPath, bundled, 0o644); err != nil {
		t.Fatal(err)
	}

	if err := writeSpoofResource("", outPath, false); err == nil {
		t.Fatal("a run with no input was accepted")
	}
	// -in and -empty together is a caller that means two different things
	if err := writeSpoofResource("domains.txt", outPath, true); err == nil {
		t.Fatal("-in with -empty was accepted")
	}

	resource, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(resource) != string(bundled) {
		t.Fatalf("the refused run wrote %q", resource)
	}
}

// An input with no domains in it is the same accident by another route, and is
// refused the same way; `-empty` is how an empty resource is asked for.
func TestWriteSpoofResourceWritesTheListAndTheExplicitEmpty(t *testing.T) {
	dir := t.TempDir()
	outPath := filepath.Join(dir, "extender_spoof.bin")

	inPath := filepath.Join(dir, "empty.txt")
	if err := os.WriteFile(inPath, []byte("# only a comment\n\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := writeSpoofResource(inPath, outPath, false); err == nil {
		t.Fatal("an input with no domains was accepted")
	}
	if _, err := os.Stat(outPath); err == nil {
		t.Fatal("the refused run wrote a resource")
	}

	inPath = filepath.Join(dir, "domains.txt")
	plainText := "one.example\n# a comment\nTwo.Example\none.example\n"
	if err := os.WriteFile(inPath, []byte(plainText), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := writeSpoofResource(inPath, outPath, false); err != nil {
		t.Fatal(err)
	}
	resource, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatal(err)
	}
	spoofDomains, err := connect.DecodeSpoofDomainsResource(resource)
	if err != nil {
		t.Fatal(err)
	}
	expected := []string{"one.example", "two.example"}
	if len(spoofDomains) != len(expected) {
		t.Fatalf("wrote %v, expected %v", spoofDomains, expected)
	}
	for i, spoofDomain := range spoofDomains {
		if spoofDomain != expected[i] {
			t.Fatalf("wrote %v, expected %v", spoofDomains, expected)
		}
	}

	// the empty resource is still reachable, on purpose
	if err := writeSpoofResource("", outPath, true); err != nil {
		t.Fatal(err)
	}
	if resource, err = os.ReadFile(outPath); err != nil {
		t.Fatal(err)
	}
	if spoofDomains, err = connect.DecodeSpoofDomainsResource(resource); err != nil {
		t.Fatal(err)
	}
	if len(spoofDomains) != 0 {
		t.Fatalf("-empty wrote %v", spoofDomains)
	}
}

// A missing input file fails rather than falling back to the empty list.
func TestWriteSpoofResourceRefusesAMissingInput(t *testing.T) {
	dir := t.TempDir()
	outPath := filepath.Join(dir, "extender_spoof.bin")
	if err := writeSpoofResource(filepath.Join(dir, "absent.txt"), outPath, false); err == nil {
		t.Fatal("a missing input was accepted")
	}
	if _, err := os.Stat(outPath); err == nil {
		t.Fatal("the refused run wrote a resource")
	}
}
