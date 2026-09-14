package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/urnetwork/connect/v2026"
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

// Each refusal names why, so an operator who runs the generator wrong is told
// which flag to fix rather than seeing a generic failure (A10).
func TestWriteSpoofResourceRefusalMessages(t *testing.T) {
	dir := t.TempDir()
	inPath := filepath.Join(dir, "domains.txt")
	if err := os.WriteFile(inPath, []byte("one.example\n"), 0600); err != nil {
		t.Fatal(err)
	}
	emptyPath := filepath.Join(dir, "empty.txt")
	if err := os.WriteFile(emptyPath, []byte("# only a comment\n\n"), 0600); err != nil {
		t.Fatal(err)
	}
	outPath := filepath.Join(dir, "out.bin")

	cases := []struct {
		name    string
		inPath  string
		empty   bool
		message string
	}{
		{
			name:    "no input at all",
			message: "needs -in",
		},
		{
			name:    "both an input and the empty flag",
			inPath:  inPath,
			empty:   true,
			message: "not both",
		},
		{
			name:    "an input that does not exist",
			inPath:  filepath.Join(dir, "missing.txt"),
			message: "read ",
		},
		{
			name:    "an input that holds no domains",
			inPath:  emptyPath,
			message: "holds no domains",
		},
	}
	for _, c := range cases {
		err := writeSpoofResource(c.inPath, outPath, c.empty)
		if err == nil {
			t.Errorf("%s was accepted", c.name)
			continue
		}
		if !strings.Contains(err.Error(), c.message) {
			t.Errorf("%s failed with %q, expected %q", c.name, err, c.message)
		}
		if _, statErr := os.Stat(outPath); statErr == nil {
			t.Errorf("%s wrote the resource anyway", c.name)
		}
	}
}

// The resource keeps the input's own order, so an operator who groups the list
// by provider gets that grouping back and a diff of two lists is readable. The
// bytes themselves are not reproducible -- the mask is fresh per run -- so two
// runs of one input decode alike without being byte-identical.
func TestWriteSpoofResourceKeepsTheInputOrder(t *testing.T) {
	dir := t.TempDir()
	inPath := filepath.Join(dir, "domains.txt")
	// deliberately not sorted, so a generator that sorted would be caught
	plainText := "zeta.example\nalpha.example\nmiddle.example\nALPHA.example\n"
	if err := os.WriteFile(inPath, []byte(plainText), 0600); err != nil {
		t.Fatal(err)
	}
	want := []string{"zeta.example", "alpha.example", "middle.example"}

	resources := [][]byte{}
	for i := range 2 {
		outPath := filepath.Join(dir, fmt.Sprintf("out%d.bin", i))
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
		if !slices.Equal(spoofDomains, want) {
			t.Fatalf("run %d decoded %v, expected %v", i, spoofDomains, want)
		}
		resources = append(resources, resource)
	}
	if bytes.Equal(resources[0], resources[1]) {
		t.Fatal("two runs produced identical bytes, so the mask is not fresh per run")
	}
	// and the names are not in the resource as plain text
	for _, spoofDomain := range want {
		for _, resource := range resources {
			if bytes.Contains(resource, []byte(spoofDomain)) {
				t.Fatalf("%q appears in the resource as plain text", spoofDomain)
			}
		}
	}
}
