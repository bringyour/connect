// Builds the bundled spoof domain resource (EXTENDER.md A10) from a plain
// text list, so operations can fill the list without the names appearing in
// the binary as plain strings.
//
//	go run ./scripts/extender_spoof -in domains.txt -out res/extender_spoof.bin
//
// The input is one domain per line; `#` comments and blank lines are ignored.
//
// `-in` is required. A run with no input would otherwise write the empty
// resource over the shipped list, which is a silent loss: the build still
// succeeds and every client stops fronting its extender dials. Pass `-empty`
// to ask for the empty resource on purpose.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/urnetwork/connect/v2026"
)

func main() {
	inPath := flag.String("in", "", "plain text domain list, one per line; required unless -empty")
	outPath := flag.String("out", "res/extender_spoof.bin", "resource file to write")
	empty := flag.Bool("empty", false, "write the empty resource, which disables random extender discovery")
	flag.Parse()

	if err := writeSpoofResource(*inPath, *outPath, *empty); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		flag.Usage()
		os.Exit(1)
	}
}

// Reads the list and writes the resource. An input path and `-empty` are
// mutually exclusive, and neither is refused rather than guessed: the caller
// that wanted a list would silently ship none.
func writeSpoofResource(inPath string, outPath string, empty bool) error {
	switch {
	case inPath == "" && !empty:
		return fmt.Errorf("extender_spoof needs -in <domain list>, or -empty to write the empty resource")
	case inPath != "" && empty:
		return fmt.Errorf("extender_spoof takes -in or -empty, not both")
	}

	plainText := []byte{}
	if inPath != "" {
		var err error
		if plainText, err = os.ReadFile(inPath); err != nil {
			return fmt.Errorf("read %s: %w", inPath, err)
		}
	}

	spoofDomains := connect.ParseSpoofDomains(plainText)
	if len(spoofDomains) == 0 && !empty {
		return fmt.Errorf("%s holds no domains; pass -empty to write the empty resource", inPath)
	}
	resource, err := connect.EncodeSpoofDomainsResource(spoofDomains)
	if err != nil {
		return fmt.Errorf("encode: %w", err)
	}
	if err := os.WriteFile(outPath, resource, 0o644); err != nil {
		return fmt.Errorf("write %s: %w", outPath, err)
	}
	fmt.Printf("wrote %s with %d domains (%d bytes)\n", outPath, len(spoofDomains), len(resource))
	return nil
}
