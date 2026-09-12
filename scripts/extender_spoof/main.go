// Builds the bundled spoof domain resource (EXTENDER.md A10) from a plain
// text list, so operations can fill the list without the names appearing in
// the binary as plain strings.
//
//	go run ./scripts/extender_spoof -in domains.txt -out res/extender_spoof.bin
//
// The input is one domain per line; `#` comments and blank lines are ignored.
// An empty or absent input produces the empty resource that ships today.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/urnetwork/connect"
)

func main() {
	inPath := flag.String("in", "", "plain text domain list, one per line; empty builds an empty list")
	outPath := flag.String("out", "res/extender_spoof.bin", "resource file to write")
	flag.Parse()

	plainText := []byte{}
	if *inPath != "" {
		var err error
		plainText, err = os.ReadFile(*inPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "read %s: %s\n", *inPath, err)
			os.Exit(1)
		}
	}

	spoofDomains := connect.ParseSpoofDomains(plainText)
	resource, err := connect.EncodeSpoofDomainsResource(spoofDomains)
	if err != nil {
		fmt.Fprintf(os.Stderr, "encode: %s\n", err)
		os.Exit(1)
	}
	if err := os.WriteFile(*outPath, resource, 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "write %s: %s\n", *outPath, err)
		os.Exit(1)
	}
	fmt.Printf("wrote %s with %d domains (%d bytes)\n", *outPath, len(spoofDomains), len(resource))
}
