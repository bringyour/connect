package connect

import (
	"bytes"
	"compress/gzip"
	"crypto/rand"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"

	_ "embed"
)

// The bundled spoof domain list (EXTENDER.md A10).
//
// A client dial fronts an extender with one of these names, so the names must
// not appear in the binary as plain strings where a scanner can lift the whole
// list out of a shipped app. The list is therefore carried as a gzip stream
// masked with a repeating key stored in the same resource, decoded once on
// first use. Masking is obfuscation, not secrecy: anyone who runs the code can
// recover the list, which is expected, since a probe can also discover any one
// name by connecting.
//
// The initial content is an operator decision. The bundled resource ships an
// empty list, which makes EnumerateExtenderProfiles yield nothing, so random
// extender discovery is inert until operations provide the list with
// scripts/extender_spoof.

// The resource is `mask || xor(gzip(one domain per line), repeat(mask))`.
const extenderSpoofMaskByteCount = 8

//go:embed res/extender_spoof.bin
var extenderSpoofResource []byte

var (
	spoofDomainsOnce      sync.Once
	spoofDomainsStateLock sync.Mutex
	spoofDomainsValues    []string
)

// The bundled spoof domain list, decoded on first use. The result is a copy
// the caller may keep. An unreadable resource yields an empty list rather than
// failing a dial: a missing spoof list only disables random discovery.
func SpoofDomains() []string {
	spoofDomainsOnce.Do(func() {
		spoofDomains, err := DecodeSpoofDomainsResource(extenderSpoofResource)
		if err != nil {
			spoofDomains = []string{}
		}
		spoofDomainsStateLock.Lock()
		defer spoofDomainsStateLock.Unlock()
		if spoofDomainsValues == nil {
			spoofDomainsValues = spoofDomains
		}
	})
	spoofDomainsStateLock.Lock()
	defer spoofDomainsStateLock.Unlock()
	return slices.Clone(spoofDomainsValues)
}

// Installs a synthetic list in place of the bundled one and returns the
// restore. Tests use it so no real domain appears in the repository.
func setSpoofDomainsForTest(spoofDomains []string) func() {
	// take the once so a later SpoofDomains does not overwrite the override
	spoofDomainsOnce.Do(func() {})
	spoofDomainsStateLock.Lock()
	defer spoofDomainsStateLock.Unlock()
	previousSpoofDomains := spoofDomainsValues
	spoofDomainsValues = slices.Clone(spoofDomains)
	return func() {
		spoofDomainsStateLock.Lock()
		defer spoofDomainsStateLock.Unlock()
		spoofDomainsValues = previousSpoofDomains
	}
}

// Normalizes the plain text form: one domain per line, `#` comments and blank
// lines dropped, lowercased, deduplicated in first-seen order.
func ParseSpoofDomains(plainText []byte) []string {
	spoofDomains := []string{}
	visited := map[string]bool{}
	for _, line := range strings.Split(string(plainText), "\n") {
		if i := strings.IndexByte(line, '#'); 0 <= i {
			line = line[0:i]
		}
		spoofDomain := strings.ToLower(strings.TrimSpace(line))
		if spoofDomain == "" || visited[spoofDomain] {
			continue
		}
		visited[spoofDomain] = true
		spoofDomains = append(spoofDomains, spoofDomain)
	}
	return spoofDomains
}

// Builds the embedded resource from a domain list. The generator command uses
// it; tests use it with the decoder to pin the round trip.
func EncodeSpoofDomainsResource(spoofDomains []string) ([]byte, error) {
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	for _, spoofDomain := range spoofDomains {
		if _, err := writer.Write([]byte(spoofDomain + "\n")); err != nil {
			return nil, err
		}
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}

	mask := make([]byte, extenderSpoofMaskByteCount)
	if _, err := rand.Read(mask); err != nil {
		return nil, err
	}
	compressedBytes := compressed.Bytes()
	resource := make([]byte, extenderSpoofMaskByteCount+len(compressedBytes))
	copy(resource[0:extenderSpoofMaskByteCount], mask)
	for i, b := range compressedBytes {
		resource[extenderSpoofMaskByteCount+i] = b ^ mask[i%extenderSpoofMaskByteCount]
	}
	return resource, nil
}

// Recovers the domain list from the embedded resource form.
func DecodeSpoofDomainsResource(resource []byte) ([]string, error) {
	if len(resource) < extenderSpoofMaskByteCount {
		return nil, fmt.Errorf("spoof resource is %d bytes, expected at least %d", len(resource), extenderSpoofMaskByteCount)
	}
	mask := resource[0:extenderSpoofMaskByteCount]
	maskedBytes := resource[extenderSpoofMaskByteCount:]
	compressedBytes := make([]byte, len(maskedBytes))
	for i, b := range maskedBytes {
		compressedBytes[i] = b ^ mask[i%extenderSpoofMaskByteCount]
	}
	reader, err := gzip.NewReader(bytes.NewReader(compressedBytes))
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	plainText, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	return ParseSpoofDomains(plainText), nil
}
