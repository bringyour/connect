// Lifecycle residue keeps package-wide qualification from hiding a server whose
// test returned while its workers stayed live (EXTENDER.md A9, G2).
//
// An ExtenderServer owns its accept loops, its quic serve goroutines and the
// bounded dns forward worker pool, and CloseAndWait joins all of them. A
// fixture that forgets the close, or a close that stops joining, leaves the
// workers in the dump: the suite still passes and the leak surfaces only where
// a provider starts and stops the role many times.
package extender

import (
	"bytes"
	"runtime"
	"testing"
	"time"
)

// Counts one stack signature in a complete goroutine dump.
func lifecycleResidueCount(stackBytes []byte, signature string) int {
	return bytes.Count(stackBytes, []byte(signature))
}

// Captures a complete goroutine dump, growing past the initial buffer rather
// than truncating the ownership evidence.
func lifecycleResidueStacks() []byte {
	for byteCount := 1024 * 1024; ; byteCount *= 2 {
		stackBytes := make([]byte, byteCount)
		writtenByteCount := runtime.Stack(stackBytes, true)
		if writtenByteCount < len(stackBytes) {
			return stackBytes[:writtenByteCount]
		}
	}
}

// Every dns forward worker and every relay direction of every test must have
// been joined before the package qualifies.
func TestZZZNoExtenderLifecycleResidue(t *testing.T) {
	signatures := []string{
		"(*extenderDnsForwarder).run(",
		"(*ExtenderServer).relay(",
		"(*ExtenderServer).serveQuicCarrier(",
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		stackBytes := lifecycleResidueStacks()
		residue := map[string]int{}
		for _, signature := range signatures {
			if count := lifecycleResidueCount(stackBytes, signature); 0 < count {
				residue[signature] = count
			}
		}
		if len(residue) == 0 {
			return
		}
		if deadline.Before(time.Now()) {
			t.Fatalf("extender lifecycle residue: %v\n%s", residue, stackBytes)
		}
		runtime.Gosched()
	}
}
