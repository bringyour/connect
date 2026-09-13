// Lifecycle residue keeps package-wide qualification from hiding a node whose
// test returned while its four loops stayed live (EXTENDER.md D1).
//
// Every Node starts runPeering, runReceive, runTopicEvents and runStatus in its
// constructor, and Close joins all four. A test that forgets its Close, or a
// Close that stops joining, leaves them in the dump: the suite still passes
// today and the leak is only found when a long-running app accumulates them.
package gossip

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

// Every node loop of every test must have been joined before the package
// qualifies. The deadline covers the libp2p host teardown each Close waits on,
// not a race in the join itself.
func TestZZZNoNodeLifecycleResidue(t *testing.T) {
	signatures := []string{
		"(*Node).runPeering(",
		"(*Node).runReceive(",
		"(*Node).runTopicEvents(",
		"(*Node).runStatus(",
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
			t.Fatalf("node lifecycle residue: %v\n%s", residue, stackBytes)
		}
		runtime.Gosched()
	}
}
