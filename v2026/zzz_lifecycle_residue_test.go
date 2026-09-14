// Lifecycle residue tests keep package-wide qualification from hiding owners
// whose tests returned while their per-instance workers remained live.
package connect

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

// Captures a complete goroutine dump, growing past the initial buffer instead
// of silently truncating the ownership evidence on a high-concurrency suite.
func lifecycleResidueStacks() []byte {
	for byteCount := 1024 * 1024; ; byteCount *= 2 {
		stackBytes := make([]byte, byteCount)
		writtenByteCount := runtime.Stack(stackBytes, true)
		if writtenByteCount < len(stackBytes) {
			return stackBytes[:writtenByteCount]
		}
	}
}

// A package-wide run may retain fixed process workers, including the two
// lazily initialized default address allocators (one per ip family; see the
// tun allocators). Per-client contract workers, per-peer ICE loops and
// additional address producers must have been joined by the test that owned
// them before package qualification can succeed.
func TestZZZNoPerInstanceLifecycleResidue(t *testing.T) {
	deadline := time.Now().Add(5 * time.Second)
	for {
		stackBytes := lifecycleResidueStacks()
		contractWorkerCount := lifecycleResidueCount(
			stackBytes,
			"(*ContractManager).CloseContractWithCheckpoint.func3",
		)
		iceWorkerCount := lifecycleResidueCount(
			stackBytes,
			"github.com/pion/ice/v4/internal/taskloop.(*Loop).runLoop",
		)
		addressWorkerCount := lifecycleResidueCount(
			stackBytes,
			"(*AddrGenerator).run(",
		)
		if contractWorkerCount == 0 && iceWorkerCount == 0 && addressWorkerCount <= 2 {
			return
		}
		if deadline.Before(time.Now()) {
			t.Fatalf(
				"per-instance lifecycle residue: contract_workers=%d ice_workers=%d address_workers=%d\n%s",
				contractWorkerCount,
				iceWorkerCount,
				addressWorkerCount,
				stackBytes,
			)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
