package connect

import (
	"sync/atomic"
)

// a process-wide memory budget that scales the default settings whose values
// dominate the per-connection and per-peer memory ceilings (queue caps,
// receive windows, socket buffers, cache bounds). The budget is advisory
// sizing state, separate from the go runtime soft memory limit; hosts set
// both together through the sdk (see sdk.SetMemoryLimit).
//
// A zero budget (the default) leaves every setting at its unscaled default.
// Settings sample the budget when a Default*Settings constructor runs, so the
// budget must be set before constructing the objects it should size. The
// mobile hosts set it at process start, before any device exists.

// budgets at or above the reference use the unscaled defaults; smaller
// budgets scale the memory-dominant settings proportionally, down to
// per-setting floors
var referenceMemoryBudgetByteCount = mib(64)

var memoryBudgetByteCount atomic.Int64
var defaultPlatformTransportBudget atomic.Pointer[PlatformTransportBudget]

func init() {
	defaultPlatformTransportBudget.Store(newDefaultPlatformTransportBudget(referenceMemoryBudgetByteCount))
}

func newDefaultPlatformTransportBudget(budgetByteCount ByteCount) *PlatformTransportBudget {
	return NewPlatformTransportBudget(
		// Keep the normal share at one quarter, but leave room for one H3
		// carrier at the supported low-memory floor. Without this matching
		// floor, an explicit H3 selection on the 8 MiB legacy host target
		// would wait forever on a 2 MiB aggregate budget for a 3 MiB claim.
		min(budgetByteCount, max(mib(3), budgetByteCount/4)),
		16,
	)
}

// NewPlatformTransportBudgetForMemoryTarget creates an independently owned
// carrier budget using the same sizing policy as the process default. A
// nonpositive target preserves the legacy process-wide budget so callers that
// explicitly disable per-owner memory sizing retain their prior behavior.
func NewPlatformTransportBudgetForMemoryTarget(
	memoryTargetByteCount ByteCount,
) *PlatformTransportBudget {
	if memoryTargetByteCount <= 0 {
		return DefaultPlatformTransportBudget()
	}
	return newDefaultPlatformTransportBudget(memoryTargetByteCount)
}

// SetMemoryBudget sets the process-wide memory budget that scales the
// memory-dominant default settings. 0 (the default) disables scaling.
func SetMemoryBudget(budgetByteCount ByteCount) {
	memoryBudgetByteCount.Store(budgetByteCount)
	if budgetByteCount <= 0 {
		budgetByteCount = referenceMemoryBudgetByteCount
	}
	// Platform carriers normally share one quarter of the process target, with
	// the single-H3 working floor applied above. A separate count cap throttles
	// cold multi-client candidate expansion even when H1's byte reservation
	// alone would allow every candidate to dial at once.
	defaultPlatformTransportBudget.Store(newDefaultPlatformTransportBudget(budgetByteCount))
}

func MemoryBudget() ByteCount {
	return memoryBudgetByteCount.Load()
}

// DefaultPlatformTransportBudget returns the process-wide budget sampled by
// new PlatformTransport settings.
func DefaultPlatformTransportBudget() *PlatformTransportBudget {
	return defaultPlatformTransportBudget.Load()
}

// memoryScale returns the budget scale in (0, 1]
func memoryTargetScale(budgetByteCount ByteCount) float64 {
	if budgetByteCount <= 0 || referenceMemoryBudgetByteCount <= budgetByteCount {
		return 1
	}
	return float64(budgetByteCount) / float64(referenceMemoryBudgetByteCount)
}

func memoryScale() float64 {
	return memoryTargetScale(memoryBudgetByteCount.Load())
}

// MemoryTargetScaledByteCount scales one default byte count from an explicit
// owner target instead of the process-global target. A nonpositive target
// retains the unscaled default; floorByteCount preserves the working minimum.
func MemoryTargetScaledByteCount(
	memoryTargetByteCount ByteCount,
	unscaledByteCount ByteCount,
	floorByteCount ByteCount,
) ByteCount {
	scaledByteCount := ByteCount(
		memoryTargetScale(memoryTargetByteCount) * float64(unscaledByteCount),
	)
	return max(floorByteCount, scaledByteCount)
}

// MemoryScaledByteCount scales a default byte count by the memory budget,
// with a floor that preserves a working minimum
func MemoryScaledByteCount(unscaledByteCount ByteCount, floorByteCount ByteCount) ByteCount {
	scaledByteCount := ByteCount(memoryScale() * float64(unscaledByteCount))
	return max(floorByteCount, scaledByteCount)
}

// MemoryScaledCount scales a default count by the memory budget,
// with a floor that preserves a working minimum
func MemoryScaledCount(unscaledCount int, floorCount int) int {
	scaledCount := int(memoryScale() * float64(unscaledCount))
	return max(floorCount, scaledCount)
}
