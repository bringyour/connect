// Pins the dependency primitive used to park netstack's idle Tcp processors.
package connect

import (
	"testing"

	gvisorsync "gvisor.dev/gvisor/pkg/sync"
)

// Distinct comparison and replacement operands are required by Sleeper,
// Gate, and Waiter. The old Arm64 race assembly overwrote the comparison
// register, causing idle netstack workers to consume their entire Cpu budget.
func TestTunGvisorParkCompareAndSwapPreservesOperands(t *testing.T) {
	for _, test := range []struct {
		initial     uintptr
		expected    uintptr
		replacement uintptr
		wantSwap    bool
		wantValue   uintptr
	}{
		{initial: 7, expected: 7, replacement: 11, wantSwap: true, wantValue: 11},
		{initial: 7, expected: 5, replacement: 11, wantSwap: false, wantValue: 7},
		{initial: 7, expected: 5, replacement: 7, wantSwap: false, wantValue: 7},
	} {
		value := test.initial
		swapped := gvisorsync.RaceUncheckedAtomicCompareAndSwapUintptr(
			&value, test.expected, test.replacement,
		)
		if swapped != test.wantSwap || value != test.wantValue {
			t.Errorf("compare %d with %d, replace with %d: swapped=%t value=%d; want %t/%d",
				test.initial, test.expected, test.replacement, swapped, value, test.wantSwap, test.wantValue)
		}
	}
}
