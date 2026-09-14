package connect

import (
	"context"
	"testing"
)

const deviceBudgetTestMemoryTarget = ByteCount(24 * 1024 * 1024)

// acquireDeviceBudgetTestH1 registers and synchronously acquires one H1
// carrier reservation. Acquisition is deterministic because callers invoke it
// only while capacity is known to be available.
func acquireDeviceBudgetTestH1(
	t *testing.T,
	budget *PlatformTransportBudget,
	byteCount ByteCount,
) *platformTransportBudgetReservation {
	t.Helper()
	reservation := budget.register(platformTransportBudgetH1, byteCount, true)
	if !reservation.Acquire(context.Background()) {
		t.Fatal("H1 reservation did not acquire available device capacity")
	}
	return reservation
}

// TestSharedPlatformTransportBudgetBlocksTheSeventeenthDeviceCarrier pins the
// production root cause: unrelated devices sharing the process budget fill its
// sixteen transport slots even though the next device owns no live carrier.
func TestSharedPlatformTransportBudgetBlocksTheSeventeenthDeviceCarrier(t *testing.T) {
	settings := DefaultPlatformTransportSettingsWithMemoryTarget(
		deviceBudgetTestMemoryTarget,
	)
	budget := settings.PlatformTransportBudget
	reservations := make([]*platformTransportBudgetReservation, 0, 17)
	for range 16 {
		reservations = append(
			reservations,
			acquireDeviceBudgetTestH1(t, budget, settings.H1BudgetByteCount),
		)
	}
	deferred := budget.register(
		platformTransportBudgetH1,
		settings.H1BudgetByteCount,
		true,
	)
	reservations = append(reservations, deferred)
	if !deferred.IsWaiting() {
		t.Fatal("seventeenth shared H1 carrier was not blocked by the process slot cap")
	}
	for _, reservation := range reservations {
		reservation.Release()
	}
}

// TestPrivatePlatformTransportBudgetsAdmitEveryDeviceWindow proves the fix at
// the same deterministic boundary: three independent devices may each admit
// their complete eight-carrier candidate window without consuming another
// device's count or byte capacity.
func TestPrivatePlatformTransportBudgetsAdmitEveryDeviceWindow(t *testing.T) {
	const deviceCount = 3
	const carrierCount = 8

	budgets := make([]*PlatformTransportBudget, 0, deviceCount)
	reservations := make([]*platformTransportBudgetReservation, 0, deviceCount*carrierCount)
	for range deviceCount {
		settings := DefaultPlatformTransportSettingsWithMemoryTarget(
			deviceBudgetTestMemoryTarget,
		)
		budget := settings.PlatformTransportBudget
		budgets = append(budgets, budget)
		for range carrierCount {
			reservation := acquireDeviceBudgetTestH1(
				t,
				budget,
				settings.H1BudgetByteCount,
			)
			if reservation.IsWaiting() {
				t.Fatal("private device H1 carrier remained waiting after acquisition")
			}
			reservations = append(reservations, reservation)
		}
	}
	if budgets[0] == budgets[1] || budgets[1] == budgets[2] || budgets[0] == budgets[2] {
		t.Fatal("independent device settings reused one platform transport budget")
	}
	for _, reservation := range reservations {
		reservation.Release()
	}
}

// TestDeviceMemoryTargetPlatformSettingsFitAutoWorkingSet ensures the private
// quarter-share can admit one target-scaled H3 carrier while preserving one H1
// fallback carrier. A mismatch here would turn explicit or Auto H3 into a
// permanent budget wait on low-memory devices.
func TestDeviceMemoryTargetPlatformSettingsFitAutoWorkingSet(t *testing.T) {
	settings := DefaultPlatformTransportSettingsWithMemoryTarget(
		deviceBudgetTestMemoryTarget,
	)
	stats := settings.PlatformTransportBudget.Stats()
	workingSet := settings.H1BudgetByteCount + settings.H3BudgetByteCount
	if stats.TotalByteCount < workingSet {
		t.Fatalf(
			"device carrier budget = %d, below H1+H3 working set %d",
			stats.TotalByteCount,
			workingSet,
		)
	}
}
