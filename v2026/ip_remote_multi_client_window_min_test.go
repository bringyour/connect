package connect

import "testing"

// A user-selected network peer is a fixed single-destination window. When that
// sole destination entered a warning state — draining, ulimit, or a health
// warning — the minimum counted only unwarned clients, so `1 <= 0` was false
// and the connect screen reported "Connecting to providers" for the whole
// session while the tunnel was up.
//
// Physically observed on 2026-07-29 with the client connected to one network
// peer: `[grid]1->2(false) points=1 CONNECTING`, where the target of 2 is the
// fixed size of 1 plus the one warned client.
func TestWindowMinSatisfiedCountsWarnedFixedDestination(t *testing.T) {
	cases := []struct {
		name                    string
		windowSizeMin           int
		clientCount             int
		warnedCount             int
		fixedDestination        bool
		strictWindowSizeHardMax bool
		windowSizeHardMax       int
		want                    bool
	}{
		{
			name:             "fixed sole destination warned still satisfies",
			windowSizeMin:    1,
			clientCount:      0,
			warnedCount:      1,
			fixedDestination: true,
			want:             true,
		},
		{
			name:             "fixed sole destination healthy satisfies",
			windowSizeMin:    1,
			clientCount:      1,
			warnedCount:      0,
			fixedDestination: true,
			want:             true,
		},
		{
			name:             "fixed destination with no client at all is unsatisfied",
			windowSizeMin:    1,
			clientCount:      0,
			warnedCount:      0,
			fixedDestination: true,
			want:             false,
		},
		{
			name:             "fixed multi destination counts warned toward the minimum",
			windowSizeMin:    3,
			clientCount:      1,
			warnedCount:      2,
			fixedDestination: true,
			want:             true,
		},
		{
			name:             "fixed multi destination short of the minimum",
			windowSizeMin:    3,
			clientCount:      1,
			warnedCount:      1,
			fixedDestination: true,
			want:             false,
		},
		{
			// An expanding window can replace a warned destination, so a
			// warning there genuinely means the minimum is not met yet.
			name:             "expanding window does not count warned clients",
			windowSizeMin:    2,
			clientCount:      1,
			warnedCount:      4,
			fixedDestination: false,
			want:             false,
		},
		{
			name:             "expanding window satisfied by unwarned clients",
			windowSizeMin:    2,
			clientCount:      2,
			warnedCount:      4,
			fixedDestination: false,
			want:             true,
		},
	}

	for _, c := range cases {
		got := windowMinSatisfied(
			c.windowSizeMin,
			c.clientCount,
			c.warnedCount,
			c.fixedDestination,
			c.strictWindowSizeHardMax,
			c.windowSizeHardMax,
		)
		if got != c.want {
			t.Errorf(
				"%s: windowMinSatisfied(min=%d, clients=%d, warned=%d, fixed=%t) = %t, want %t",
				c.name,
				c.windowSizeMin,
				c.clientCount,
				c.warnedCount,
				c.fixedDestination,
				got,
				c.want,
			)
		}
	}
}

// A strict mobile quality window owns at most four clients. A warned client
// carrying established flows remains owned so those flows are not destroyed,
// but it is excluded from new-flow selection. The old minimum calculation
// therefore saw only three clients and reported CONNECTING while the strict
// admission gate rejected every replacement because all four ownership slots
// were occupied. This is the exact closed loop observed on Android on
// 2026-09-02: five clients across quality and speed, warned clients retained,
// and repeated expand_decline reason=strict_hard_max events.
func TestStrictWindowSaturationCountsRetainedWarningTowardMinimum(t *testing.T) {
	firstId := NewId()
	secondId := NewId()
	thirdId := NewId()
	warnedId := NewId()
	windowSize := WindowSizeSettings{
		WindowSizeMin:     4,
		WindowSizeMax:     4,
		WindowSizeHardMax: 4,
	}
	window := &multiClientWindow{
		settings: &MultiClientSettings{StrictWindowSizeHardMax: true},
		clients: map[Id]*multiClientChannel{
			firstId:  nil,
			secondId: nil,
			thirdId:  nil,
			warnedId: nil,
		},
	}

	if window.strictWindowAdmissionAllowed(NewId(), windowSize) {
		t.Fatal("strict ownership ceiling unexpectedly admitted a replacement")
	}
	if !windowMinSatisfied(
		windowSize.WindowSizeMin,
		3,
		1,
		false,
		window.settings.StrictWindowSizeHardMax,
		windowSize.WindowSizeHardMax,
	) {
		t.Fatal("full strict window with a selectable client did not satisfy its minimum")
	}
}

// Android presents the merge of its quality and speed windows. The field
// failure had three selectable quality clients, one retained quality warning,
// and one retained speed warning: neither old per-window calculation reported
// satisfied, so their OR could never become true. The saturated quality window
// is usable and now supplies the merged minimum; the all-warned speed window
// correctly remains unsatisfied.
func TestMergedWindowMinimumSatisfiedAtStrictOwnershipCeiling(t *testing.T) {
	quality := NewRemoteUserNatMultiClientMonitorWithDefaults()
	speed := NewRemoteUserNatMultiClientMonitorWithDefaults()
	merged := NewMergedMultiClientMonitor([]MultiClientMonitor{quality, speed})

	quality.AddWindowExpandEvent(
		windowMinSatisfied(4, 3, 1, false, true, 4),
		4,
		false,
	)
	speed.AddWindowExpandEvent(
		windowMinSatisfied(1, 0, 1, false, true, 1),
		1,
		false,
	)

	if !merged.WindowExpandEvent().MinSatisfied {
		t.Fatal("usable saturated quality window did not satisfy the merged mobile window")
	}
}

// Saturation cannot make an unusable window look connected. When every owned
// client is warned, there is no ordinary candidate for a new flow even though
// the strict ownership ceiling prevents backfill.
func TestStrictWindowSaturationRequiresSelectableClient(t *testing.T) {
	if windowMinSatisfied(4, 0, 4, false, true, 4) {
		t.Fatal("all-warned strict window unexpectedly satisfied its minimum")
	}
}

// A retained warning counts only after strict admission has made replacement
// impossible. With an ownership slot available, the ordinary expanding-window
// contract remains in force and the monitor stays unsatisfied until backfill.
func TestStrictWindowWithReplacementSlotWaitsForBackfill(t *testing.T) {
	if windowMinSatisfied(4, 3, 1, false, true, 5) {
		t.Fatal("strict window with a replacement slot unexpectedly satisfied its minimum")
	}
}

// Server and other availability-first profiles do not opt into the strict
// ownership ceiling. Their warned clients must remain excluded because those
// profiles are allowed to admit a temporary replacement beyond the target.
func TestNonStrictWindowWaitsForWarnedClientReplacement(t *testing.T) {
	if windowMinSatisfied(4, 3, 1, false, false, 4) {
		t.Fatal("non-strict window counted a warned client toward its minimum")
	}
}
