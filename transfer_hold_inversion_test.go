package connect

import "testing"

// THROUGHPUTFIX §37.17 and §37.18: the crossing budget at which a client's
// receive hold falls below an unbudgeted peer's send window, reproduced on
// unmodified main and never pinned as a relationship until here.
//
// The two constants pass through the same scale
// (`transfer.go`, `ResendQueueMaxByteCount` and `ReceiveQueueMaxByteCount`), so
// on ONE host the hold is 1.25 times the window at every budget and the
// ordering never inverts. They do not live on one host. A provider runs
// unbudgeted, so its window is the whole 2 MiB; a client at budget B holds
// `max(320 KiB, 2.5 MiB x B / 64 MiB)`. The hold is under the peer's window
// whenever 2.5 MiB x B / 64 MiB < 2 MiB, which is B < 51.2 MiB — every mobile
// budget this program has discussed. Measured at a 24 MiB client against two
// routes with one killed mid-transfer: the hold saturated in four of four runs
// and none completed; a 52 MiB client completed clean.
//
// What this row is and is not. It pins the relationship — the same-host ratio
// and the cross-host crossing — and deliberately does not assert that the
// shipped budgets are inverted, because that would freeze the defect in place
// and fail the day someone fixes it. It is a guard: it passes before and after,
// and it fails when either constant moves without the other, which is the way
// the crossing would silently shift.
//
// What refutes it: rescaling the hold or the window alone, changing either
// floor out of their 1.25 ratio, or moving the reference, each of which moves
// the crossing budget away from 0.8 of the sender's.
func TestTheReceiveHoldAndThePeerWindowCrossAtFourFifthsOfTheSendersBudget(t *testing.T) {
	restore := MemoryBudget()
	t.Cleanup(func() { SetMemoryBudget(restore) })

	// The relationship above is the one between two scaled CONSTANTS, which is
	// what shipped when §37.17 measured it. The window rule now ships on
	// (`f8d564b`), and under it the hold is not a constant at all: it is the
	// budget's eighth, and so is the send ceiling, which is why the crossing
	// moves. The constant regime is still reachable by one SetWindowSizing
	// call and is still what a rollback produces, so it is pinned here as
	// itself rather than as the default; the rule's own relationship is
	// asserted at the end, where it belongs.
	t.Cleanup(func() { SetWindowSizing(DefaultWindowSizing()) })
	SetWindowSizing(WindowSizingConstant)

	// a provider runs unbudgeted, so the sender's window is the unscaled
	// constant. This is the peer a shipped client actually faces.
	SetMemoryBudget(0)
	peerWindow := DefaultSendBufferSettings().ResendQueueMaxByteCount

	// on one host the hold is 1.25 times the window at every budget, floors
	// included, which is why the inversion is asymmetric rather than general
	for _, budget := range []ByteCount{
		0, mib(1), mib(8), mib(24), mib(32), mib(51), mib(52), mib(64), mib(256),
	} {
		SetMemoryBudget(budget)
		window := DefaultSendBufferSettings().ResendQueueMaxByteCount
		hold := DefaultReceiveBufferSettings().ReceiveQueueMaxByteCount
		if 4*hold != 5*window {
			t.Errorf(
				"at a %d byte budget one host holds %d against its own window of %d, a ratio of %.3f rather than 1.25; the two are scaled by the same factor by construction, and a host whose hold is not 1.25 times its window has had one of them moved alone",
				budget, hold, window, float64(hold)/float64(window),
			)
		}
	}

	// the crossing, bracketed with shipping settings rather than asserted from
	// the formula: at 51 MiB the client holds less than the peer may have
	// outstanding, and at 52 MiB it does not
	SetMemoryBudget(mib(51))
	below := DefaultReceiveBufferSettings().ReceiveQueueMaxByteCount
	SetMemoryBudget(mib(52))
	above := DefaultReceiveBufferSettings().ReceiveQueueMaxByteCount
	if !(below < peerWindow && peerWindow <= above) {
		t.Errorf(
			"the hold reads %d at a 51 MiB budget and %d at 52 MiB against an unbudgeted peer's %d byte window; the crossing is meant to sit between them, at four fifths of the sender's budget, and a crossing that has moved means a client and its peer now invert at a different budget than the record says",
			below, above, peerWindow,
		)
	}

	// the general condition, stated as the record states it: the receiver
	// inverts below 0.8 of the sender's budget. Computed from the hold at the
	// reference, where it is whole, rather than from whatever budget is set.
	SetMemoryBudget(referenceMemoryBudgetByteCount)
	holdAtReference := DefaultReceiveBufferSettings().ReceiveQueueMaxByteCount
	crossing := referenceMemoryBudgetByteCount * peerWindow / holdAtReference
	t.Logf(
		"unbudgeted peer window %d, hold at 51 MiB %d, at 52 MiB %d, crossing near %d (0.8 of the reference)",
		peerWindow, below, above, crossing,
	)
	for _, shipped := range []struct {
		name   string
		budget ByteCount
	}{
		{"the 8 MiB legacy target", mib(8)},
		{"a 24 MiB mobile device target", mib(24)},
		{"the 32 MiB iOS extension", mib(32)},
		{"the 48 MiB macOS extension", mib(48)},
	} {
		SetMemoryBudget(shipped.budget)
		hold := DefaultReceiveBufferSettings().ReceiveQueueMaxByteCount
		t.Logf(
			"%s: hold %d against peer window %d, %.2f times inverted",
			shipped.name, hold, peerWindow, float64(peerWindow)/float64(hold),
		)
	}

	// And the regime that ships. Under the rule the hold and the send ceiling
	// are the same draw on the same budget — the eighth — so a host holds
	// exactly what it may have outstanding, at every budget, and the 1.25
	// ratio above is replaced by 1. The crossing against an unbudgeted peer
	// does not disappear; it moves, and it moves by arithmetic this row can
	// state: an unbudgeted peer keeps the 2 MiB constant, so a budgeted client
	// is under it exactly while its eighth is under 2 MiB, which is below
	// 16 MiB rather than below 51.2. Every shipped mobile budget is above it.
	SetWindowSizing(WindowSizingFromDelivery)
	for _, budget := range []ByteCount{mib(8), mib(16), mib(24), mib(32), mib(64), mib(256)} {
		SetMemoryBudget(budget)
		share := transferBudgetShareByteCount()
		hold := DefaultReceiveBufferSettings().ReceiveQueueMaxByteCount
		sendSettings := DefaultSendBufferSettings()
		if sendSettings.ResendQueueBudget == nil {
			t.Fatalf("at a %d byte budget the rule attached no send pool", budget)
		}
		ceiling := sendSettings.ResendQueueBudget.LendableByteCount(
			sendSettings.ResendQueueMinByteCount,
		)
		if hold != share || ceiling != share {
			t.Errorf(
				"at a %d byte budget the rule gives a %d byte hold against a %d byte send ceiling, both meant to be the %d byte share; a host that draws unequal shares inverts against itself",
				budget, hold, ceiling, share,
			)
		}
		inverted := hold < peerWindow
		if wantInverted := budget < mib(16); inverted != wantInverted {
			t.Errorf(
				"at a %d byte budget the hold is %d against an unbudgeted peer's %d byte window, inverted = %t, want %t; the crossing under the rule is where the budget's eighth meets the peer's constant, which is 16 MiB",
				budget, hold, peerWindow, inverted, wantInverted,
			)
		}
	}
}

// THROUGHPUTFIX §37.18: the protection that replaced the floor raise, asserted
// because it ships as a zero value and nothing reads it back.
//
// U-09 in THROUGHPUT-TESTGAPS states the contract as "for every supported
// client budget, the receive hold is at least an unbudgeted peer's send
// window". That guard was considered and deliberately rejected. §37.18 chose
// never-evict over the floor raise precisely because the raise "would have
// taken a 24 MiB phone from 938 KiB to 2 MiB against a ceiling this program
// deferred", and committed-prefix acknowledgement makes the inversion
// survivable rather than absent. So the hold is still below the peer's window
// at every shipped budget by decision, and a row asserting otherwise would sit
// permanently red against the record rather than find anything.
//
// What is genuinely unpinned is whether the protection is on.
// `ReceiveHoldPolicy` is the zero value of its kind, so committed-prefix ships
// by reason of iota order and an unset field. Reorder those constants, or set
// the field anywhere along a settings path, and the tree silently returns to
// evicting — which withdraws an acknowledgement the sender still leases, the
// failure §37.20 measured at 45 and 108 acknowledged evictions per run. Nothing
// reads it back.
//
// A guard: it passes today and fails the moment the default moves.
func TestTheHoldKeepsTheProtectionThatReplacedTheFloorRaise(t *testing.T) {
	restore := MemoryBudget()
	t.Cleanup(func() { SetMemoryBudget(restore) })

	// the zero value has to be the safe policy, because that is the only reason
	// the shipping settings get it
	if ReceiveHoldCommittedPrefix != 0 {
		t.Errorf(
			"ReceiveHoldCommittedPrefix is %d rather than the zero value; the shipping settings never set ReceiveHoldPolicy, so the protection is on only while the safe policy is the one an unset field resolves to",
			ReceiveHoldCommittedPrefix,
		)
	}

	for _, budget := range []ByteCount{0, mib(8), mib(24), mib(32), mib(48), mib(64), mib(256)} {
		SetMemoryBudget(budget)
		settings := DefaultReceiveBufferSettings()
		if settings.ReceiveHoldPolicy != ReceiveHoldCommittedPrefix {
			t.Errorf(
				"at a %d byte budget the shipping hold policy is %d rather than committed-prefix (%d). The hold is under an unbudgeted peer's window at every shipped budget by decision, and committed-prefix is what makes that survivable: evicting withdraws an acknowledgement the sender still leases, and refusing starves a middle gap",
				budget, settings.ReceiveHoldPolicy, ReceiveHoldCommittedPrefix,
			)
		}
	}
	t.Logf("shipping hold policy is committed-prefix (%d) at every budget", ReceiveHoldCommittedPrefix)
}
