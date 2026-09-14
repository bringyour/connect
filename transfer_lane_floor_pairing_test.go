package connect

import (
	"testing"
)

// THROUGHPUT-TESTGAPS U-12. THROUGHPUTFIX §27 and §28.3 state the rule in
// prose, `SendBufferSettings.LaneFloorByteCount`'s own doc comment states it
// again as a "deployment rule, because the default has a sharp edge", and
// nothing enforces it: a nonzero `LogicalDataLaneCount` with a zero
// `LaneFloorByteCount` ships the starvation §27 row F1 measured. The two
// fields are one decision and the tree lets them be set apart.
//
// Why the pairing is a cliff rather than a gradient. With lanes off, one
// sequence owns `ResendQueueMaxByteCount` outright. Turning lanes on with no
// floor replaces that with a single shared pool of the same size whose cap,
// for every lane, is the whole pool: a light lane beside a saturating one is
// reduced to the one item an empty queue always admits, which is not a share
// of anything. So the field that looks like an optimisation is the field that
// decides whether the other one starves, and it defaults to the value that
// starves.
//
// This is a GUARD and says so plainly: connect ships both at zero
// (`transfer.go`, `LogicalDataLaneCount: 0` and `LaneFloorByteCount: 0`), so
// the row passes on the tree as it stands. It exists because the failure it
// guards is silent — no error, no log, no counter, just one flow's latency
// under another flow's bulk transfer — and because the enabling change is a
// one-line settings edit that reviews as harmless.
//
// What it cannot reach, recorded rather than implied. The shipped violation
// §28.3 names lives in `sdk/mobile_memory_policy.go`, which sets
// `LogicalDataLaneCount = 8` on mobile H1 and sets no floor. The sdk is a
// separate module that imports this one, so a row here cannot read it; this
// row holds the rule for every settings value connect itself ships and for
// any caller that builds on them, and the sdk needs the mirror of it in its
// own package.
func TestALaneCountWithoutALaneFloorIsNotAConfigurationWeShip(t *testing.T) {
	shipped := []struct {
		name     string
		settings *SendBufferSettings
	}{
		{"DefaultSendBufferSettings", DefaultSendBufferSettings()},
		{
			"DefaultSendBufferSettingsWithBufferSize",
			DefaultSendBufferSettingsWithBufferSize(defaultTransferBufferSize),
		},
		{
			"DefaultClientSettings.SendBufferSettings",
			DefaultClientSettings().SendBufferSettings,
		},
		{
			"DefaultClientSettingsWithBufferSize.SendBufferSettings",
			DefaultClientSettingsWithBufferSize(defaultTransferBufferSize).SendBufferSettings,
		},
		{
			"DefaultClientSettingsNoNetworkEvents.SendBufferSettings",
			DefaultClientSettingsNoNetworkEvents().SendBufferSettings,
		},
	}

	for _, shipped := range shipped {
		settings := shipped.settings
		if settings == nil {
			t.Errorf("%s ships no send buffer settings", shipped.name)
			continue
		}
		if settings.LogicalDataLaneCount != 0 && settings.LaneFloorByteCount <= 0 {
			t.Errorf(
				"%s names %d logical data lanes with a lane floor of %d. A nonzero lane count with a zero floor ships the starvation of THROUGHPUTFIX §27 row F1: the per-sequence resend queue becomes one shared pool whose cap is the whole pool for every lane, so a light lane beside a saturating one holds the single item an empty queue admits. The two fields are one decision and must be set in one change; the candidate scale is ResendQueueMinByteCount, which lane zero and every distinct destination on an sdk-hosted provider already keep",
				shipped.name,
				settings.LogicalDataLaneCount,
				settings.LaneFloorByteCount,
			)
		}
		if settings.LaneFloorByteCount < 0 {
			t.Errorf(
				"%s names a negative lane floor %d",
				shipped.name,
				settings.LaneFloorByteCount,
			)
		}

		// The other half of the same decision, and the half the deployment
		// rule does not state. A floor is an exemption from the shared pool
		// rather than a reservation in it (§27 row F3), so the floors may sum
		// above the pool without reserving anything — but a set of floors
		// whose sum exceeds the pool is a set no arrangement of traffic can
		// honour at once, which is the guarantee withdrawn under exactly the
		// load it was written for. Bounded here at the lane maximum the
		// sequence enforces rather than at the configured count, because
		// `LogicalDataLaneCount` is clamped to `maxLogicalDataLaneCount`
		// before it is used.
		laneCount := min(settings.LogicalDataLaneCount, maxLogicalDataLaneCount)
		if 0 < laneCount && 0 < settings.LaneFloorByteCount {
			committed := ByteCount(laneCount) * settings.LaneFloorByteCount
			if settings.ResendQueueMaxByteCount < committed {
				t.Errorf(
					"%s commits %d lanes x %d bytes of floor = %d against a resend queue of %d. The floors cannot all be held at once, so the guarantee fails under the load it exists for",
					shipped.name,
					laneCount,
					settings.LaneFloorByteCount,
					committed,
					settings.ResendQueueMaxByteCount,
				)
			}
		}
	}
}

// The arithmetic the deployment rule above will meet the day it is followed,
// asserted now so that the campaign reads it as a constraint rather than
// discovering it as a result.
//
// `LaneFloorByteCount`'s doc names `ResendQueueMinByteCount` as the candidate
// scale. That constant is flat — `kib(256)`, `transfer.go` — while the pool it
// would be carved out of, `ResendQueueMaxByteCount`, is
// `MemoryScaledByteCount(mib(2), kib(256))` and therefore shrinks with the
// budget below the 64 MiB reference. At the reference the eight lanes the
// sequence permits commit exactly the whole pool; at every budget below it
// they commit more than the pool holds, and at the 24 MiB mobile target —
// which is where `LogicalDataLaneCount = 8` is actually set — they commit
// about two and two thirds of it.
//
// So this row does not say the candidate is wrong. It says the candidate is
// unscaled against a scaled pool, which is the same defect in miniature that
// THROUGHPUTFIX §37.22 found everywhere else, and it fails if anyone claims
// the pair fits without making the floor a draw on the same quantity the pool
// is a draw on.
func TestTheCandidateLaneFloorDoesNotFitTheMobilePool(t *testing.T) {
	defer SetMemoryBudget(0)

	for _, budget := range []ByteCount{mib(8), mib(16), mib(24), mib(32), mib(64), mib(128)} {
		SetMemoryBudget(budget)
		settings := DefaultSendBufferSettings()
		pool := settings.ResendQueueMaxByteCount
		candidateFloor := settings.ResendQueueMinByteCount
		committed := ByteCount(maxLogicalDataLaneCount) * candidateFloor

		if candidateFloor <= 0 {
			t.Fatalf(
				"budget %d: the candidate lane floor scale ResendQueueMinByteCount is %d, so the deployment rule names nothing",
				budget,
				candidateFloor,
			)
		}
		if pool < candidateFloor {
			t.Errorf(
				"budget %d: the resend queue %d is below its own per-sequence floor %d",
				budget,
				pool,
				candidateFloor,
			)
		}

		fits := committed <= pool
		t.Logf(
			"budget %d: pool %d, candidate floor %d, %d lanes commit %d, fits=%t",
			budget,
			pool,
			candidateFloor,
			maxLogicalDataLaneCount,
			committed,
			fits,
		)

		// The reference is the only budget at which the unscaled candidate and
		// the scaled pool agree, and they agree exactly. That exactness is the
		// evidence that the candidate was chosen at the reference and never
		// carried down; assert it, so that a change to either constant that
		// breaks the coincidence is read here rather than inferred later from
		// a campaign that fits on desktop and starves on a phone.
		if budget == referenceMemoryBudgetByteCount {
			if committed != pool {
				t.Errorf(
					"at the %d reference the %d candidate floors commit %d against a pool of %d; they were equal by construction, so one of ResendQueueMinByteCount, ResendQueueMaxByteCount or maxLogicalDataLaneCount moved without the others",
					budget,
					maxLogicalDataLaneCount,
					committed,
					pool,
				)
			}
			continue
		}
		if budget < referenceMemoryBudgetByteCount && fits {
			t.Errorf(
				"budget %d is below the %d reference, where the flat candidate floor is expected to overcommit the scaled pool, and it fits: %d lanes x %d = %d against %d. If the floor became a draw on the budget this row has served its purpose and should be rewritten to assert the draw",
				budget,
				referenceMemoryBudgetByteCount,
				maxLogicalDataLaneCount,
				candidateFloor,
				committed,
				pool,
			)
		}
	}
}
