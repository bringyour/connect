package connect

import (
	"math/rand"
	"testing"
	"time"
)

// laneBurstLoss is the campaign's two-state loss model (server/connect/perfvar
// mixed-direct-burst-loss): a good state with a small background loss that
// enters a bad state, where most packets are lost, and leaves it again a few
// packets later. Bursts arrive about every hundred packets, so the model is
// clustered loss, not long clean stretches between bursts.
type laneBurstLoss struct {
	goodToBad float64
	badToGood float64
	goodLoss  float64
	badLoss   float64
}

// campaignBurstLoss are the campaign's parameters verbatim.
var campaignBurstLoss = laneBurstLoss{
	goodToBad: 0.01,
	badToGood: 0.35,
	goodLoss:  0.002,
	badLoss:   0.65,
}

// laneLossProcess is a seeded per-packet loss process, independent with one
// probability or the two-state burst chain, so a classification test is
// deterministic and repeatable.
type laneLossProcess struct {
	random      *rand.Rand
	independent float64
	burst       *laneBurstLoss
	bad         bool
}

func newLaneLossProcess(seed int64, independent float64, burst *laneBurstLoss) *laneLossProcess {
	return &laneLossProcess{
		random:      rand.New(rand.NewSource(seed)),
		independent: independent,
		burst:       burst,
	}
}

// lost reports whether the next packet is lost.
func (self *laneLossProcess) lost() bool {
	if self.burst == nil {
		return self.random.Float64() < self.independent
	}
	if self.bad {
		if self.random.Float64() < self.burst.badToGood {
			self.bad = false
		}
	} else if self.random.Float64() < self.burst.goodToBad {
		self.bad = true
	}
	probability := self.burst.goodLoss
	if self.bad {
		probability = self.burst.badLoss
	}
	return self.random.Float64() < probability
}

// FLIGHTGATEFIX §18, contract item 1 in its dynamic form. A lane's
// classification drives three behaviours, so it must neither flap nor
// stick: under sustained loss at any rate that matters the lane is
// classified losing steadily, with only a handful of regime transitions
// per thousand packets, and a lane that loses nothing is never classified
// losing at all. The campaign found the mild-loss cell alternating between
// the two regimes at the worst cadence, paying the grace in one and the
// window reduction in the other; this test measures exactly that cadence.
//
// The events that count: a proven loss is a recovery the lane forced the
// sender to write or a timeout of an item the lane carried; progress is a
// clean acknowledgement of an item the lane carried. Both are driven here
// one per packet from a seeded loss process.
func TestLaneClassificationHoldsSteadyUnderSustainedLoss(t *testing.T) {
	const packets = 50_000
	const (
		// a handful of transitions per thousand packets, not dozens
		transitionsPerThousandBound = 2.0
		// the steady classification is held
		losingFractionBound = 0.95
	)
	regimes := []struct {
		name   string
		loss   *laneLossProcess
		losing bool
	}{
		{"clean or reordering, nothing lost", newLaneLossProcess(1, 0, nil), false},
		{"0.5% independent loss", newLaneLossProcess(2, 0.005, nil), true},
		{"1% independent loss", newLaneLossProcess(3, 0.01, nil), true},
		{"3% independent loss", newLaneLossProcess(4, 0.03, nil), true},
		{"campaign two-state burst loss", newLaneLossProcess(5, 0, &campaignBurstLoss), true},
	}
	for _, regime := range regimes {
		sequence, _ := newSelectiveAckRecoveryTestSequence(1, time.Unix(1_700_000_000, 0))
		transitions := 0
		losingPackets := 0
		losses := 0
		was := sequence.unreliableLaneLosing()
		for range packets {
			if regime.loss.lost() {
				losses += 1
				sequence.noteUnreliableLaneLoss()
			} else {
				sequence.noteUnreliableLaneProgress()
			}
			now := sequence.unreliableLaneLosing()
			if now != was {
				transitions += 1
				was = now
			}
			if now {
				losingPackets += 1
			}
		}
		perThousand := float64(transitions) * 1000 / packets
		fraction := float64(losingPackets) / packets
		t.Logf(
			"%s: %d losses in %d packets, classified losing %.1f%% of the time, %.2f transitions per thousand packets",
			regime.name, losses, packets, 100*fraction, perThousand,
		)
		if regime.losing {
			if transitionsPerThousandBound < perThousand {
				t.Errorf(
					"%s: the classification flaps, %.2f transitions per thousand packets against a bound of %.0f; "+
						"the sequence alternates between the losing and the clean regime and pays for both",
					regime.name, perThousand, transitionsPerThousandBound,
				)
			}
			if fraction < losingFractionBound {
				t.Errorf(
					"%s: classified losing only %.1f%% of the time against a bound of %.0f%%; "+
						"a lane losing at this rate must be held in the losing regime",
					regime.name, 100*fraction, 100*losingFractionBound,
				)
			}
		} else if transitions != 0 || losingPackets != 0 {
			t.Errorf(
				"%s: a lane that lost nothing was classified losing for %d packets with %d transitions",
				regime.name, losingPackets, transitions,
			)
		}
	}
}
