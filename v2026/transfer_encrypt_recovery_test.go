package connect

// Deterministic pin for epoch-desync recovery latency (DESIGNNOTES2.md §8.5-1
// tradeoff, and the regression it shipped): with the plaintext-during-rekey
// fallback removed, a sender holding an established cipher keeps sealing at a
// peer that lost its responder session entirely (peer instance restart) — the
// wraps are undecryptable there and dropped. Recovery must be EVENT-DRIVEN:
// the receiver nacks the unknown wrap (EncryptedControlUnknownWrapNack,
// echoing its current epoch or none) and the sealing side restarts its
// handshake, healing in on the order of a round trip plus a handshake.
//
// Without the nack the sender only re-initiates via its send-sequence
// lifecycle (AcquireForSend on sequence creation): an ACTIVE sequence with
// unacked backlog resends into the void until AckTimeout (default 60s) tears
// it down. In the integration chaos suite that stall races the 60s progress
// watchdog — TestConnectWithSymmetricContractsWithChaosWithNewInstanceEncrypted
// went from 66s (pre §8.5) to 5/5 attempt timeouts (r18-1), and ~50% attempt
// failures in isolation. This test constructs the desync directly and bounds
// the recovery.

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// waitForSealedSession polls until the client reports a sealed peer session,
// so a test's desync injection provably targets an ESTABLISHED sender — not a
// still-plaintext pre-establishment window that would make the scenario
// vacuous (opportunistic delivers plaintext before establishment, so a drop
// before sealing recovers "for free" with no recovery machinery at all).
func waitForSealedSession(t *testing.T, ctx context.Context, client *Client) {
	t.Helper()
	sealedDeadline := time.Now().Add(30 * time.Second)
	for {
		sealed := false
		for _, state := range client.encryptionSessionManager.PeerEncryptionStates() {
			if state.Sealed {
				sealed = true
				break
			}
		}
		if sealed {
			return
		}
		if sealedDeadline.Before(time.Now()) {
			t.Fatal("the client never reported a sealed session")
		}
		select {
		case <-ctx.Done():
			t.Fatal("ctx done before the session sealed")
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// TestEncryptedPeerSessionLossRecovery: an established, actively-sending flow
// must recover promptly (well under the sequence AckTimeout) after the peer
// loses all of its encryption-session state.
func TestEncryptedPeerSessionLossRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	a, b, aClientId, bClientId, _, receivesB := requiredGatePair(
		ctx, EncryptionModeOpportunistic, EncryptionModeOpportunistic, nil, true)
	defer a.Cancel()
	defer b.Cancel()

	// establish: the first message both opens the contract path and completes
	// the handshake (opportunistic seals from establishment on)
	if ok := a.SendWithTimeout(
		requiredGateFrame(t, "before"),
		bClientId,
		func(error) {},
		30*time.Second,
	); !ok {
		t.Fatal("initial send refused")
	}
	select {
	case got := <-receivesB:
		AssertEqual(t, "before", got)
	case <-time.After(30 * time.Second):
		t.Fatal("initial delivery did not arrive")
	}

	// the desync below must target an established sender, not a race
	// against establishment itself
	waitForSealedSession(t, ctx, a)

	// the peer restarts: b forgets every session for a, while a's established
	// epoch keeps serving its sends — which b can no longer read
	b.encryptionSessionManager.Testing_DropSessions(aClientId)

	// an active flow: keep sending; the recovery bound is the assertion.
	// 20s sits far above a nack-driven recovery (a round trip + handshake +
	// resend interval) and far below the sequence-lifecycle fallback
	// (AckTimeout 60s + re-establishment).
	recoveryDeadline := time.Now().Add(20 * time.Second)
	recovered := false
	sendIndex := 0
	for !recovered && time.Now().Before(recoveryDeadline) {
		a.SendWithTimeout(
			requiredGateFrame(t, fmt.Sprintf("after-%d", sendIndex)),
			bClientId,
			func(error) {},
			0,
		)
		sendIndex += 1
		select {
		case got := <-receivesB:
			if got != "before" {
				recovered = true
			}
		case <-time.After(200 * time.Millisecond):
		}
	}
	if !recovered {
		t.Fatalf(
			"no delivery within 20s of the peer losing its sessions (%d sends): recovery is waiting on the send-sequence lifecycle (AckTimeout) instead of an unknown-wrap nack",
			sendIndex,
		)
	}
}

// sessionEstablishedEpochId reads a session's established epoch id under its
// lock, zero when none.
func sessionEstablishedEpochId(s *peerEncryptionSession) Id {
	s.stateLock.Lock()
	defer s.stateLock.Unlock()
	if s.establishedEpoch == nil {
		return Id{}
	}
	return s.establishedEpoch.epochId
}

// TestNackEchoingCurrentEpochIsIgnored: a nack that names the sealer's own
// established epoch is corruption-in-flight (or stale/forged) evidence, not
// desync — the epoch must NOT be demoted. This is what makes the nack safe
// under chaos: corrupted wraps produce nacks that echo the epoch both sides
// share, and demoting on those would turn every corrupted frame into a
// rekey.
func TestNackEchoingCurrentEpochIsIgnored(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	a, b, _, bClientId, _, receivesB := requiredGatePair(
		ctx, EncryptionModeRequired, EncryptionModeRequired, nil, true)
	defer a.Cancel()
	defer b.Cancel()

	if ok := a.SendWithTimeout(
		requiredGateFrame(t, "establish"),
		bClientId,
		func(error) {},
		30*time.Second,
	); !ok {
		t.Fatal("initial send refused")
	}
	select {
	case <-receivesB:
	case <-time.After(30 * time.Second):
		t.Fatal("initial delivery did not arrive")
	}

	session := a.encryptionSessionManager.Lookup(bClientId, sequenceTlsRoleClient, false)
	if session == nil {
		t.Fatal("no client session")
	}
	establishedEpochId := sessionEstablishedEpochId(session)
	if establishedEpochId == (Id{}) {
		t.Fatal("no established epoch")
	}

	session.handleUnknownWrapNack(establishedEpochId)

	if got := sessionEstablishedEpochId(session); got != establishedEpochId {
		t.Fatalf(
			"a nack echoing the current epoch demoted it (%s -> %s): corruption in flight must not force rekeys",
			establishedEpochId, got,
		)
	}
}

// TestForgedNackCausesNoOutage: an attacker (or a stale replay) injecting an
// unknown-wrap nack against a healthy Required session costs at most a
// rate-limited re-handshake — delivery continues and the session re-seals.
// Combined with the entry gate and write backstop (pinned in the gate tests),
// this bounds the forged-nack blast radius under Required to handshake
// churn: never an outage, never plaintext.
func TestForgedNackCausesNoOutage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	a, b, _, bClientId, _, receivesB := requiredGatePair(
		ctx, EncryptionModeRequired, EncryptionModeRequired, nil, true)
	defer a.Cancel()
	defer b.Cancel()

	if ok := a.SendWithTimeout(
		requiredGateFrame(t, "establish"),
		bClientId,
		func(error) {},
		30*time.Second,
	); !ok {
		t.Fatal("initial send refused")
	}
	select {
	case <-receivesB:
	case <-time.After(30 * time.Second):
		t.Fatal("initial delivery did not arrive")
	}

	session := a.encryptionSessionManager.Lookup(bClientId, sequenceTlsRoleClient, false)
	if session == nil {
		t.Fatal("no client session")
	}
	// the forgery: no epoch named (the strongest desync claim)
	session.handleUnknownWrapNack(Id{})

	// delivery continues: the demoted session re-handshakes against the live
	// peer and the entry gate holds sends sealed-only in the interim
	if ok := a.SendWithTimeout(
		requiredGateFrame(t, "after-forged-nack"),
		bClientId,
		func(error) {},
		20*time.Second,
	); !ok {
		t.Fatal("send refused after a forged nack: the forgery caused an outage")
	}
	select {
	case got := <-receivesB:
		AssertEqual(t, "after-forged-nack", got)
	case <-time.After(20 * time.Second):
		t.Fatal("no delivery after a forged nack: the forgery caused an outage")
	}
}

// sessionNackRestartStamp reads the nack-restart cooldown stamp under the
// session lock. A single stamp value across a nack flood proves a single
// demote was admitted.
func sessionNackRestartStamp(s *peerEncryptionSession) time.Time {
	s.stateLock.Lock()
	defer s.stateLock.Unlock()
	return s.nextUnknownWrapNackRestartTime
}

// sessionLastNackEmitStamp reads the emission rate-limit stamp under the
// session lock.
func sessionLastNackEmitStamp(s *peerEncryptionSession) time.Time {
	s.stateLock.Lock()
	defer s.stateLock.Unlock()
	return s.lastUnknownWrapNackTime
}

// TestNackStormDuringRecoveryConverges pins the livelock discovered while
// building this recovery: nacks legitimately KEEP ARRIVING while a recovery
// handshake is in flight (the peer nacks every old-epoch resend it sees), and
// the first design — restart the handshake on every admissible nack —
// abandoned the completing epoch each time, milliseconds before promotion,
// forever. The handler's guards (established-nil while a recovery is
// unresolved, the restart cooldown after) must let recovery converge under a
// continuous storm, and the session must re-seal once the storm stops.
func TestNackStormDuringRecoveryConverges(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	a, b, aClientId, bClientId, _, receivesB := requiredGatePair(
		ctx, EncryptionModeOpportunistic, EncryptionModeOpportunistic, nil, true)
	defer a.Cancel()
	defer b.Cancel()

	if ok := a.SendWithTimeout(
		requiredGateFrame(t, "before"),
		bClientId,
		func(error) {},
		30*time.Second,
	); !ok {
		t.Fatal("initial send refused")
	}
	select {
	case <-receivesB:
	case <-time.After(30 * time.Second):
		t.Fatal("initial delivery did not arrive")
	}

	session := a.encryptionSessionManager.Lookup(bClientId, sequenceTlsRoleClient, false)
	if session == nil {
		t.Fatal("no client session")
	}

	// the desync must hit an ESTABLISHED sender: dropped earlier, the
	// opportunistic pre-establishment plaintext window recovers "for free"
	// and the storm exercises nothing
	waitForSealedSession(t, ctx, a)

	// the peer restarts, and the storm begins: a nack every 25ms, far denser
	// than the organic once-per-second emission, modeling the stale-nack
	// pileup the in-flight recovery observed (plus adversarial replay)
	b.encryptionSessionManager.Testing_DropSessions(aClientId)
	stormCtx, stormCancel := context.WithCancel(ctx)
	defer stormCancel()
	go func() {
		for {
			select {
			case <-stormCtx.Done():
				return
			case <-time.After(25 * time.Millisecond):
			}
			session.handleUnknownWrapNack(Id{})
		}
	}()

	// delivery must still recover promptly under the storm
	recoveryDeadline := time.Now().Add(20 * time.Second)
	recovered := false
	sendIndex := 0
	for !recovered && time.Now().Before(recoveryDeadline) {
		a.SendWithTimeout(
			requiredGateFrame(t, fmt.Sprintf("storm-%d", sendIndex)),
			bClientId,
			func(error) {},
			0,
		)
		sendIndex += 1
		select {
		case got := <-receivesB:
			if got != "before" {
				recovered = true
			}
		case <-time.After(200 * time.Millisecond):
		}
	}
	if !recovered {
		t.Fatalf(
			"no delivery within 20s under a nack storm (%d sends): admissible nacks are destroying the in-flight recovery (the livelock this design replaced)",
			sendIndex,
		)
	}

	// and once the storm stops, the session must re-seal within the cooldown
	// horizon: the storm may only delay sealing, never destroy the ability
	stormCancel()
	sealedDeadline := time.Now().Add(20 * time.Second)
	for {
		if sessionEstablishedEpochId(session) != (Id{}) {
			break
		}
		if sealedDeadline.Before(time.Now()) {
			t.Fatal("the session never re-sealed after the storm stopped")
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// TestNackDemoteChurnIsBounded pins the quantitative churn bound: a flood of
// admissible forged nacks against an established session admits exactly ONE
// demote — the established-nil guard absorbs the flood while the recovery is
// unresolved, and the restart cooldown absorbs it after. The observable is
// the cooldown stamp: stamped once at the admitted demote and untouched by
// every subsequent flood call inside the window.
func TestNackDemoteChurnIsBounded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	a, b, _, bClientId, _, receivesB := requiredGatePair(
		ctx, EncryptionModeRequired, EncryptionModeRequired, nil, true)
	defer a.Cancel()
	defer b.Cancel()

	if ok := a.SendWithTimeout(
		requiredGateFrame(t, "establish"),
		bClientId,
		func(error) {},
		30*time.Second,
	); !ok {
		t.Fatal("initial send refused")
	}
	select {
	case <-receivesB:
	case <-time.After(30 * time.Second):
		t.Fatal("initial delivery did not arrive")
	}

	session := a.encryptionSessionManager.Lookup(bClientId, sequenceTlsRoleClient, false)
	if session == nil {
		t.Fatal("no client session")
	}
	if sessionEstablishedEpochId(session) == (Id{}) {
		t.Fatal("no established epoch")
	}

	// flood: 100 forged nacks over ~1s, all admissible on their face
	// (unknown epoch), against the default 5s cooldown
	for i := 0; i < 100; i += 1 {
		session.handleUnknownWrapNack(Id{})
		if i == 0 {
			if sessionNackRestartStamp(session).IsZero() {
				t.Fatal("the first admissible nack did not stamp the cooldown (no demote admitted)")
			}
		}
		time.Sleep(10 * time.Millisecond)
	}

	firstStamp := sessionNackRestartStamp(session)
	if firstStamp.IsZero() {
		t.Fatal("no demote was admitted")
	}
	// the whole flood ran inside the first cooldown window, so the stamp must
	// not have advanced: exactly one demote for the entire flood
	if time.Now().After(firstStamp) {
		t.Fatal("test assumption broken: the flood outlasted the cooldown window; shorten the flood or lengthen the cooldown")
	}
	// re-read after the flood: unchanged stamp == no second demote
	if got := sessionNackRestartStamp(session); !got.Equal(firstStamp) {
		t.Fatalf(
			"the cooldown stamp advanced during the flood (%v -> %v): more than one demote was admitted inside one cooldown window",
			firstStamp, got,
		)
	}
}

// TestUnknownWrapNackEmissionIsRateLimited pins the receive-side half of the
// pacing: a burst of undecryptable wraps emits at most one nack per
// UnknownWrapNackMinInterval, so a sealing burst against a lost peer costs
// one control per interval, not one per frame.
func TestUnknownWrapNackEmissionIsRateLimited(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	minInterval := 300 * time.Millisecond
	a, b, _, bClientId, _, receivesB := requiredGatePair(
		ctx, EncryptionModeOpportunistic, EncryptionModeOpportunistic,
		func(s *ClientSettings) {
			s.EncryptionSettings.UnknownWrapNackMinInterval = minInterval
		},
		true)
	defer a.Cancel()
	defer b.Cancel()

	if ok := a.SendWithTimeout(
		requiredGateFrame(t, "establish"),
		bClientId,
		func(error) {},
		30*time.Second,
	); !ok {
		t.Fatal("initial send refused")
	}
	select {
	case <-receivesB:
	case <-time.After(30 * time.Second):
		t.Fatal("initial delivery did not arrive")
	}

	session := a.encryptionSessionManager.Lookup(bClientId, sequenceTlsRoleClient, false)
	if session == nil {
		t.Fatal("no client session")
	}

	// burst: three back-to-back emissions; only the first may stamp
	session.sendUnknownWrapNack()
	firstStamp := sessionLastNackEmitStamp(session)
	if firstStamp.IsZero() {
		t.Fatal("the first emission did not stamp the rate limit")
	}
	session.sendUnknownWrapNack()
	session.sendUnknownWrapNack()
	if got := sessionLastNackEmitStamp(session); !got.Equal(firstStamp) {
		t.Fatalf(
			"burst emissions re-stamped the rate limit (%v -> %v): more than one nack per interval",
			firstStamp, got,
		)
	}

	// after the interval elapses, the next emission is admitted
	time.Sleep(minInterval + 50*time.Millisecond)
	session.sendUnknownWrapNack()
	if got := sessionLastNackEmitStamp(session); got.Equal(firstStamp) {
		t.Fatal("an emission after the interval elapsed was not admitted")
	}
}

// An undecryptable wrap can race the responder's identity-proof completion:
// its matching handshake epoch is live but not readable yet. The nack must
// name that generation so the already-sealed initiator recognizes normal
// convergence instead of demoting the cipher and putting a new ClientHello
// behind the dropped application's receive-sequence gap.
func TestUnknownWrapNackNamesInFlightEpoch(t *testing.T) {
	epochId := NewId()
	var nackedEpochId Id
	session := &peerEncryptionSession{
		settings: DefaultEncryptionSettings(),
		epoch: &tlsHandshakeEpoch{
			epochId:           epochId,
			establishmentDone: make(chan struct{}),
		},
		unknownWrapNackForTest: func(ec *protocol.EncryptedControl) {
			if parsed, err := IdFromBytes(ec.GetEpochId()); err == nil {
				nackedEpochId = parsed
			}
		},
	}

	session.sendUnknownWrapNack()

	if nackedEpochId != epochId {
		t.Fatalf(
			"nack epoch = %s, want live handshake epoch %s; empty feedback falsely claims the responder lost all session state",
			nackedEpochId,
			epochId,
		)
	}
}

// A finished but unestablished epoch is not convergence evidence. Leaving the
// nack generation unset is what tells a sealed initiator to replace state the
// responder can no longer complete or read.
func TestUnknownWrapNackDoesNotNameFailedEpoch(t *testing.T) {
	epochId := NewId()
	establishmentDone := make(chan struct{})
	close(establishmentDone)
	var nackedEpochBytes []byte
	session := &peerEncryptionSession{
		settings: DefaultEncryptionSettings(),
		epoch: &tlsHandshakeEpoch{
			epochId:           epochId,
			establishmentDone: establishmentDone,
		},
		unknownWrapNackForTest: func(ec *protocol.EncryptedControl) {
			nackedEpochBytes = bytes.Clone(ec.GetEpochId())
		},
	}

	session.sendUnknownWrapNack()

	if len(nackedEpochBytes) != 0 {
		t.Fatalf(
			"failed epoch was advertised as live: got %x, want an unset nack generation",
			nackedEpochBytes,
		)
	}
}

// TestNackWithStaleRealEpochDoesNotDemote pins the refinement behind the
// Dns*Encrypted Required retry pattern: a nack carrying a REAL but older
// (client-minted) epoch means the peer is alive and transiently lagging an
// in-flight rekey — its copy of the current epoch's ClientHello is already on
// the reliable EncryptedControl sequence toward it, so convergence is
// guaranteed. The sealer must NOT demote its established epoch on such a nack:
// demoting moves the target (tears down the healthy epoch, starts a newer
// one) and, under a slow transport, oscillates into repeated rehandshakes.
// Only an EMPTY nacked epoch (peer has no session, cannot converge) earns a
// demote — asserted here as the contrast, and by
// TestEncryptedPeerSessionLossRecovery / TestForgedNackCausesNoOutage.
func TestNackWithStaleRealEpochDoesNotDemote(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	a, b, _, bClientId, _, receivesB := requiredGatePair(
		ctx, EncryptionModeRequired, EncryptionModeRequired, nil, true)
	defer a.Cancel()
	defer b.Cancel()

	if ok := a.SendWithTimeout(
		requiredGateFrame(t, "establish"),
		bClientId,
		func(error) {},
		30*time.Second,
	); !ok {
		t.Fatal("initial send refused")
	}
	select {
	case <-receivesB:
	case <-time.After(30 * time.Second):
		t.Fatal("initial delivery did not arrive")
	}

	session := a.encryptionSessionManager.Lookup(bClientId, sequenceTlsRoleClient, false)
	if session == nil {
		t.Fatal("no client session")
	}
	establishedEpochId := sessionEstablishedEpochId(session)
	if establishedEpochId == (Id{}) {
		t.Fatal("no established epoch")
	}

	// a real but DIFFERENT epoch: a distinct client-minted generation the
	// peer reports it can still read (the transient-rekey-lag signal)
	staleRealEpoch := NewId()
	if staleRealEpoch == establishedEpochId {
		t.Fatal("generated epoch collided with the established one")
	}
	session.handleUnknownWrapNack(staleRealEpoch)

	if got := sessionEstablishedEpochId(session); got != establishedEpochId {
		t.Fatalf(
			"a nack with a real (older) epoch demoted the established epoch (%s -> %s): the peer is alive and converging via the reliable ClientHello resend; demoting moves the target and oscillates under slow transports",
			establishedEpochId, got,
		)
	}

	// and traffic keeps flowing on the undisturbed epoch
	if ok := a.SendWithTimeout(
		requiredGateFrame(t, "after-stale-nack"),
		bClientId,
		func(error) {},
		20*time.Second,
	); !ok {
		t.Fatal("send refused after a stale-real-epoch nack")
	}
	select {
	case got := <-receivesB:
		AssertEqual(t, "after-stale-nack", got)
	case <-time.After(20 * time.Second):
		t.Fatal("no delivery after a stale-real-epoch nack")
	}
}
