package connect

import (
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/urnetwork/connect/protocol"
)

// THROUGHPUTFIX §10.9. The provider releases a source whose socket-owned TCP
// returns stay undelivered, and the only fact it has to decide on is silence.
// These rows pin what that silence is measured over. The quantity is the
// source's own acknowledgements, not one item's admission, because admission
// is a fact about the occupancy of the provider's own queue: a live client
// that acknowledges slowly with several flows parked loses the broadcast race
// for every freed slot, a freed slot restarted the clock for the flow that won
// it, and a provider with no carrier delivered nothing to anyone.
//
// T is `ReturnSendAbandonTimeout` at test scale. The rows that assert when a
// decision lands bound it above at 1.5 T, which is the widest bound that still
// excludes the per-item clock's 1.6 T; the rows that assert a decision does not
// land run for several T with acknowledgements at a quarter of T, so a stall of
// a whole T would be needed to turn one green.

// a return sequence that admits one pack and parks every producer behind it,
// so a test decides when a slot frees
func installAdmittingProviderReturnSequence(
	t *testing.T,
	provider *RemoteUserNatProvider,
	client *Client,
	peerId Id,
) *SendSequence {
	t.Helper()
	sequence := installProviderReturnTestSequence(t, provider, client, sendSequenceId{
		Destination:         peerId,
		CompanionContract:   true,
		ForceStream:         true,
		EncryptionRole:      sequenceTlsRoleServer,
		EncryptionCompanion: false,
	})
	sequence.packs = make(chan *SendPack, 1)
	// a closed done keeps the fixture's cancellation joining immediately, as
	// the unreachable fixture does
	sequence.done = make(chan struct{})
	close(sequence.done)
	return sequence
}

// takes the admitted pack, acknowledges it as the destination would, and frees
// its slot; the pack's frames are returned to the pool the fixture owns
func ackProviderReturnPack(t *testing.T, sequence *SendSequence) {
	t.Helper()
	select {
	case pack := <-sequence.packs:
		if pack.ackTarget == nil {
			t.Fatal("an admitted socket-owned return carries no ack target, so no acknowledgement can reach the source")
		}
		pack.ackTarget.sendAckResult(0, nil)
		pack.returnFrames()
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for an admitted return pack to acknowledge")
	}
}

// takes the admitted pack and refills its slot, so the source has something
// acknowledgeable while every producer stays parked
func swapProviderReturnPack(t *testing.T, sequence *SendSequence) *SendPack {
	t.Helper()
	sequence.packMutex.Lock()
	defer sequence.packMutex.Unlock()
	select {
	case pack := <-sequence.packs:
		sequence.packs <- &SendPack{}
		return pack
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for an admitted return pack")
		return nil
	}
}

// A1. A client that acknowledges what the provider does deliver is alive,
// however long its next item has been unable to find a slot. The release must
// not fire on it, and when a slot frees the parked item is delivered.
func TestLiveClientStalledPastAbandonTimeoutIsNotRetired(t *testing.T) {
	abandonTimeout := 400 * time.Millisecond
	provider, localUserNat, client := newUnreachableSourceTestProvider(t, abandonTimeout)
	peerId := NewId()
	sequence := installAdmittingProviderReturnSequence(t, provider, client, peerId)
	nat := observeUnreachableSourceNat(t, localUserNat, peerId)
	released := make(chan struct{})
	var releasedOnce sync.Once
	provider.afterUnreachableSourceReleaseForTest = func(Id) {
		releasedOnce.Do(func() { close(released) })
	}
	var sentCount atomic.Int32
	provider.afterReturnSendAttemptForTest = func(result providerReturnSendResult) {
		if result.sent {
			sentCount.Add(1)
		}
	}

	admitted := startUnreachableProviderReturn(t, provider, peerId)
	waitProviderSourceLifecycleBarrier(t, admitted, "the first return admitted")
	pack := swapProviderReturnPack(t, sequence)
	defer pack.returnFrames()

	parked := startUnreachableProviderReturn(t, provider, peerId)
	deadline := time.After(6 * abandonTimeout)
	acknowledge := time.NewTicker(abandonTimeout / 4)
	defer acknowledge.Stop()
	for stalled := true; stalled; {
		select {
		case <-acknowledge.C:
			pack.ackTarget.sendAckResult(0, nil)
		case <-released:
			t.Fatal("the source was released while its destination was acknowledging")
		case <-parked:
			t.Fatal("the parked return was abandoned while its destination was acknowledging")
		case <-deadline:
			stalled = false
		}
	}

	// the slot frees: the parked return is delivered, not abandoned
	<-sequence.packs
	waitProviderSourceLifecycleBarrier(t, parked, "the parked return once a slot freed")
	if closedProviderTestChannel(nat.retired) {
		t.Fatal("the NAT retired the flows of a source that was acknowledging")
	}
	if sentCount.Load() < 2 {
		t.Fatalf("%d returns were delivered, want both the first and the parked one", sentCount.Load())
	}
}

// A2. The same claim where the report's zombies lived: many flows of one
// source parked on a queue that frees one slot at a time. Admission is a
// lottery with no queue order, so an individual flow's wait is unbounded by
// anything the client does; only the client's silence may decide.
func TestSlowLiveClientWithManyFlowsIsNotRetired(t *testing.T) {
	abandonTimeout := 400 * time.Millisecond
	provider, localUserNat, client := newUnreachableSourceTestProvider(t, abandonTimeout)
	peerId := NewId()
	sequence := installAdmittingProviderReturnSequence(t, provider, client, peerId)
	nat := observeUnreachableSourceNat(t, localUserNat, peerId)
	released := make(chan struct{})
	var releasedOnce sync.Once
	provider.afterUnreachableSourceReleaseForTest = func(Id) {
		releasedOnce.Do(func() { close(released) })
	}
	var sentCount atomic.Int32
	var abandonedCount atomic.Int32
	provider.afterReturnSendAttemptForTest = func(result providerReturnSendResult) {
		if result.sent {
			sentCount.Add(1)
		}
	}

	const flowCount = 8
	returned := make([]<-chan struct{}, flowCount)
	for i := range returned {
		returned[i] = startUnreachableProviderReturn(t, provider, peerId)
	}

	// one delivery acknowledged per half T; the last flow waits more than 3 T
	// for its slot, and no flow may be released for waiting
	started := time.Now()
	deliver := time.NewTicker(abandonTimeout / 2)
	defer deliver.Stop()
	for delivered := 0; delivered < flowCount; delivered++ {
		select {
		case <-deliver.C:
			ackProviderReturnPack(t, sequence)
		case <-released:
			t.Fatalf("the source was released after %s of one delivery per half timeout", time.Since(started))
		}
	}
	for i, flow := range returned {
		waitProviderSourceLifecycleBarrier(t, flow, "a parked flow of a slow live client")
		_ = i
	}
	elapsed := time.Since(started)

	if closedProviderTestChannel(released) {
		t.Fatal("the source was released although every delivery was acknowledged")
	}
	if closedProviderTestChannel(nat.retired) {
		t.Fatal("the NAT retired the flows of a slow live client")
	}
	if elapsed < 3*abandonTimeout {
		t.Fatalf("the eight flows drained in %s, less than the 3 timeouts the row needs an individual wait to exceed", elapsed)
	}
	if int(sentCount.Load()) < flowCount {
		t.Fatalf("%d of %d returns were delivered", sentCount.Load(), flowCount)
	}
	if 0 < abandonedCount.Load() {
		t.Fatalf("%d returns were abandoned", abandonedCount.Load())
	}
}

// A3, and H7. A client that was downloading and then died: its last
// acknowledgement is the last moment the provider has evidence of it, and the
// release must land one timeout after that, whatever its queue did in between.
// The per-item clock restarted on the item that won the freed slot, so main
// releases a timeout after that admission instead.
func TestReleaseTracksClientDeathNotItemProgress(t *testing.T) {
	abandonTimeout := 200 * time.Millisecond
	provider, localUserNat, client := newUnreachableSourceTestProvider(t, abandonTimeout)
	peerId := NewId()
	sequence := installAdmittingProviderReturnSequence(t, provider, client, peerId)
	nat := observeUnreachableSourceNat(t, localUserNat, peerId)
	nat.finishFlow()

	acknowledged := startUnreachableProviderReturn(t, provider, peerId)
	waitProviderSourceLifecycleBarrier(t, acknowledged, "the acknowledged return")
	ackProviderReturnPack(t, sequence)
	lastAck := time.Now()

	// the client is gone from here: one more return is admitted into the freed
	// slot, one parks behind it, the slot frees again at 0.6 T, and nothing is
	// ever acknowledged again
	outstanding := startUnreachableProviderReturn(t, provider, peerId)
	waitProviderSourceLifecycleBarrier(t, outstanding, "the unacknowledged return")
	parked := startUnreachableProviderReturn(t, provider, peerId)
	time.Sleep(6 * abandonTimeout / 10)
	ackedPack := <-sequence.packs
	defer ackedPack.returnFrames()
	waitProviderSourceLifecycleBarrier(t, parked, "the return admitted into the freed slot")

	abandoned := startUnreachableProviderReturn(t, provider, peerId)
	waitProviderSourceLifecycleBarrier(t, abandoned, "the release decision")
	elapsed := time.Since(lastAck)

	if elapsed < abandonTimeout || 3*abandonTimeout/2 < elapsed {
		t.Fatalf(
			"the release decision landed %s after the destination's last acknowledgement, want one timeout of %s and at most 1.5 of them; a clock restarted by a freed slot lands at 1.6",
			elapsed,
			abandonTimeout,
		)
	}
}

// A3b, the characterisation the row above needs beside it. A source that has
// never acknowledged anything has no last acknowledgement to measure from, so
// its first admission is the floor and the decision lands 1.6 T after the
// producer first parked. That is the design (§10.6), not a regression: the
// provider has delivered something it has not yet been told was lost, and the
// count rising from zero is the only evidence of when that began. It holds on
// both trees, so it documents rather than guards.
func TestFirstAdmissionAfterAParkedStartRestartsTheClock(t *testing.T) {
	abandonTimeout := 200 * time.Millisecond
	provider, localUserNat, client := newUnreachableSourceTestProvider(t, abandonTimeout)
	peerId := NewId()
	sequence := installAdmittingProviderReturnSequence(t, provider, client, peerId)
	// the slot starts full, so the first return parks
	sequence.packs <- &SendPack{}
	nat := observeUnreachableSourceNat(t, localUserNat, peerId)
	nat.finishFlow()

	parkedAt := time.Now()
	parked := startUnreachableProviderReturn(t, provider, peerId)
	time.Sleep(6 * abandonTimeout / 10)
	<-sequence.packs
	waitProviderSourceLifecycleBarrier(t, parked, "the first return admitted after parking")

	abandoned := startUnreachableProviderReturn(t, provider, peerId)
	waitProviderSourceLifecycleBarrier(t, abandoned, "the release decision")
	elapsed := time.Since(parkedAt)

	if elapsed < 3*abandonTimeout/2 || 2*abandonTimeout < elapsed {
		t.Fatalf(
			"the release decision landed %s after the first return parked, want about 1.6 of the %s timeout: 0.6 to the first admission and one from there",
			elapsed,
			abandonTimeout,
		)
	}
}

// A4. A provider with no carrier has delivered nothing to anyone, so its
// silence is its own and says nothing about any client. Main read every client
// as gone in that state and released all of them at once.
func TestSilenceIsInadmissibleWithoutACarrier(t *testing.T) {
	abandonTimeout := 200 * time.Millisecond
	provider, localUserNat, client := newUnreachableSourceTestProvider(t, abandonTimeout)
	var carrier atomic.Bool
	provider.hasActiveTransportForTest = carrier.Load
	peerId := NewId()
	installUnreachableProviderReturnSequence(t, provider, client, peerId)
	nat := observeUnreachableSourceNat(t, localUserNat, peerId)
	nat.finishFlow()

	abandoned := startUnreachableProviderReturn(t, provider, peerId)
	select {
	case <-abandoned:
		t.Fatal("the source was released while the provider had no carrier to reach it with")
	case <-time.After(5 * abandonTimeout):
	}

	carrierAt := time.Now()
	carrier.Store(true)
	waitProviderSourceLifecycleBarrier(t, abandoned, "the release decision once the carrier returned")
	elapsed := time.Since(carrierAt)

	if elapsed < abandonTimeout || 3*abandonTimeout/2 < elapsed {
		t.Fatalf(
			"the release decision landed %s after the carrier returned, want one timeout of %s and at most 1.5 of them; the silence before the carrier is the provider's own",
			elapsed,
			abandonTimeout,
		)
	}
}

// starts one datagram return toward peerId, the UDP mirror of the socket-owned
// TCP return above; the channel closes when the producer returns
func startDatagramProviderReturn(
	t *testing.T,
	provider *RemoteUserNatProvider,
	peerId Id,
) <-chan struct{} {
	t.Helper()
	packet := MessagePoolCopy(craftSecurityPacket(
		IpProtocolUdp,
		net.ParseIP("203.0.113.7"),
		53,
		net.ParseIP("10.0.0.9"),
		42001,
		false,
		[]byte("unreachable destination"),
	))
	ipPath, err := ParseIpPath(packet)
	if err != nil {
		MessagePoolReturn(packet)
		t.Fatalf("parse provider UDP return packet: %v", err)
	}
	producerReturned := make(chan struct{})
	go func() {
		defer close(producerReturned)
		// borrowed for the call; see the TCP helper this mirrors
		defer MessagePoolReturn(packet)
		provider.receiveTransferWithRecovery(
			SourceId(peerId),
			TransferKey{
				ForceStream:         true,
				EncryptionRole:      protocol.SequenceRole_SequenceRoleServer,
				EncryptionCompanion: false,
			},
			protocol.ProvideMode_Public,
			receiveRecoveryModeNonblocking,
			ipPath,
			packet,
		)
	}()
	return producerReturned
}

// THROUGHPUTFIX H9, the bounding fact stated positively rather than read off
// the source. The unbounded retry that the abandon timeout exists to bound is
// entered only by a socket-owned TCP return: the provider already consumed
// those upstream bytes and no layer below can reproduce them. A datagram
// return makes no such promise, so it is refused once and its producer returns,
// and UDP has no zombie flows to retire and nothing for the release to decide.
// A refactor that widened the retry to every return would make every UDP flow
// of a gone client hold a producer for the abandon timeout; this row is what
// would catch it.
func TestDatagramReturnDoesNotEnterTheAbandonRetry(t *testing.T) {
	abandonTimeout := 2 * time.Second
	provider, localUserNat, client := newUnreachableSourceTestProvider(t, abandonTimeout)
	peerId := NewId()
	installUnreachableProviderReturnSequence(t, provider, client, peerId)
	nat := observeUnreachableSourceNat(t, localUserNat, peerId)
	nat.finishFlow()
	released := make(chan struct{})
	var releasedOnce sync.Once
	provider.afterUnreachableSourceReleaseForTest = func(Id) {
		releasedOnce.Do(func() { close(released) })
	}
	var attemptCount atomic.Int32
	provider.afterReturnSendAttemptForTest = func(providerReturnSendResult) {
		attemptCount.Add(1)
	}

	started := time.Now()
	producerReturned := startDatagramProviderReturn(t, provider, peerId)
	select {
	case <-producerReturned:
	case <-time.After(abandonTimeout / 4):
		t.Fatalf("the datagram return had not returned %s after it was refused; it must not wait on the socket-owned retry", time.Since(started))
	}

	// at most one: a datagram return either never reaches the socket-owned
	// retry loop, which is where it is today, or is refused by it once
	if attempts := attemptCount.Load(); 1 < attempts {
		t.Errorf("the refused datagram return made %d admission attempts, so it entered the retry loop that only a socket-owned return may enter", attempts)
	}
	t.Logf("the refused datagram return returned in %s with %d retry-loop attempts", time.Since(started), attemptCount.Load())
	if closedProviderTestChannel(released) {
		t.Error("a refused datagram return released its source; only a socket-owned return may decide that")
	}
	if closedProviderTestChannel(nat.retired) {
		t.Error("a refused datagram return retired its source's NAT flows")
	}
}
