package connect

import (
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// installs a return sequence that never admits a Pack and whose cancellation
// joins immediately, the Transfer shape of a destination that disappeared with
// socket-owned return data in flight
func installUnreachableProviderReturnSequence(
	t *testing.T,
	provider *RemoteUserNatProvider,
	client *Client,
	peerId Id,
) {
	t.Helper()
	sequence := installProviderReturnTestSequence(t, provider, client, sendSequenceId{
		Destination:         peerId,
		CompanionContract:   true,
		ForceStream:         true,
		EncryptionRole:      sequenceTlsRoleServer,
		EncryptionCompanion: false,
	})
	sequence.packs = make(chan *SendPack)
	sequence.done = make(chan struct{})
	close(sequence.done)
}

// starts one socket-owned TCP return toward peerId; the channel closes when the
// producer returns
func startUnreachableProviderReturn(
	t *testing.T,
	provider *RemoteUserNatProvider,
	peerId Id,
) <-chan struct{} {
	t.Helper()
	packet := MessagePoolCopy(craftSecurityPacket(
		IpProtocolTcp,
		net.ParseIP("203.0.113.7"),
		8080,
		net.ParseIP("10.0.0.9"),
		42001,
		false,
		[]byte("unreachable destination"),
	))
	ipPath, err := ParseIpPath(packet)
	if err != nil {
		MessagePoolReturn(packet)
		t.Fatalf("parse provider TCP return packet: %v", err)
	}
	producerReturned := make(chan struct{})
	go func() {
		defer close(producerReturned)
		withBorrowedMessage(packet, func(packet []byte) {
			provider.receiveTransferWithRecovery(
				SourceId(peerId),
				TransferKey{
					ForceStream:         true,
					EncryptionRole:      protocol.SequenceRole_SequenceRoleServer,
					EncryptionCompanion: false,
				},
				protocol.ProvideMode_Public,
				receiveRecoveryModeTcpSocket,
				ipPath,
				packet,
			)
		})
	}()
	return producerReturned
}

// observes the NAT tombstone for one source; its retirement parks on flowDone
// until the test releases it, standing in for a live flow that must be joined
type unreachableSourceNatObserver struct {
	flowDone     chan struct{}
	flowDoneOnce sync.Once
	retired      chan struct{}
	readmitted   chan struct{}
}

// finishes the observed flow so its retirement join can complete
func (self *unreachableSourceNatObserver) finishFlow() {
	self.flowDoneOnce.Do(func() { close(self.flowDone) })
}

func observeUnreachableSourceNat(
	t *testing.T,
	localUserNat *LocalUserNat,
	peerId Id,
) *unreachableSourceNatObserver {
	t.Helper()
	observer := &unreachableSourceNatObserver{
		flowDone:   make(chan struct{}),
		retired:    make(chan struct{}),
		readmitted: make(chan struct{}),
	}
	var retiredOnce, readmittedOnce sync.Once
	unsub := localUserNat.addSourceRetirementCallback(func(sourceId Id, retired bool) []<-chan struct{} {
		if sourceId != peerId {
			return nil
		}
		if !retired {
			readmittedOnce.Do(func() { close(observer.readmitted) })
			return nil
		}
		retiredOnce.Do(func() { close(observer.retired) })
		return []<-chan struct{}{observer.flowDone}
	})
	t.Cleanup(func() {
		observer.finishFlow()
		unsub()
	})
	return observer
}

func closedProviderTestChannel(channel <-chan struct{}) bool {
	select {
	case <-channel:
		return true
	default:
		return false
	}
}

// returnSendTestClock stands in for the clock that evaluates a source's
// acknowledgement silence. The first retry moves it past any abandon timeout, so
// every later abandon check sees an expired stall without the test waiting.
type returnSendTestClock struct {
	now          atomic.Int64
	retryCount   atomic.Int64
	expiredRetry chan struct{}
	expiredOnce  sync.Once
}

func (self *returnSendTestClock) Now() time.Time {
	return time.Unix(0, self.now.Load())
}

// retried runs before each retry, i.e. only after an abandon check declined to
// release. The first call expires the stall; the second proves that an abandon
// check has already seen the expired stall and still declined.
func (self *returnSendTestClock) retried() {
	switch self.retryCount.Add(1) {
	case 1:
		self.now.Add(int64(1000 * time.Hour))
	case 2:
		self.expiredOnce.Do(func() { close(self.expiredRetry) })
	}
}

func newUnreachableSourceTestProvider(
	t *testing.T,
	abandonTimeout time.Duration,
) (*RemoteUserNatProvider, *LocalUserNat, *Client) {
	// every test built on this fixture reconciles pool ownership at its
	// boundary, which catches a buffer taken and never returned as well as one
	// returned that something else owned
	assertMessagePoolOwnership(t)
	provider, localUserNat, client := newProviderSourceLifecycleTestFixture(t, func(settings *RemoteUserNatProviderSettings) {
		settings.WriteTimeout = 0
		settings.ReturnSendRetryTimeout = time.Millisecond
		settings.ReturnSendAbandonTimeout = abandonTimeout
	})
	// the backend state is process-wide and other tests' clients trip it;
	// these tests decide it explicitly. The fixture client registers no
	// transport, and silence without a carrier is inadmissible
	// (THROUGHPUTFIX §10), so these tests hold a carrier explicitly too.
	provider.backendDegradedForTest = func() bool { return false }
	provider.hasActiveTransportForTest = func() bool { return true }
	return provider, localUserNat, client
}

func newClockedUnreachableSourceTestProvider(
	t *testing.T,
	abandonTimeout time.Duration,
) (*RemoteUserNatProvider, *LocalUserNat, *Client, *returnSendTestClock) {
	t.Helper()
	provider, localUserNat, client := newUnreachableSourceTestProvider(t, abandonTimeout)
	clock := &returnSendTestClock{expiredRetry: make(chan struct{})}
	clock.now.Store(time.Now().UnixNano())
	provider.returnSendNowForTest = clock.Now
	provider.beforeTcpReturnSendRetryForTest = clock.retried
	return provider, localUserNat, client, clock
}

// A socket-owned TCP return retries past an ordinary Ack timeout because the
// provider already consumed those upstream bytes. It must not retry forever
// against a destination that is gone: those flows never tear down, keep
// retransmitting, and throttle every other client of the provider until it
// restarts. After ReturnSendAbandonTimeout the provider releases the source.
// While the release joins the source's flows the source is refused, exactly
// as during terminal retirement; afterwards it is readmitted, because a client
// that reconnects keeps its id.
func TestRemoteUserNatProviderReleasesUnreachableTcpReturnSource(t *testing.T) {
	provider, localUserNat, client, _ := newClockedUnreachableSourceTestProvider(t, time.Hour)
	peerId := NewId()
	installUnreachableProviderReturnSequence(t, provider, client, peerId)
	nat := observeUnreachableSourceNat(t, localUserNat, peerId)
	released := make(chan struct{})
	var releasedOnce sync.Once
	provider.afterUnreachableSourceReleaseForTest = func(sourceId Id) {
		if sourceId == peerId {
			releasedOnce.Do(func() { close(released) })
		}
	}

	producerReturned := startUnreachableProviderReturn(t, provider, peerId)
	waitProviderSourceLifecycleBarrier(t, producerReturned, "abandoned TCP return producer")
	waitProviderSourceLifecycleBarrier(t, nat.retired, "NAT flow retirement for the unreachable source")

	// the release is joining a flow: the source is refused, not re-admitted
	if sourceLifecycle := provider.acquireSourceLifecycle(peerId); sourceLifecycle != nil {
		provider.releaseSourceLifecycle(peerId, sourceLifecycle)
		t.Fatal("source admitted while its release was still joining flows")
	}
	requireProviderSourceLifecycleBlocked(t, released, "unreachable source release")
	if closedProviderTestChannel(nat.readmitted) {
		t.Fatal("NAT readmitted the source before its flows were joined")
	}

	nat.finishFlow()
	waitProviderSourceLifecycleBarrier(t, released, "unreachable source release")
	if !closedProviderTestChannel(nat.readmitted) {
		t.Fatal("NAT did not readmit the released source")
	}
	sourceLifecycle := provider.acquireSourceLifecycle(peerId)
	if sourceLifecycle == nil {
		t.Fatal("released source was tombstoned; a reconnecting client would be refused until restart")
	}
	if sourceLifecycle.terminal {
		t.Fatal("released source lifecycle is terminal")
	}
	provider.releaseSourceLifecycle(peerId, sourceLifecycle)
}

// A Reliability status that lands while a release is joining makes the source
// terminal. The release's transient owner must not readmit it: the provider
// keeps the tombstone for the rest of its generation.
func TestRemoteUserNatProviderReliabilityDuringUnreachableReleaseStaysTerminal(t *testing.T) {
	provider, localUserNat, client, _ := newClockedUnreachableSourceTestProvider(t, time.Hour)
	peerId := NewId()
	installUnreachableProviderReturnSequence(t, provider, client, peerId)
	nat := observeUnreachableSourceNat(t, localUserNat, peerId)
	released := make(chan struct{})
	var releasedOnce sync.Once
	provider.afterUnreachableSourceReleaseForTest = func(sourceId Id) {
		if sourceId == peerId {
			releasedOnce.Do(func() { close(released) })
		}
	}
	terminalRetired := make(chan struct{})
	var terminalOnce sync.Once
	provider.afterSourceRetirementForTest = func(sourceId Id) {
		if sourceId == peerId {
			terminalOnce.Do(func() { close(terminalRetired) })
		}
	}

	startUnreachableProviderReturn(t, provider, peerId)
	waitProviderSourceLifecycleBarrier(t, nat.retired, "NAT flow retirement for the unreachable source")
	provider.contractStatus(providerSourceReliabilityStatus(peerId))
	nat.finishFlow()
	waitProviderSourceLifecycleBarrier(t, released, "unreachable source release")
	waitProviderSourceLifecycleBarrier(t, terminalRetired, "terminal source retirement")

	if sourceLifecycle := provider.acquireSourceLifecycle(peerId); sourceLifecycle != nil {
		provider.releaseSourceLifecycle(peerId, sourceLifecycle)
		t.Fatal("terminal source was readmitted by the unreachable release")
	}
	if closedProviderTestChannel(nat.readmitted) {
		t.Fatal("NAT readmitted a source the provider retired as terminal")
	}
}

// Close joins a release that is still waiting on flows, and the provider
// cancellation ends that wait: the release must not run its Transfer
// cancellation on a client that can outlive this provider generation.
func TestRemoteUserNatProviderCloseJoinsUnreachableRelease(t *testing.T) {
	provider, localUserNat, client, _ := newClockedUnreachableSourceTestProvider(t, time.Hour)
	peerId := NewId()
	installUnreachableProviderReturnSequence(t, provider, client, peerId)
	nat := observeUnreachableSourceNat(t, localUserNat, peerId)
	released := make(chan struct{})
	var releasedOnce sync.Once
	provider.afterUnreachableSourceReleaseForTest = func(sourceId Id) {
		if sourceId == peerId {
			releasedOnce.Do(func() { close(released) })
		}
	}

	startUnreachableProviderReturn(t, provider, peerId)
	waitProviderSourceLifecycleBarrier(t, nat.retired, "NAT flow retirement for the unreachable source")
	// the flow is held by this test until its cleanup, so a release that has
	// already finished here did not join it
	if closedProviderTestChannel(released) {
		t.Fatal("the release finished while its flow was still live")
	}

	closeReturned := make(chan struct{})
	go func() {
		provider.Close()
		close(closeReturned)
	}()
	waitProviderSourceLifecycleBarrier(t, closeReturned, "provider close joining the release")
	// Close returns only after the release worker has run to completion, so the
	// worker can never touch the client after this provider generation ends.
	// This states the contract; it does not guard it: a Close that stopped
	// joining the worker still passed, because the worker finishes during
	// Close's other joins (PROVIDERFIXES.md, known test gaps).
	if !closedProviderTestChannel(released) {
		t.Fatal("provider close returned before the in-flight release finished")
	}
}

// While the backend is degraded no destination can get a contract, so every
// socket-owned return stalls for a reason that says nothing about the
// destination. The provider must keep retrying and release only once the
// backend recovers.
// The abandon timeout is one nanosecond, so the stall is expired at every check
// after the first: the first reads a silence of zero because it stamps the
// stall's start itself, and every later one reads at least the retry timeout.
// The row then turns on whether an expired check declines, not on how long the
// test waits for it, and nothing here sleeps or races a timeout.
//
// The retry hook runs on the producer's own goroutine, between the two abandon
// checks of one iteration and the next, and a release ends that loop. So the
// third retry is itself the proof that at least two checks saw an expired stall
// and released nothing; the count is the assertion, and the recovery is driven
// from the same place rather than after a wait.
func TestRemoteUserNatProviderDoesNotReleaseSourceWhileBackendDegraded(t *testing.T) {
	provider, localUserNat, client := newUnreachableSourceTestProvider(t, time.Nanosecond)
	var degraded atomic.Bool
	degraded.Store(true)
	provider.backendDegradedForTest = degraded.Load
	peerId := NewId()
	installUnreachableProviderReturnSequence(t, provider, client, peerId)
	nat := observeUnreachableSourceNat(t, localUserNat, peerId)
	nat.finishFlow()

	// a release that happens while the backend is degraded is the defect this
	// row exists for, recorded where it would happen rather than inferred
	var releasedWhileDegraded atomic.Bool
	released := make(chan struct{})
	var releasedOnce sync.Once
	recovered := make(chan struct{})
	var recoveredOnce sync.Once
	provider.afterUnreachableSourceReleaseForTest = func(sourceId Id) {
		if sourceId != peerId {
			return
		}
		if degraded.Load() {
			// end the wait below on this too, so the defect reports itself
			// instead of appearing as a barrier that timed out
			releasedWhileDegraded.Store(true)
			recoveredOnce.Do(func() { close(recovered) })
		}
		releasedOnce.Do(func() { close(released) })
	}
	const declinedRetryCount = 3
	var retryCount atomic.Int64
	provider.beforeTcpReturnSendRetryForTest = func() {
		if retryCount.Add(1) != declinedRetryCount {
			return
		}
		degraded.Store(false)
		recoveredOnce.Do(func() { close(recovered) })
	}

	producerReturned := startUnreachableProviderReturn(t, provider, peerId)
	waitProviderSourceLifecycleBarrier(
		t,
		recovered,
		"the third retry of a return whose abandon check declined while the backend was degraded",
	)
	if releasedWhileDegraded.Load() {
		t.Fatal(
			"the source was released while the backend was degraded, so a stall with no contract to send through was read as a silent client",
		)
	}
	waitProviderSourceLifecycleBarrier(t, producerReturned, "TCP return producer after backend recovery")
	waitProviderSourceLifecycleBarrier(t, released, "unreachable source release after backend recovery")
	waitProviderSourceLifecycleBarrier(t, nat.retired, "NAT flow retirement after backend recovery")
	if retries := retryCount.Load(); retries < declinedRetryCount {
		t.Fatalf(
			"the return retried %d times against the %d this row counts, so no abandon check is known to have declined an expired stall",
			retries,
			declinedRetryCount,
		)
	}
}

// A non-positive abandon timeout keeps the historical behavior: the socket
// reader keeps its consumed bytes and retries until the source or provider
// closes.
func TestRemoteUserNatProviderUnboundedTcpReturnRetryWhenAbandonDisabled(t *testing.T) {
	provider, _, client, clock := newClockedUnreachableSourceTestProvider(t, 0)
	peerId := NewId()
	installUnreachableProviderReturnSequence(t, provider, client, peerId)
	provider.afterUnreachableSourceReleaseForTest = func(sourceId Id) {
		t.Errorf("source %s released with the abandon timeout disabled", sourceId)
	}

	producerReturned := startUnreachableProviderReturn(t, provider, peerId)
	waitProviderSourceLifecycleBarrier(t, clock.expiredRetry, "retry after a 1000 h stall with abandon disabled")
	if closedProviderTestChannel(producerReturned) {
		t.Fatal("TCP return abandoned with the abandon timeout disabled")
	}
	// closing the provider is the only release
	provider.Close()
	waitProviderSourceLifecycleBarrier(t, producerReturned, "TCP return producer after provider close")
}
