package connect

import (
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/urnetwork/connect/protocol"
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

func newUnreachableSourceTestProvider(
	t *testing.T,
	abandonTimeout time.Duration,
) (*RemoteUserNatProvider, *LocalUserNat, *Client) {
	provider, localUserNat, client := newProviderSourceLifecycleTestFixture(t, func(settings *RemoteUserNatProviderSettings) {
		settings.WriteTimeout = 0
		settings.ReturnSendRetryTimeout = time.Millisecond
		settings.ReturnSendAbandonTimeout = abandonTimeout
	})
	// the backend state is process-wide and other tests' clients trip it;
	// these tests decide it explicitly
	provider.backendDegradedForTest = func() bool { return false }
	return provider, localUserNat, client
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
	provider, localUserNat, client := newUnreachableSourceTestProvider(t, 50*time.Millisecond)
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
	provider, localUserNat, client := newUnreachableSourceTestProvider(t, 50*time.Millisecond)
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
	provider, localUserNat, client := newUnreachableSourceTestProvider(t, 50*time.Millisecond)
	peerId := NewId()
	installUnreachableProviderReturnSequence(t, provider, client, peerId)
	nat := observeUnreachableSourceNat(t, localUserNat, peerId)
	provider.afterUnreachableSourceReleaseForTest = func(Id) {}

	startUnreachableProviderReturn(t, provider, peerId)
	waitProviderSourceLifecycleBarrier(t, nat.retired, "NAT flow retirement for the unreachable source")

	closeReturned := make(chan struct{})
	go func() {
		provider.Close()
		close(closeReturned)
	}()
	waitProviderSourceLifecycleBarrier(t, closeReturned, "provider close joining the release")
}

// While the backend is degraded no destination can get a contract, so every
// socket-owned return stalls for a reason that says nothing about the
// destination. The provider must keep retrying and release only once the
// backend recovers.
func TestRemoteUserNatProviderDoesNotReleaseSourceWhileBackendDegraded(t *testing.T) {
	provider, localUserNat, client := newUnreachableSourceTestProvider(t, 20*time.Millisecond)
	var degraded atomic.Bool
	degraded.Store(true)
	provider.backendDegradedForTest = degraded.Load
	peerId := NewId()
	installUnreachableProviderReturnSequence(t, provider, client, peerId)
	nat := observeUnreachableSourceNat(t, localUserNat, peerId)
	nat.finishFlow()

	producerReturned := startUnreachableProviderReturn(t, provider, peerId)
	select {
	case <-producerReturned:
		t.Fatal("TCP return abandoned while the backend was degraded")
	case <-time.After(250 * time.Millisecond):
	}

	degraded.Store(false)
	waitProviderSourceLifecycleBarrier(t, producerReturned, "TCP return producer after backend recovery")
	waitProviderSourceLifecycleBarrier(t, nat.retired, "NAT flow retirement after backend recovery")
}

// A non-positive abandon timeout keeps the historical behavior: the socket
// reader keeps its consumed bytes and retries until the source or provider
// closes.
func TestRemoteUserNatProviderUnboundedTcpReturnRetryWhenAbandonDisabled(t *testing.T) {
	provider, _, client := newUnreachableSourceTestProvider(t, 0)
	peerId := NewId()
	installUnreachableProviderReturnSequence(t, provider, client, peerId)
	provider.afterUnreachableSourceReleaseForTest = func(sourceId Id) {
		t.Errorf("source %s released with the abandon timeout disabled", sourceId)
	}

	producerReturned := startUnreachableProviderReturn(t, provider, peerId)
	select {
	case <-producerReturned:
		t.Fatal("TCP return abandoned with the abandon timeout disabled")
	case <-time.After(250 * time.Millisecond):
	}
	// closing the provider is the only release
	provider.Close()
	waitProviderSourceLifecycleBarrier(t, producerReturned, "TCP return producer after provider close")
}
