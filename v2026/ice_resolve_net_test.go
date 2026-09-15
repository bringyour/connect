//go:build !js

// This file pins native ICE name resolution to the lifetime of the exact peer
// generation that initiated it.
package connect

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/pion/transport/v4/stdnet"
	"github.com/pion/webrtc/v4"
)

// blockingIceResolver exposes entry into a real net.Resolver lookup and holds
// its wire dial until the lookup context is canceled.
type blockingIceResolver struct {
	resolver *net.Resolver
	entered  chan struct{}
	exited   chan struct{}
}

// newBlockingIceResolver creates an explicit cancellation barrier without a
// reachable DNS server or scheduler-dependent packet timeout.
func newBlockingIceResolver() *blockingIceResolver {
	entered := make(chan struct{})
	exited := make(chan struct{})
	var enteredOnce sync.Once
	var exitedOnce sync.Once
	resolver := &net.Resolver{
		PreferGo:     true,
		StrictErrors: true,
		Dial: func(ctx context.Context, network string, address string) (
			net.Conn,
			error,
		) {
			enteredOnce.Do(func() { close(entered) })
			<-ctx.Done()
			exitedOnce.Do(func() { close(exited) })
			return nil, context.Cause(ctx)
		},
	}
	return &blockingIceResolver{
		resolver: resolver,
		entered:  entered,
		exited:   exited,
	}
}

// waitForIceResolveTestSignal bounds a positive synchronization edge; elapsed
// time is not part of the behavior being asserted.
func waitForIceResolveTestSignal(
	t *testing.T,
	signal <-chan struct{},
	description string,
) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

// TestPeerConnectionResolveNetCancelsStunAndTurnLookups covers both resolver
// entry points used by Pion gathering. The old transport.Net calls had no
// cancellation input and remained parked in DNS after their peer was gone.
func TestPeerConnectionResolveNetCancelsStunAndTurnLookups(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		base, err := stdnet.NewNet()
		if err != nil {
			t.Fatal(err)
		}
		cases := []struct {
			network string
			address string
			resolve func(*peerConnectionResolveNet) error
		}{
			{
				network: testUdpNetwork(ipVersion),
				address: "stun.blocked.invalid:3478",
				resolve: func(network *peerConnectionResolveNet) error {
					_, resolveErr := network.ResolveUDPAddr(testUdpNetwork(ipVersion), "stun.blocked.invalid:3478")
					return resolveErr
				},
			},
			{
				network: testTcpNetwork(ipVersion),
				address: "turn.blocked.invalid:3478",
				resolve: func(network *peerConnectionResolveNet) error {
					_, resolveErr := network.ResolveTCPAddr(testTcpNetwork(ipVersion), "turn.blocked.invalid:3478")
					return resolveErr
				},
			},
		}
		for _, testCase := range cases {
			blockedResolver := newBlockingIceResolver()
			network, cancel := newPeerConnectionResolveNet(
				base,
				blockedResolver.resolver,
				time.Hour,
			)
			if network.resolver == blockedResolver.resolver {
				t.Fatal("peer resolver aliases the shared resolver")
			}
			if !network.resolver.PreferGo || !network.resolver.StrictErrors {
				t.Fatal("peer resolver did not preserve resolver policy")
			}
			resolveResult := make(chan error, 1)
			go func() {
				resolveResult <- testCase.resolve(network)
			}()
			waitForIceResolveTestSignal(
				t,
				blockedResolver.entered,
				testCase.network+" resolver entry for "+testCase.address,
			)
			cancel()
			select {
			case resolveErr := <-resolveResult:
				if !errors.Is(resolveErr, context.Canceled) {
					t.Fatalf(
						"%s resolve error = %v, want context cancellation",
						testCase.network,
						resolveErr,
					)
				}
			case <-time.After(5 * time.Second):
				t.Fatalf("%s resolve ignored peer cancellation", testCase.network)
			}
			waitForIceResolveTestSignal(
				t,
				blockedResolver.exited,
				testCase.network+" resolver exit",
			)
		}
	})
}

// TestWebRtcPeerTeardownCancelsBlockedStunResolution reproduces the production
// shutdown ordering: SetLocalDescription starts a STUN hostname lookup, then
// the peer retires while that lookup is blocked. Teardown must cancel DNS
// before PeerConnection.Close joins the ICE gatherer's WaitGroup.
func TestWebRtcPeerTeardownCancelsBlockedStunResolution(t *testing.T) {
	blockedResolver := newBlockingIceResolver()
	settings := DefaultWebRtcSettings()
	settings.Log = NewNoopLogger()
	settings.EnableDatagramFastPath = false
	settings.UseLoopbackOnlyIceInterfaces = true
	settings.StunGatherTimeout = time.Hour
	settings.IceServerUrls = []string{"stun:stun.blocked.invalid:3478"}
	settings.iceResolverForTest = blockedResolver.resolver
	factory, _, err := newWebRtcPeerConnectionFactory(settings, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer factory.Close()

	peerCtx, cancelPeer := context.WithCancel(context.Background())
	defer cancelPeer()
	peer, err := newPeerConn(
		peerCtx,
		peerConnKey{PeerId: NewId(), StreamId: NewId()},
		NewId(),
		true,
		consumingLifecycleSignalSender{},
		settings,
		func() (*webrtc.PeerConnection, context.CancelFunc, error) {
			return factory.NewPeerConnection(false)
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !peer.startWorker("peer connection run", peer.Run) {
		t.Fatal("could not start peer connection")
	}
	waitForIceResolveTestSignal(
		t,
		blockedResolver.entered,
		"Pion STUN resolver entry",
	)

	go peer.teardown()
	waitForIceResolveTestSignal(t, peer.teardownDone, "peer teardown")
	waitForIceResolveTestSignal(t, blockedResolver.exited, "STUN resolver exit")
	waitForIceResolveTestSignal(t, peer.workers.Done(), "peer workers")
}
