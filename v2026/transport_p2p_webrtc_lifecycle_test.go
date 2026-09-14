package connect

// Deterministic pin for the closed-conn signal-delivery contract: a peerConn
// that has been closed must treat late inbound signals as a cheap no-op —
// no buffering, no pion calls, no error, bounded time under a flood.
//
// Discovered while root-causing the ForceStream+Encrypted integration flake:
// the stall windows of failed attempts were filled with a per-signal
// "[pion:ice]Failed to handle message: the agent is closed" loop — closed
// associations still being fed while wedged attempts ground on. Signals for
// a dead conn are meaningless (the outer transport recreates the conn and
// fresh signals target the replacement), so the guard drops them at the
// peerConn boundary.

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"

	"github.com/urnetwork/connect/v2026/protocol"
)

func lifecycleTestManager(ctx context.Context, t *testing.T) *WebRtcManager {
	settings := DefaultWebRtcSettings()
	settings.Log = NewNoopLogger()
	// hermetic same-host mode, like every establishing test in
	// transport_p2p_webrtc_test.go
	settings.IceServerUrls = nil
	settings.UseLoopbackOnlyIceInterfaces = true
	return newTestWebRtcManager(t, ctx, newSignalPipe(nil), settings)
}

// testIceHostCandidate is a host ICE candidate line for the family: the
// address sits in the family's documentation range so nothing routes.
func testIceHostCandidate(ipVersion int) string {
	if ipVersion == 6 {
		return "candidate:0 1 udp 2122252543 2001:db8::1 40000 typ host"
	}
	return "candidate:0 1 udp 2122252543 192.0.2.1 40000 typ host"
}

func lifecycleCandidateSignals(t *testing.T, ipVersion int, streamId Id, count int) *protocol.ExchangeSignals {
	candidateJson, err := json.Marshal(webrtc.ICECandidateInit{
		Candidate: testIceHostCandidate(ipVersion),
	})
	if err != nil {
		t.Fatalf("marshal candidate: %v", err)
	}
	signals := make([]*protocol.ExchangeSignal, 0, count)
	for i := 0; i < count; i += 1 {
		signals = append(signals, &protocol.ExchangeSignal{
			SignalType:   protocol.SignalType_IceCandidate,
			IceCandidate: candidateJson,
		})
	}
	return &protocol.ExchangeSignals{
		StreamId: streamId.Bytes(),
		Signals:  signals,
	}
}

// TestClosedPeerConnDropsLateSignals: after a conn closes, delivered signals
// must not accumulate state (the pre-SDP candidate buffer stays empty), must
// not error (one dead conn must not fail a caller's batch), and a flood must
// return in bounded time (the no-op is cheap — no locks into pion, no
// per-signal work that could occupy the shared signal-delivery path).
func TestClosedPeerConnDropsLateSignals(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		testClosedPeerConnDropsLateSignals(t, ipVersion)
	})
}

func testClosedPeerConnDropsLateSignals(t *testing.T, ipVersion int) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	manager := lifecycleTestManager(ctx, t)

	peerId := NewId()
	streamId := NewId()
	// local client id for the conn's path: the manager keys inbound signals
	// by (source peer, stream)
	selfId := NewId()

	conn, err := manager.NewP2pConnPassive(ctx, NewTransferPath(selfId, peerId, streamId))
	AssertEqual(t, err, nil)
	pconn := conn.(*peerConn)

	// sanity: a LIVE fresh passive conn buffers pre-SDP candidates — this is
	// what makes the buffer a discriminating observable for the closed path
	source := SourceId(peerId)
	source.StreamId = streamId
	// eventually-consistent: delivery can race the conn's startup, so retry
	// the sanity probe briefly — the pin is that a LIVE conn buffers at all
	sanityDeadline := time.Now().Add(5 * time.Second)
	for {
		err = manager.ReceiveExchangeSignals(
			source,
			TransferKey{},
			lifecycleCandidateSignals(t, ipVersion, streamId, 1),
		)
		AssertEqual(t, err, nil)
		buffered := func() int {
			pconn.signalLock.Lock()
			defer pconn.signalLock.Unlock()
			return len(pconn.remoteIceCandidateBuffer)
		}()
		if 0 < buffered {
			break
		}
		if sanityDeadline.Before(time.Now()) {
			t.Fatal("a live passive conn never buffered a pre-SDP candidate: the buffer observable is broken, so this test cannot discriminate")
		}
		time.Sleep(10 * time.Millisecond)
	}

	conn.Close()
	// teardown clears the pre-close buffer asynchronously; wait for the clear
	// so the flood below starts from a known-zero baseline. The property
	// under test is that late signals never repopulate it.
	clearDeadline := time.Now().Add(5 * time.Second)
	for {
		buffered := func() int {
			pconn.signalLock.Lock()
			defer pconn.signalLock.Unlock()
			return len(pconn.remoteIceCandidateBuffer)
		}()
		if buffered == 0 {
			break
		}
		if clearDeadline.Before(time.Now()) {
			t.Fatal("teardown never cleared the candidate buffer")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// The hazard window: teardown DEREGISTERS the conn asynchronously, but
	// signals arriving between close and deregistration still route to it.
	// Pin that window deterministically by re-registering the closed conn —
	// the per-conn guard is then the only defense (the suite's established
	// construct-the-racy-state-directly pattern).
	func() {
		manager.stateLock.Lock()
		defer manager.stateLock.Unlock()
		manager.peerConns[peerConnKey{PeerId: peerId, StreamId: streamId}] = pconn
	}()

	// late signals: dropped without error, without buffering, in bounded time
	startTime := time.Now()
	for i := 0; i < 100; i += 1 {
		err = manager.ReceiveExchangeSignals(
			source,
			TransferKey{},
			lifecycleCandidateSignals(t, ipVersion, streamId, 10),
		)
		if err != nil {
			t.Fatalf("late signals to a closed conn must be a no-op, not an error (batch %d): %v", i, err)
		}
	}
	elapsed := time.Since(startTime)
	if 5*time.Second <= elapsed {
		t.Fatalf("1000 late signals took %s: the closed-conn path is doing per-signal work", elapsed)
	}
	func() {
		pconn.signalLock.Lock()
		defer pconn.signalLock.Unlock()
		if got := len(pconn.remoteIceCandidateBuffer); got != 0 {
			t.Fatalf(
				"a closed conn accumulated %d late candidates: late signals must be dropped at the boundary, not processed into a dead association",
				got,
			)
		}
	}()
}

// blockingSignalSender models a fully backpressured transfer send queue: a
// blocking-context send parks forever (until test ctx cancel), while a send
// carrying the signalSendNonBlocking marker returns immediately, recording
// it. This discriminates the receive-path send contract by construction.
type blockingSignalSender struct {
	ctx              context.Context
	nonBlockingCount int32
	blockedCount     int32
}

// SendSignal consumes one frame after modeling blocking or nonblocking delivery.
func (self *blockingSignalSender) SendSignal(_ Id, signal *protocol.Frame, opts ...any) {
	defer MessagePoolReturn(signal.MessageBytes)
	for _, opt := range opts {
		if _, ok := opt.(signalSendNonBlocking); ok {
			atomic.AddInt32(&self.nonBlockingCount, 1)
			return
		}
	}
	atomic.AddInt32(&self.blockedCount, 1)
	<-self.ctx.Done()
}

// TestReceivePathSignalSendsDoNotBlock: a response send produced by an
// inbound signal (here: WaitingForSdpOffer making the active side replay its
// cached offer) must use the non-blocking contract — with the transfer send
// queue fully backpressured, signal delivery must still return promptly.
// Blocking here wedges the shared signal-delivery path for every peer on the
// client (the CODESTYLE receive rule, observed as integration stalls).
func TestReceivePathSignalSendsDoNotBlock(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultWebRtcSettings()
	settings.Log = NewNoopLogger()
	settings.IceServerUrls = nil
	settings.UseLoopbackOnlyIceInterfaces = true
	sender := &blockingSignalSender{ctx: ctx}
	manager := newTestWebRtcManager(t, ctx, sender, settings)

	peerId := NewId()
	streamId := NewId()
	selfId := NewId()

	// the ACTIVE side caches an offer at setup (its initial offer send runs
	// in the conn's own sender context, where blocking is the intended
	// backpressure — it parks against the blocked sender in its own
	// goroutine and does not affect this test's delivery path)
	conn, err := manager.NewP2pConnActive(ctx, NewTransferPath(selfId, peerId, streamId))
	AssertEqual(t, err, nil)
	pconn := conn.(*peerConn)
	defer conn.Close()

	// wait for the cached offer (the replay source) to exist
	offerDeadline := time.Now().Add(10 * time.Second)
	for pconn.offerSignal() == nil {
		if offerDeadline.Before(time.Now()) {
			t.Fatal("the active conn never cached an offer")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// inbound WaitingForSdpOffer: the receive path replays the cached offer.
	// With a fully backpressured sender this must still return promptly.
	source := SourceId(peerId)
	source.StreamId = streamId
	done := make(chan error, 1)
	go func() {
		done <- manager.ReceiveExchangeSignals(source, TransferKey{}, &protocol.ExchangeSignals{
			StreamId: streamId.Bytes(),
			Signals: []*protocol.ExchangeSignal{{
				SignalType: protocol.SignalType_WaitingForSdpOffer,
			}},
		})
	}()
	select {
	case err := <-done:
		AssertEqual(t, err, nil)
	case <-time.After(5 * time.Second):
		t.Fatal("an inbound signal's response send blocked the signal-delivery path: receive-path sends must use timeout 0 and drop under backpressure")
	}
	if got := atomic.LoadInt32(&sender.nonBlockingCount); got == 0 {
		t.Fatal("the receive-path response send did not carry the non-blocking contract")
	}
}

// A Pion ICE callback shares Pion's event machinery. Candidate publication
// must therefore use the same zero-timeout transfer handoff as an inbound
// signal response, even though the candidate was generated locally.
func TestPionIceCandidateCallbackSendDoesNotBlock(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		testPionIceCandidateCallbackSendDoesNotBlock(t, ipVersion)
	})
}

func testPionIceCandidateCallbackSendDoesNotBlock(t *testing.T, ipVersion int) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sender := &blockingSignalSender{ctx: ctx}
	conn := &peerConn{
		ctx:                ctx,
		key:                peerConnKey{PeerId: NewId(), StreamId: NewId()},
		signalSender:       sender,
		signalGeneration:   NewId(),
		iceCandidatesReady: true,
	}
	candidate := &webrtc.ICECandidate{
		Foundation: "nonblocking",
		Priority:   1,
		Address:    testDocAddr(ipVersion, 1).String(),
		Protocol:   webrtc.ICEProtocolUDP,
		Port:       10000,
		Typ:        webrtc.ICECandidateTypeHost,
		Component:  1,
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		conn.handleLocalIceCandidate(candidate)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Pion ICE callback blocked on a saturated Transfer sender")
	}
	if got := atomic.LoadInt32(&sender.nonBlockingCount); got != 1 {
		t.Fatalf("nonblocking candidate sends = %d, want 1", got)
	}
	if got := atomic.LoadInt32(&sender.blockedCount); got != 0 {
		t.Fatalf("blocking candidate sends = %d, want 0", got)
	}
}
